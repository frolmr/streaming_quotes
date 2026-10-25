//! Runtime registry of subscribers.
//!
//! The registry tracks three things:
//!
//! 1. **Who is subscribed to what** — a `ticker -> [subscriber]` index used
//!    by the quote generator to fan out each quote to the right clients.
//! 2. **How to reach each subscriber** — each subscriber owns a bounded
//!    channel (`Sender<StockQuote>`) drained by a per-subscriber sender
//!    thread, plus the UDP address that thread sends to.
//! 3. **When each subscriber was last seen** — used by the ping evictor to
//!    drop subscribers that stopped pinging.
//!
//! All state lives behind a single `Mutex`. The read paths (`sender_channels_for`,
//! `active_tickers`, `snapshot`) hold it briefly and return owned/cloned data,
//! so callers never hold the lock while doing socket I/O. Write paths
//! (`register`, `evict_stale`, `remove`) detach subscribers under the lock and
//! **join their sender threads outside it**.
//!
//! See [`PING_TIMEOUT`] for the liveness threshold, and [`Registry::evict_stale`]
//! for the API the evictor thread calls periodically.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc::{self, Sender};
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::quote::StockQuote;

/// How long a subscriber may go without a PING before we drop it.
///
/// A subscriber whose `last_ping` is older than this at the time
/// [`Registry::evict_stale`] is called will be removed and its sender
/// thread shut down.
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// How to spawn a per-subscriber sender thread.
///
/// The registry does not know how to talk to a `UdpSocket` — it only knows
/// that each subscriber needs a thread to drain its [`StockQuote`] channel.
/// The caller supplies the actual spawn policy via this closure. The server
/// passes a closure that spawns a real UDP-sending thread; tests pass a
/// no-op, so registration doesn't create OS threads in unit tests.
///
/// The closure receives:
/// - the newly allocated [`SubscriberId`] (for logging),
/// - the subscriber's UDP address (where the thread will send),
/// - the receiving end of that subscriber's quote channel.
///
/// It must return the thread's [`JoinHandle`], which the registry stores so
/// it can join the thread on eviction. Dropping the sender end (which the
/// registry does on removal) must cause the thread to exit.
pub type SpawnSender =
    Box<dyn FnOnce(SubscriberId, SocketAddr, mpsc::Receiver<StockQuote>) -> JoinHandle<()> + Send>;

/// Opaque, monotonically increasing subscriber identifier.
///
/// Issued by [`Registry::register`] and used everywhere a subscriber needs
/// to be referred to without exposing its internals. Two subscriptions from
/// the same UDP address never coexist (see [`RegisterError::AddrInUse`]),
/// so an ID uniquely identifies a live subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriberId(pub u64);

/// Internal record for one connected client.
///
/// Not part of the public API — the registry returns subscriber IDs and
/// sender channels to callers, never whole `Subscriber`s.
#[derive(Debug)]
pub struct Subscriber {
    /// Stable identifier for this subscription.
    pub id: SubscriberId,
    /// Where the sender thread sends quotes.
    pub udp_addr: SocketAddr,
    /// Last time a PING was seen from this subscriber's address.
    pub last_ping: Instant,
    /// Sending half of this subscriber's quote channel. Cloned by
    /// [`Registry::sender_channels_for`] so the generator can fan out
    /// quotes without holding the registry lock.
    pub quote_tx: Sender<StockQuote>,
    /// Join handle for the sender thread. Taken out by `shutdown`, and
    /// `Option` only so it can be `take()`n exactly once.
    pub sender_thread: Option<JoinHandle<()>>,
}

impl Subscriber {
    /// Drop the sender channel (ending the sender thread's `for` loop)
    /// then join the thread. Consumes `self`.
    ///
    /// Order matters: dropping `quote_tx` is what causes the sender thread's
    /// `for quote in rx` loop to terminate. Joining before dropping would
    /// deadlock.
    fn shutdown(self) {
        let Subscriber {
            quote_tx,
            sender_thread,
            ..
        } = self;
        drop(quote_tx);
        if let Some(h) = sender_thread {
            let _ = h.join();
        }
    }
}

/// Errors returned by [`Registry::register`].
#[derive(Debug, PartialEq)]
pub enum RegisterError {
    /// A subscriber with this UDP address is already registered.
    ///
    /// The registry rejects duplicate addresses rather than replacing the
    /// previous subscriber. This is intentional: a second client claiming
    /// the same address would otherwise silently evict the first. The
    /// previous subscriber must be evicted (by ping timeout) or explicitly
    /// removed before its address can be reused.
    AddrInUse,
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegisterError::AddrInUse => write!(f, "udp address already registered"),
        }
    }
}

impl std::error::Error for RegisterError {}

/// Mutable state of [`Registry`]. Not public — always accessed through
/// `Registry`'s `Mutex`.
#[derive(Default)]
struct RegistryInner {
    /// ticker -> list of subscribers interested in that ticker.
    ticker_to_subs_idx: HashMap<String, Vec<SubscriberId>>,
    /// subscriber id -> full subscriber record.
    subscribers_idx: HashMap<SubscriberId, Subscriber>,
    /// UDP address -> subscriber id, used both for `record_ping` lookups
    /// and to reject duplicate registrations.
    addr_to_sub_idx: HashMap<SocketAddr, SubscriberId>,
    /// Monotonic counter for allocating `SubscriberId`s.
    next_id: u64,
}

impl RegistryInner {
    /// Remove `id` from all maps and return the `Subscriber`. Panics if
    /// the id is unknown.
    ///
    /// Does **not** shut down the sender thread — the caller is expected
    /// to do that outside the registry lock, to avoid joining while holding
    /// the mutex.
    fn take(&mut self, id: SubscriberId) -> Subscriber {
        let sub = self
            .subscribers_idx
            .remove(&id)
            .expect("take of unknown id");
        self.addr_to_sub_idx.remove(&sub.udp_addr);
        self.ticker_to_subs_idx.retain(|_, ids| {
            ids.retain(|x| *x != id);
            !ids.is_empty()
        });
        sub
    }
}

/// Tracks live subscribers and their ticker subscriptions.
///
/// All methods are safe to call from multiple threads; the entire state is
/// behind a single `Mutex`. Reads clone the data they return, so the lock
/// is never held across socket I/O.
///
/// # Typical lifecycle
///
/// 1. TCP handler calls [`register`](Registry::register) with the client's
///    UDP address, its ticker list, and a `SpawnSender` closure.
/// 2. The quote generator calls [`active_tickers`](Registry::active_tickers)
///    and [`sender_channels_for`](Registry::sender_channels_for) each tick,
///    then sends each quote into the returned channels.
/// 3. The ping receiver calls [`record_ping`](Registry::record_ping) on
///    each `PING` datagram.
/// 4. The evictor periodically calls [`evict_stale`](Registry::evict_stale),
///    which removes subscribers whose `last_ping` is older than
///    [`PING_TIMEOUT`] and joins their sender threads.
#[derive(Default)]
pub struct Registry {
    inner: Mutex<RegistryInner>,
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new subscriber.
    ///
    /// Allocates a [`SubscriberId`], creates a quote channel, calls `spawn`
    /// to start the per-subscriber sender thread, and records the
    /// subscription for each ticker in `tickers`.
    ///
    /// Returns `AddrInUse` if `udp_addr` is already registered — the
    /// existing subscriber is **not** replaced. Evict the old one (via
    /// [`evict_stale`](Registry::evict_stale) after the ping timeout) before
    /// reusing the address.
    ///
    /// The sender thread is spawned **before** the subscriber is inserted
    /// into the maps. The thread itself does not touch the registry, so
    /// this cannot deadlock.
    pub fn register(
        &self,
        udp_addr: SocketAddr,
        tickers: &[String],
        spawn: SpawnSender,
    ) -> Result<SubscriberId, RegisterError> {
        let (quote_tx, quote_rx) = mpsc::channel::<StockQuote>();

        let id = {
            let mut inner = self.inner.lock().expect("registry mutex poisoned");
            if inner.addr_to_sub_idx.contains_key(&udp_addr) {
                return Err(RegisterError::AddrInUse);
            }
            let id = SubscriberId(inner.next_id);
            inner.next_id += 1;
            id
        };

        let handle = spawn(id, udp_addr, quote_rx);

        {
            let mut inner = self.inner.lock().expect("registry mutex poisoned");
            inner.subscribers_idx.insert(
                id,
                Subscriber {
                    id,
                    udp_addr,
                    last_ping: Instant::now(),
                    quote_tx,
                    sender_thread: Some(handle),
                },
            );
            inner.addr_to_sub_idx.insert(udp_addr, id);
            for ticker in tickers {
                inner
                    .ticker_to_subs_idx
                    .entry(ticker.clone())
                    .or_default()
                    .push(id);
            }
        }
        Ok(id)
    }

    /// Record a PING from `udp_addr` and refresh its liveness timestamp.
    ///
    /// Returns the subscriber's ID on success, or `None` if the address is
    /// not registered (which typically means the subscriber was already
    /// evicted, or the PING came from a stale client).
    pub fn record_ping(&self, udp_addr: SocketAddr) -> Option<SubscriberId> {
        let mut inner = self.inner.lock().expect("registry mutex poisoned");
        let &id = inner.addr_to_sub_idx.get(&udp_addr)?;
        if let Some(sub) = inner.subscribers_idx.get_mut(&id) {
            sub.last_ping = Instant::now();
        }
        Some(id)
    }

    /// Sender channels for `ticker`. The generator sends on these;
    /// unbounded channels, so `send` never blocks.
    ///
    /// Returns an empty `Vec` if no subscriber is currently interested in
    /// `ticker`. The returned channels are clones: dropping them does not
    /// end the sender threads, because the registry holds its own `Sender`
    /// per subscriber.
    pub fn sender_channels_for(&self, ticker: &str) -> Vec<Sender<StockQuote>> {
        let inner = self.inner.lock().expect("registry mutex poisoned");
        let Some(ids) = inner.ticker_to_subs_idx.get(ticker) else {
            return Vec::new();
        };
        ids.iter()
            .filter_map(|id| inner.subscribers_idx.get(id).map(|s| s.quote_tx.clone()))
            .collect()
    }

    /// Tickers that currently have at least one subscriber.
    ///
    /// The generator iterates this per tick: generating quotes for tickers
    /// nobody is subscribed to would be wasted work.
    pub fn active_tickers(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("registry mutex poisoned");
        inner
            .ticker_to_subs_idx
            .iter()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Remove and shut down subscribers whose last PING is older than
    /// `PING_TIMEOUT`. Returns the removed IDs for logging.
    ///
    /// `now` is passed in rather than read inside so the function is a pure
    /// function of its arguments (useful for tests) and so a caller doing
    /// bulk eviction uses one timestamp for the whole batch.
    ///
    /// Sender threads are joined **after** releasing the lock.
    pub fn evict_stale(&self, now: Instant) -> Vec<SubscriberId> {
        let removed: Vec<Subscriber> = {
            let mut inner = self.inner.lock().expect("registry mutex poisoned");
            let stale: Vec<SubscriberId> = inner
                .subscribers_idx
                .iter()
                .filter(|(_, s)| now.duration_since(s.last_ping) > PING_TIMEOUT)
                .map(|(&id, _)| id)
                .collect();

            stale.into_iter().map(|id| inner.take(id)).collect()
        };

        let ids = removed.iter().map(|s| s.id).collect();
        for sub in removed {
            sub.shutdown();
        }

        ids
    }

    /// Remove and shut down a subscriber. Returns `true` if it existed.
    ///
    /// Used by graceful shutdown to tear everything down cleanly. The sender
    /// thread is joined before returning.
    pub fn remove(&self, id: SubscriberId) -> bool {
        let removed = {
            let mut inner = self.inner.lock().expect("registry mutex poisoned");
            if !inner.subscribers_idx.contains_key(&id) {
                return false;
            }
            inner.take(id)
        };
        removed.shutdown();
        true
    }

    /// Number of currently registered subscribers.
    ///
    /// Cheap (one lock, one `len()`) but not used by the server — kept for
    /// tests and diagnostics.
    pub fn subscriber_count(&self) -> usize {
        self.inner
            .lock()
            .expect("registry mutex poisoned")
            .subscribers_idx
            .len()
    }

    /// Log-friendly view: (id, udp_addr, tickers) for every subscriber.
    ///
    /// Each subscriber's ticker list is sorted, so the output is stable
    /// across calls. Used by the server to include registry state in log
    /// lines (and it's how the "phantom subscriber" bug was diagnosed).
    pub fn snapshot(&self) -> Vec<(SubscriberId, SocketAddr, Vec<String>)> {
        let inner = self.inner.lock().expect("registry mutex poisoned");
        inner
            .subscribers_idx
            .values()
            .map(|sub| {
                let mut tickers: Vec<String> = inner
                    .ticker_to_subs_idx
                    .iter()
                    .filter(|(_, ids)| ids.contains(&sub.id))
                    .map(|(t, _)| t.clone())
                    .collect();
                tickers.sort_unstable();
                (sub.id, sub.udp_addr, tickers)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn tickers(t: &[&str]) -> Vec<String> {
        t.iter().map(|s| s.to_string()).collect()
    }

    fn noop_spawn(
        _id: SubscriberId,
        _addr: SocketAddr,
        _rx: mpsc::Receiver<StockQuote>,
    ) -> JoinHandle<()> {
        thread::spawn(|| {})
    }

    fn reg(r: &Registry, addr: SocketAddr, ts: &[&str]) -> SubscriberId {
        r.register(addr, &tickers(ts), Box::new(noop_spawn))
            .expect("register should succeed")
    }

    #[test]
    fn register_new_subscriber() {
        let r = Registry::new();
        let _id = reg(&r, addr(1000), &["AAPL", "TSLA"]);
        assert_eq!(r.subscriber_count(), 1);
        assert_eq!(r.sender_channels_for("AAPL").len(), 1);
        assert_eq!(r.sender_channels_for("TSLA").len(), 1);
        assert!(r.sender_channels_for("MSFT").is_empty());
    }

    #[test]
    fn re_register_same_addr_is_rejected() {
        let r = Registry::new();
        let id1 = reg(&r, addr(1000), &["AAPL", "TSLA"]);
        let err = r
            .register(addr(1000), &tickers(&["MSFT"]), Box::new(noop_spawn))
            .unwrap_err();
        assert_eq!(err, RegisterError::AddrInUse);
        // The original subscriber is untouched.
        assert_eq!(r.subscriber_count(), 1);
        assert_eq!(r.sender_channels_for("AAPL").len(), 1);
        assert_eq!(r.sender_channels_for("TSLA").len(), 1);
        assert!(r.sender_channels_for("MSFT").is_empty());
        assert_eq!(r.record_ping(addr(1000)), Some(id1));
    }

    #[test]
    fn two_addrs_get_distinct_ids() {
        let r = Registry::new();
        let id1 = reg(&r, addr(1000), &["AAPL"]);
        let id2 = reg(&r, addr(2000), &["AAPL"]);
        assert_ne!(id1, id2);
        assert_eq!(r.sender_channels_for("AAPL").len(), 2);
    }

    #[test]
    fn record_ping_unknown_addr_returns_none() {
        let r = Registry::new();
        assert_eq!(r.record_ping(addr(9999)), None);
    }

    #[test]
    fn record_ping_updates_timestamp() {
        let r = Registry::new();
        let id = reg(&r, addr(1000), &["AAPL"]);
        {
            let mut inner = r.inner.lock().unwrap();
            let s = inner.subscribers_idx.get_mut(&id).unwrap();
            s.last_ping = Instant::now() - Duration::from_secs(1);
        }
        assert_eq!(r.record_ping(addr(1000)), Some(id));
        let last = r
            .inner
            .lock()
            .unwrap()
            .subscribers_idx
            .get(&id)
            .unwrap()
            .last_ping;
        assert!(Instant::now().duration_since(last) < Duration::from_millis(50));
    }

    #[test]
    fn evict_removes_only_stale() {
        let r = Registry::new();
        let id_fresh = reg(&r, addr(1000), &["AAPL"]);
        let id_stale = reg(&r, addr(2000), &["MSFT"]);

        {
            let mut inner = r.inner.lock().unwrap();
            inner.subscribers_idx.get_mut(&id_stale).unwrap().last_ping =
                Instant::now() - PING_TIMEOUT - Duration::from_secs(1);
        }

        let evicted = r.evict_stale(Instant::now());
        assert_eq!(evicted, vec![id_stale]);
        assert_eq!(r.subscriber_count(), 1);
        assert!(r.sender_channels_for("MSFT").is_empty());
        assert_eq!(r.sender_channels_for("AAPL").len(), 1);
        assert_eq!(r.record_ping(addr(1000)), Some(id_fresh));
    }

    #[test]
    fn removing_last_subscriber_drops_ticker_entry() {
        let r = Registry::new();
        let id = reg(&r, addr(1000), &["AAPL"]);
        assert_eq!(r.active_tickers(), vec!["AAPL".to_string()]);
        assert!(r.remove(id));
        assert!(r.active_tickers().is_empty());
        assert!(!r.remove(id));
    }

    #[test]
    fn snapshot_shows_ids_and_tickers() {
        let r = Registry::new();
        reg(&r, addr(1000), &["TSLA", "AAPL"]);
        let snap = r.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].1, addr(1000));
        assert_eq!(snap[0].2, vec!["AAPL".to_string(), "TSLA".to_string()]);
    }

    #[test]
    fn remove_shuts_down_sender_thread() {
        let r = Registry::new();
        let (exited_tx, exited_rx) = mpsc::channel::<()>();
        let spawn: SpawnSender = Box::new(move |_id, _addr, rx| {
            let exited_tx = exited_tx.clone();
            thread::spawn(move || {
                for _ in rx {}
                let _ = exited_tx.send(());
            })
        });
        let id = r.register(addr(1000), &tickers(&["AAPL"]), spawn).unwrap();

        assert!(r.remove(id));
        exited_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("sender thread did not exit after remove");
    }
}
