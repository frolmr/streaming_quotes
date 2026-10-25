use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use log::{debug, error, info, warn};
use rand::RngExt;

use stock_streamer::protocol::{parse_stream_command, StreamResponse, PING_MESSAGE, STOP_MESSAGE};
use stock_streamer::quote::StockQuote;
use stock_streamer::registry::{RegisterError, Registry, SpawnSender, SubscriberId, PING_TIMEOUT};
use stock_streamer::tickers::{allowed_tickers, unknown_tickers};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:34254";

#[derive(Debug, Parser)]
#[command(name = "server", version, about = "Stock quote streaming server")]
struct Cli {
    /// Address to bind for both TCP (control) and UDP (quotes + ping).
    ///
    /// TCP and UDP are separate namespaces, so the same `ip:port` for both
    /// is legal, and it means clients only need to know one port number.
    #[arg(long, value_name = "IP:PORT", default_value = DEFAULT_BIND_ADDR)]
    bind_addr: SocketAddr,
}

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    info!("starting");
    info!(
        "allow-list ({} tickers): {:?}",
        allowed_tickers().len(),
        sorted_allowed()
    );

    let registry = Arc::new(Registry::new());

    let udp = UdpSocket::bind(cli.bind_addr)?;
    info!("UDP socket bound at {}", udp.local_addr()?);
    let udp = Arc::new(udp);

    let shutdown = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();

    {
        let shutdown = Arc::clone(&shutdown);
        if let Err(e) = ctrlc::set_handler(move || {
            if !shutdown.swap(true, Ordering::SeqCst) {
                info!("SIGINT received, shutting down");
                let _ = shutdown_tx.send(());
            }
        }) {
            error!("failed to install SIGINT handler: {e}");
            error!("Ctrl-C will kill the process without sending STOP");
        }
    }

    spawn_quote_generator(Arc::clone(&registry));
    spawn_ping_receiver(Arc::clone(&registry), Arc::clone(&udp));
    spawn_evictor(Arc::clone(&registry));

    let listener = TcpListener::bind(cli.bind_addr)?;
    listener.set_nonblocking(true)?;
    info!("TCP listening on {}", listener.local_addr()?);

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => handle_tcp_client(stream, &registry, Arc::clone(&udp)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                match shutdown_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            Err(e) => warn!("accept error: {e}"),
        }
    }

    do_shutdown(&registry, &udp);
    Ok(())
}

fn do_shutdown(registry: &Registry, udp: &UdpSocket) {
    info!("broadcasting STOP to all subscribers");
    for (id, addr, _tickers) in registry.snapshot() {
        match udp.send_to(STOP_MESSAGE, addr) {
            Ok(n) => debug!("STOP sent to {:?} ({} bytes)", id, n),
            Err(e) => warn!("failed to send STOP to {:?} ({}): {e}", id, addr),
        }
    }

    info!("closing subscriber sender threads");
    let ids: Vec<SubscriberId> = registry
        .snapshot()
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();
    for id in ids {
        registry.remove(id);
    }

    info!("shutdown complete");
}

fn sorted_allowed() -> Vec<&'static str> {
    let mut v: Vec<_> = allowed_tickers().iter().copied().collect();
    v.sort_unstable();
    v
}

fn handle_tcp_client(stream: TcpStream, registry: &Registry, udp: Arc<UdpSocket>) {
    let peer = stream.peer_addr().ok();
    debug!("accepted TCP connection from {:?}", peer);

    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;
    let mut line = String::new();

    let response = match reader.read_line(&mut line) {
        Ok(0) => {
            debug!("client closed connection without sending anything");
            return;
        }
        Ok(_) => {
            debug!("received raw line: {:?}", line);
            build_stream_response(&line, peer, registry, udp)
        }
        Err(e) => {
            warn!("error reading from client: {e}");
            return;
        }
    };

    debug!("responding: {:?}", response.to_wire().trim_end());
    if let Err(e) = writer.write_all(response.to_wire().as_bytes()) {
        warn!("failed to write response to client: {e}");
    }
    let _ = writer.flush();
}

fn build_stream_response(
    line: &str,
    peer: Option<SocketAddr>,
    registry: &Registry,
    udp: Arc<UdpSocket>,
) -> StreamResponse {
    let cmd = match parse_stream_command(line) {
        Ok(c) => c,
        Err(_) => return StreamResponse::Err("invalid command".into()),
    };

    let mut udp_addr = cmd.client_udp_addr;
    if udp_addr.ip().is_unspecified() {
        match peer.map(|p| p.ip()) {
            Some(ip) => udp_addr.set_ip(ip),
            None => return StreamResponse::Err("invalid udp address".into()),
        }
    }

    if cmd.tickers.is_empty() {
        return StreamResponse::Err("empty ticker list".into());
    }

    let ticker_refs: Vec<&str> = cmd.tickers.iter().map(String::as_str).collect();
    if let Some(bad) = unknown_tickers(&ticker_refs).first() {
        return StreamResponse::Err(format!("unknown ticker: {bad}"));
    }

    let spawn: SpawnSender = Box::new(move |id, addr, rx| {
        let udp = Arc::clone(&udp);
        thread::spawn(move || sender_thread_body(id, addr, rx, udp))
    });

    let id = match registry.register(udp_addr, &cmd.tickers, spawn) {
        Ok(id) => id,
        Err(RegisterError::AddrInUse) => {
            return StreamResponse::Err("udp address already registered".into());
        }
    };
    info!(
        "registered {:?} udp={} tickers={:?}",
        id, udp_addr, cmd.tickers
    );
    info!(
        "registry: {} subscriber(s) {:?}, active tickers={:?}",
        registry.subscriber_count(),
        registry.snapshot(),
        registry.active_tickers()
    );

    StreamResponse::Ok
}

fn sender_thread_body(
    id: SubscriberId,
    addr: SocketAddr,
    rx: mpsc::Receiver<StockQuote>,
    udp: Arc<UdpSocket>,
) {
    debug!("sender thread for {:?} ({}) started", id, addr);
    for quote in rx {
        let payload = format!("{}\n", quote.to_wire_line());
        if let Err(e) = udp.send_to(payload.as_bytes(), addr) {
            warn!("send of {} to {} failed: {e}", quote.ticker, addr);
        }
    }
    debug!("sender thread for {:?} ({}) exiting", id, addr);
}

fn spawn_quote_generator(registry: Arc<Registry>) {
    thread::spawn(move || {
        let mut rng = rand::rng();
        loop {
            let sleep_ms = rng.random_range(100..=500u64);
            thread::sleep(Duration::from_millis(sleep_ms));

            for ticker in registry.active_tickers() {
                let quote = StockQuote::random(&ticker);
                for tx in registry.sender_channels_for(&ticker) {
                    let _ = tx.send(quote.clone());
                }
            }
        }
    });
}

fn spawn_ping_receiver(registry: Arc<Registry>, udp: Arc<UdpSocket>) {
    thread::spawn(move || {
        let mut buf = [0u8; 64];
        loop {
            match udp.recv_from(&mut buf) {
                Ok((n, src)) => {
                    let data = &buf[..n];
                    if data == PING_MESSAGE {
                        if registry.record_ping(src).is_none() {
                            warn!(
                                "ping from unknown addr {:?} — snapshot: {:?}",
                                src,
                                registry.snapshot()
                            );
                        }
                    } else {
                        warn!(
                            "unexpected datagram from {:?} ({} bytes): {:?}",
                            src,
                            n,
                            String::from_utf8_lossy(data)
                        );
                    }
                }
                Err(e) => warn!("UDP recv error: {e}"),
            }
        }
    });
}

fn spawn_evictor(registry: Arc<Registry>) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));
        for id in registry.evict_stale(Instant::now()) {
            info!(
                "evicted {:?} (no ping for >{}s)",
                id,
                PING_TIMEOUT.as_secs()
            );
        }
    });
}
