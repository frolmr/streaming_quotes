//! Wire protocol between server and client.
//!
//! The protocol is split across two transports:
//!
//! **TCP (control)** — the client sends exactly one line and the server
//! replies with exactly one line:
//!
//! ```text
//! client → server:  STREAM <udp_addr> <TICKER1>, <TICKER2>, ...\n
//! server → client:  OK\n
//!                or ERR <reason>\n
//! ```
//!
//! The client keeps the TCP connection only long enough to read the
//! response; after that, all traffic is UDP.
//!
//! **UDP (data + liveness)**
//!
//! - The server sends quotes to each subscriber's UDP address, one quote
//!   per datagram, in the format defined by [`crate::quote::StockQuote`].
//! - The client sends [`PING_MESSAGE`] periodically to keep its
//!   subscription alive. The server evicts subscribers that stop pinging
//!   (see [`crate::registry::PING_TIMEOUT`]).
//! - On graceful shutdown the server sends [`STOP_MESSAGE`] so clients
//!   can exit cleanly instead of waiting for the ping timeout.
//!
//! All messages are line-oriented: they end with `\n`, and parsers are
//! lenient about a trailing `\r\n` so the protocol works across platforms.
//!
//! # Example
//!
//! ```ignore
//! let cmd = parse_stream_command("STREAM 127.0.0.1:9000 AAPL, MSFT\n")?;
//! assert_eq!(cmd.tickers, vec!["AAPL".to_string(), "MSFT".to_string()]);
//! ```

use std::net::SocketAddr;

/// PING message, sent by clients over UDP to keep their subscription alive.
///
/// Wire form is the literal bytes `"PING\n"`. Sent from the same UDP
/// socket the client bound for receiving quotes — the server uses the
/// datagram's source address to look up the subscriber. A client that
/// pings from a *different* socket than it receives quotes on will be
/// evicted despite pinging, so this is a real constraint, not just a
/// convention.
pub const PING_MESSAGE: &[u8] = b"PING\n";

/// Sent by the server over UDP to all subscribers on graceful shutdown.
///
/// Wire form is the literal bytes `"STOP\n"`. This is a control message,
/// not a quote: clients must recognize it *before* attempting to parse
/// the datagram as a [`crate::quote::StockQuote`]. It is best-effort
/// (UDP — no ack, no retry); if a client misses it, the server's eviction
/// timeout eventually removes the subscription and the client's own
/// quote-gap watchdog makes it exit.
pub const STOP_MESSAGE: &[u8] = b"STOP\n";

/// A parsed `STREAM` command received from a client over TCP.
///
/// Wire format:
/// ```text
/// STREAM <ip>:<port> <TICKER1>, <TICKER2>, ...\n
/// ```
///
/// `client_udp_addr` is the address the client will receive quotes on —
/// **not** the address of the TCP connection. The server normalizes
/// `0.0.0.0` to the TCP peer IP before storing it, so a client may
/// advertise `0.0.0.0:9000` (meaning "any interface") and still be
/// reachable.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamCommand {
    /// UDP address the client is listening on for quotes.
    ///
    /// Parsed verbatim from the wire; the caller is responsible for
    /// normalizing unspecified addresses.
    pub client_udp_addr: SocketAddr,
    /// Tickers the client wants to subscribe to.
    ///
    /// Deduplicated? No — duplicates are preserved. Whitespace around
    /// each entry is trimmed and empty entries are dropped, so
    /// `"AAPL, TSLA ,MSFT"` becomes `["AAPL", "TSLA", "MSFT"]`.
    pub tickers: Vec<String>,
}

/// Errors returned by [`parse_stream_command`].
///
/// Each variant corresponds to a distinct failure point in parsing, so
/// callers can map them to different `ERR` responses if desired. The
/// current server collapses all of them into `ERR invalid command`, but
/// the variants exist so that decision can change without touching the
/// parser.
#[derive(Debug)]
pub enum ProtocolError {
    /// The input line was empty (or only whitespace).
    Empty,
    /// The line did not start with `"STREAM "`.
    BadPrefix,
    /// The line started with `"STREAM "` but had no address token.
    MissingAddr,
    /// The line had an address but no ticker list.
    MissingTickers,
    /// The address token was present but didn't parse as `SocketAddr`
    /// (hostnames are **not** accepted here — the client must send an
    /// actual IP).
    BadAddr(String),
    /// The ticker list was present but contained no non-empty entries
    /// (e.g. `"STREAM 127.0.0.1:1 , ,"`).
    NoTickers,
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::Empty => write!(f, "empty line"),
            ProtocolError::BadPrefix => write!(f, "expected line to start with 'STREAM '"),
            ProtocolError::MissingAddr => write!(f, "missing client address"),
            ProtocolError::MissingTickers => write!(f, "missing ticker list"),
            ProtocolError::BadAddr(a) => write!(f, "invalid client address: {a}"),
            ProtocolError::NoTickers => write!(f, "no tickers provided"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Parse a `STREAM` command from a single line.
///
/// Accepts the line with or without a trailing `\n` / `\r\n`, and trims
/// leading/trailing whitespace from the whole line. Individual ticker
/// entries are trimmed as well (see [`StreamCommand::tickers`]).
///
/// # Errors
///
/// Returns [`ProtocolError`] describing the first failure encountered.
/// Note that ticker *validity* (whether a ticker exists in the allow-list)
/// is **not** checked here — that's a separate concern handled by the
/// server via [`crate::tickers::unknown_tickers`].
pub fn parse_stream_command(line: &str) -> Result<StreamCommand, ProtocolError> {
    let line = line.trim_end_matches(['\r', '\n']).trim();
    if line.is_empty() {
        return Err(ProtocolError::Empty);
    }

    let rest = line
        .strip_prefix("STREAM ")
        .ok_or(ProtocolError::BadPrefix)?;

    let mut parts = rest.splitn(2, char::is_whitespace);
    let addr_str = parts.next().ok_or(ProtocolError::MissingAddr)?;
    let tickers_str = parts.next().ok_or(ProtocolError::MissingTickers)?;

    let client_udp_addr: SocketAddr = addr_str
        .parse()
        .map_err(|_| ProtocolError::BadAddr(addr_str.to_string()))?;

    let tickers: Vec<String> = tickers_str
        .split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect();

    if tickers.is_empty() {
        return Err(ProtocolError::NoTickers);
    }

    Ok(StreamCommand {
        client_udp_addr,
        tickers,
    })
}

/// Server response to a `STREAM` command. Clients read exactly one line.
///
/// Wire format:
/// - success: `OK\n`
/// - failure: `ERR <reason>\n`
///
/// The reason string is human-readable and not intended to be parsed
/// programmatically — clients just print it. That leaves the server free
/// to change wording without breaking clients.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamResponse {
    /// Subscription accepted.
    Ok,
    /// Subscription rejected; the string is the reason, without the
    /// `ERR ` prefix or trailing newline.
    Err(String),
}

impl StreamResponse {
    /// Serialize to the wire form, **including** the trailing `\n`.
    ///
    /// Unlike [`crate::quote::StockQuote::to_wire_line`], the framing is
    /// baked in here because the response is exactly one line by
    /// definition — there's no batching to do.
    pub fn to_wire(&self) -> String {
        match self {
            StreamResponse::Ok => "OK\n".to_string(),
            StreamResponse::Err(reason) => format!("ERR {reason}\n"),
        }
    }

    /// Parse a single response line (the inverse of [`Self::to_wire`]).
    ///
    /// Accepts a trailing `\r\n` for symmetry with the rest of the
    /// protocol. Any other non-empty input is an error — `OK` and `ERR `
    /// are the only valid prefixes.
    ///
    /// # Errors
    ///
    /// Returns a `String` describing the problem if the line doesn't
    /// start with `"OK"` or `"ERR "`. The `String` (rather than a
    /// dedicated error type) is deliberate: response parsing failures
    /// are always fatal for the client and just get printed, so the
    /// error only needs to be human-readable.
    pub fn parse_line(line: &str) -> Result<StreamResponse, String> {
        let line = line.trim_end_matches(['\r', '\n']);
        if line == "OK" {
            Ok(StreamResponse::Ok)
        } else if let Some(rest) = line.strip_prefix("ERR ") {
            Ok(StreamResponse::Err(rest.to_string()))
        } else {
            Err(format!("unrecognized response: {line:?}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_command() {
        let cmd = parse_stream_command("STREAM 127.0.0.1:34254 AAPL, TSLA ,MSFT\n").unwrap();
        assert_eq!(cmd.client_udp_addr, "127.0.0.1:34254".parse().unwrap());
        assert_eq!(cmd.tickers, vec!["AAPL", "TSLA", "MSFT"]);
    }

    #[test]
    fn rejects_bad_prefix() {
        assert!(matches!(
            parse_stream_command("HELLO 127.0.0.1:1 AAPL"),
            Err(ProtocolError::BadPrefix)
        ));
    }

    #[test]
    fn rejects_bad_addr() {
        assert!(matches!(
            parse_stream_command("STREAM not-an-addr AAPL"),
            Err(ProtocolError::BadAddr(_))
        ));
    }

    #[test]
    fn rejects_empty_tickers() {
        assert!(matches!(
            parse_stream_command("STREAM 127.0.0.1:1 , ,"),
            Err(ProtocolError::NoTickers)
        ));
    }

    #[test]
    fn parses_ok_response() {
        assert_eq!(StreamResponse::parse_line("OK\n"), Ok(StreamResponse::Ok));
    }

    #[test]
    fn parses_err_response() {
        assert_eq!(
            StreamResponse::parse_line("ERR unknown ticker: GOOG\n"),
            Ok(StreamResponse::Err("unknown ticker: GOOG".to_string()))
        );
    }

    #[test]
    fn rejects_garbage_response() {
        assert!(StreamResponse::parse_line("hello\n").is_err());
    }

    #[test]
    fn formats_responses() {
        assert_eq!(StreamResponse::Ok.to_wire(), "OK\n");
        assert_eq!(
            StreamResponse::Err("invalid udp address".into()).to_wire(),
            "ERR invalid udp address\n"
        );
    }
}
