//! Stock quote data type and its wire format.
//!
//! A [`StockQuote`] is the unit of data the server streams to subscribers.
//! It is generated randomly by the server (there is no real market data
//! behind it) and sent one-quote-per-datagram over UDP.
//!
//! The wire format is a single line, four `|`-separated fields:
//!
//! ```text
//! <ticker>|<price>|<volume>|<timestamp_ms>
//! ```
//!
//! For example: `AAPL|150.25|4200|1710000000123`.
//!
//! The record itself does **not** include a trailing newline —
//! [`StockQuote::to_wire_line`] returns the bare line, and the transport
//! is responsible for framing (the server appends `\n` before sending).
//! [`StockQuote::from_wire_line`] is lenient on input and trims a trailing
//! `\n` or `\r\n`, so either form is accepted when parsing.

use rand::RngExt;

/// A single stock quote: ticker, price, traded volume, and a Unix
/// timestamp in milliseconds.
///
/// This is the message the server streams to clients. All fields are
/// `pub`, so consumers can build quotes by hand (e.g. in tests) as well
/// as through [`StockQuote::random`] or [`StockQuote::from_wire_line`].
#[derive(Debug, Clone, PartialEq)]
pub struct StockQuote {
    /// Ticker symbol (e.g. `"AAPL"`).
    pub ticker: String,
    /// Last traded price. Unconstrained `f64`; the random generator
    /// currently produces values in `50.0..500.0`.
    pub price: f64,
    /// Traded volume. The random generator uses different ranges for
    /// "hot" tickers (AAPL/MSFT/TSLA) than for others.
    pub volume: u32,
    /// Unix epoch timestamp in milliseconds.
    pub timestamp_ms: u64,
}

/// Errors returned by [`StockQuote::from_wire_line`].
///
/// Each variant carries enough context to diagnose a malformed datagram:
/// numeric variants keep the offending substring, and
/// [`WrongFieldCount`](StockQuoteError::WrongFieldCount) reports how many
/// fields were actually present.
#[derive(Debug, Clone, PartialEq)]
pub enum StockQuoteError {
    /// The line had the wrong number of `|`-separated fields.
    ///
    /// A well-formed quote has exactly 4 fields. `got` is the actual
    /// count — 0..=3 for too few, >4 for too many.
    WrongFieldCount { got: usize },
    /// The price field could not be parsed as `f64`.
    InvalidPrice(String),
    /// The volume field could not be parsed as `u32` (e.g. non-numeric or negative).
    InvalidVolume(String),
    /// The timestamp field could not be parsed as `u64`.
    InvalidTimestamp(String),
    /// The ticker field was empty (e.g. the line started with `|`).
    EmptyTicker,
}

impl std::fmt::Display for StockQuoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StockQuoteError::WrongFieldCount { got } => {
                write!(f, "expected 4 fields, got {got}")
            }
            StockQuoteError::InvalidPrice(s) => write!(f, "invalid price: {s:?}"),
            StockQuoteError::InvalidVolume(s) => write!(f, "invalid volume: {s:?}"),
            StockQuoteError::InvalidTimestamp(s) => write!(f, "invalid timestamp: {s:?}"),
            StockQuoteError::EmptyTicker => write!(f, "empty ticker"),
        }
    }
}

impl std::error::Error for StockQuoteError {}

impl StockQuote {
    /// Generate a random quote for `ticker` right now.
    ///
    /// Intended for the server's quote generator. The distribution is
    /// deliberately simple and not statistically meaningful:
    ///
    /// - price is drawn uniformly from `50.0..500.0`,
    /// - volume depends on the ticker: `AAPL`, `MSFT`, `TSLA` get
    ///   `1000..6000`, everything else gets `100..1100`,
    /// - timestamp is the current wall-clock time in milliseconds.
    ///
    /// The ticker string is stored as-is (no case normalization, no
    /// validation) — validation is the server's job, via the allow-list.
    pub fn random(ticker: &str) -> StockQuote {
        let mut rng = rand::rng();
        let price = rng.random_range(50.0..500.0);
        let volume = match ticker {
            "AAPL" | "MSFT" | "TSLA" => rng.random_range(1000..6000),
            _ => rng.random_range(100..1100),
        };
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        StockQuote {
            ticker: ticker.to_string(),
            price,
            volume,
            timestamp_ms,
        }
    }

    /// Serialize to the wire format:
    /// `<ticker>|<price>|<volume>|<timestamp_ms>`.
    ///
    /// No trailing newline — the caller adds framing when sending.
    /// The inverse of this method is [`StockQuote::from_wire_line`];
    /// `from_wire_line(q.to_wire_line())` is always `Ok(q)` for any
    /// `StockQuote` produced by [`StockQuote::random`] (see the
    /// `roundtrip` test).
    pub fn to_wire_line(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp_ms
        )
    }

    /// Parse from the wire format. Trailing `\n` / `\r\n` is trimmed.
    ///
    /// # Errors
    ///
    /// Returns [`StockQuoteError`] for any malformed input:
    /// - [`WrongFieldCount`](StockQuoteError::WrongFieldCount) if the
    ///   number of `|`-separated fields is not exactly 4,
    /// - [`EmptyTicker`](StockQuoteError::EmptyTicker) if the ticker
    ///   field is empty,
    /// - one of the `Invalid*` variants if a numeric field doesn't parse.
    ///
    /// Parsing is strict: no whitespace is trimmed around fields, and
    /// extra `|` characters cause `WrongFieldCount` (not a silent
    /// partial parse). `f64` parsing accepts `"inf"` and `"NaN"` — this
    /// is intentional, because the format doesn't promise to reject
    /// non-finite values; consumers that care can check
    /// `price.is_finite()` themselves.
    pub fn from_wire_line(line: &str) -> Result<Self, StockQuoteError> {
        let line = line.trim_end_matches(['\r', '\n']);

        let mut parts = line.splitn(4, '|');
        let ticker = parts
            .next()
            .ok_or(StockQuoteError::WrongFieldCount { got: 0 })?;
        let price_s = parts
            .next()
            .ok_or(StockQuoteError::WrongFieldCount { got: 1 })?;
        let volume_s = parts
            .next()
            .ok_or(StockQuoteError::WrongFieldCount { got: 2 })?;
        let ts_s = parts
            .next()
            .ok_or(StockQuoteError::WrongFieldCount { got: 3 })?;

        if ts_s.contains('|') {
            return Err(StockQuoteError::WrongFieldCount {
                got: line.split('|').count(),
            });
        }
        if ticker.is_empty() {
            return Err(StockQuoteError::EmptyTicker);
        }

        let price = price_s
            .parse::<f64>()
            .map_err(|_| StockQuoteError::InvalidPrice(price_s.to_string()))?;
        let volume = volume_s
            .parse::<u32>()
            .map_err(|_| StockQuoteError::InvalidVolume(volume_s.to_string()))?;
        let timestamp_ms = ts_s
            .parse::<u64>()
            .map_err(|_| StockQuoteError::InvalidTimestamp(ts_s.to_string()))?;

        Ok(StockQuote {
            ticker: ticker.to_string(),
            price,
            volume,
            timestamp_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StockQuote {
        StockQuote {
            ticker: "AAPL".into(),
            price: 150.25,
            volume: 4200,
            timestamp_ms: 1_710_000_000_123,
        }
    }

    #[test]
    fn roundtrip() {
        let q = sample();
        let line = q.to_wire_line();
        assert_eq!(line, "AAPL|150.25|4200|1710000000123");
        assert_eq!(StockQuote::from_wire_line(&line).unwrap(), q);
    }

    #[test]
    fn to_wire_line_has_no_newline() {
        assert!(!sample().to_wire_line().contains('\n'));
    }

    #[test]
    fn parses_line_with_trailing_newline() {
        assert_eq!(
            StockQuote::from_wire_line("AAPL|150.25|4200|1710000000123\n").unwrap(),
            sample()
        );
    }

    #[test]
    fn parses_line_with_crlf() {
        assert_eq!(
            StockQuote::from_wire_line("AAPL|150.25|4200|1710000000123\r\n").unwrap(),
            sample()
        );
    }

    #[test]
    fn rejects_too_few_fields() {
        for line in ["", "AAPL", "AAPL|1.0", "AAPL|1.0|100"] {
            assert!(
                matches!(
                    StockQuote::from_wire_line(line),
                    Err(StockQuoteError::WrongFieldCount { .. })
                ),
                "expected WrongFieldCount for {line:?}"
            );
        }
    }

    #[test]
    fn rejects_too_many_fields() {
        let err = StockQuote::from_wire_line("AAPL|1.0|100|123|extra").unwrap_err();
        assert_eq!(err, StockQuoteError::WrongFieldCount { got: 5 });
    }

    #[test]
    fn rejects_empty_ticker() {
        assert_eq!(
            StockQuote::from_wire_line("|1.0|100|123").unwrap_err(),
            StockQuoteError::EmptyTicker
        );
    }

    #[test]
    fn rejects_bad_numbers() {
        assert!(matches!(
            StockQuote::from_wire_line("AAPL|abc|100|123"),
            Err(StockQuoteError::InvalidPrice(_))
        ));
        assert!(matches!(
            StockQuote::from_wire_line("AAPL|1.0|abc|123"),
            Err(StockQuoteError::InvalidVolume(_))
        ));
        assert!(matches!(
            StockQuote::from_wire_line("AAPL|1.0|100|abc"),
            Err(StockQuoteError::InvalidTimestamp(_))
        ));
        assert!(matches!(
            StockQuote::from_wire_line("AAPL|1.0|-1|123"),
            Err(StockQuoteError::InvalidVolume(_))
        ));
    }

    #[test]
    fn accepts_zero_and_extreme_values() {
        let q = StockQuote::from_wire_line("X|0.0|0|0").unwrap();
        assert_eq!(q.price, 0.0);
        assert_eq!(q.volume, 0);
        assert_eq!(q.timestamp_ms, 0);

        let q = StockQuote::from_wire_line(&format!("X|1.5|{}|{}", u32::MAX, u64::MAX)).unwrap();
        assert_eq!(q.volume, u32::MAX);
        assert_eq!(q.timestamp_ms, u64::MAX);
    }

    #[test]
    fn stock_quote_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<StockQuote>();
    }
}
