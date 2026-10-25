//! Allow-list of stock tickers.
//!
//! The list is embedded into the binary at compile time from
//! `assets/tickers.txt`, so the server rejects any ticker that wasn't
//! present when the binary was built. This is intentional: the allow-list
//! is a static property of the deployment, not a runtime setting.
//!
//! The file is parsed leniently — commas and whitespace both separate
//! entries, empty entries are ignored, so `A, B, C` and one-per-line
//! both work.

use std::collections::HashSet;
use std::sync::OnceLock;

/// Raw contents of `assets/tickers.txt`, embedded at compile time.
const TICKERS_RAW: &str = include_str!("../assets/tickers.txt");

static ALLOWED_TICKERS: OnceLock<HashSet<&'static str>> = OnceLock::new();

/// Returns the set of tickers this binary was built with.
///
/// The set is parsed on first call and cached for the lifetime of the
/// process, so subsequent calls are essentially free. Ticker comparison
/// is **case-sensitive** and exact: `"AAPL"` matches, `"aapl"` does not.
///
/// # Panics
///
/// Does not panic. If `assets/tickers.txt` is empty at build time, the
/// returned set is empty and every subscription will be rejected as
/// "unknown ticker".
pub fn allowed_tickers() -> &'static HashSet<&'static str> {
    ALLOWED_TICKERS.get_or_init(|| {
        TICKERS_RAW
            .split(|c: char| c == ',' || c.is_whitespace())
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .collect()
    })
}

/// Returns the entries in `requested` that are **not** in the allow-list,
/// in the same order as `requested` and with duplicates preserved.
///
/// An empty result means every requested ticker is valid. This is a
/// pure function of `requested` and the build-time allow-list — it does
/// not consult any runtime state.
///
/// # Examples
///
/// ```ignore
/// // Assuming assets/tickers.txt contains "AAPL, MSFT":
/// assert!(unknown_tickers(&["AAPL", "MSFT"]).is_empty());
/// assert_eq!(unknown_tickers(&["AAPL", "GOOG"]), vec!["GOOG".to_string()]);
/// ```
pub fn unknown_tickers(requested: &[&str]) -> Vec<String> {
    let allowed = allowed_tickers();
    requested
        .iter()
        .filter(|t| !allowed.contains(*t))
        .map(|t| t.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_list_contains_expected_entries() {
        let allowed = allowed_tickers();
        for t in ["NVDA", "AAPL", "MSFT", "META", "AMZN", "TSLA", "NFLX", "MU"] {
            assert!(allowed.contains(t), "expected {t} in allow-list");
        }
        assert!(!allowed.contains("GOOG"));
    }

    #[test]
    fn unknown_tickers_reports_only_invalid_ones() {
        let bad = unknown_tickers(&["AAPL", "GOOG", "TSLA", "NOPE"]);
        assert_eq!(bad, vec!["GOOG".to_string(), "NOPE".to_string()]);
    }

    #[test]
    fn unknown_tickers_empty_when_all_valid() {
        assert!(unknown_tickers(&["AAPL", "MSFT"]).is_empty());
    }
}
