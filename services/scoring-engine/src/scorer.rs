//! Scoring algorithm as specified in §6.1

pub fn compute_score(
    max_tps: i64,
    p99_latency_ns: i64,
    error_rate: f64,
    correctness_passed: bool,
) -> (f64, f64, f64, f64) {
    // Stage 1: Qualification gate
    if !correctness_passed {
        return (0.0, 0.0, 0.0, 0.0);
    }

    if error_rate >= 0.01 {
        // >1% error rate = fail
        return (0.0, 0.0, 0.0, 0.0);
    }

    // Stage 2: Performance Scoring

    // Throughput Score (0-100, logarithmic)
    // 10k TPS → 66 pts, 100k TPS → 83 pts, 1M TPS → 100 pts
    let throughput_score = if max_tps <= 0 {
        0.0
    } else {
        let tps_f = max_tps as f64;
        (100.0 * (tps_f.log10() / 6.0)).clamp(0.0, 100.0)
    };

    // Latency Score (0-100, exponential penalty)
    let p99_ms = p99_latency_ns as f64 / 1_000_000.0;
    let latency_score = if p99_ms < 1.0 {
        100.0
    } else if p99_ms < 10.0 {
        (100.0 - (p99_ms - 1.0) * 2.22).max(0.0)
    } else if p99_ms < 100.0 {
        (80.0 - (p99_ms - 10.0) * 0.33).max(0.0)
    } else {
        (50.0 * (-p99_ms / 100.0_f64).exp()).max(0.0)
    };

    // Stability Score (0-100)
    let stability_score = 100.0 * (1.0 - error_rate);

    // Weighted Final Score
    let final_score = 0.50 * throughput_score
        + 0.30 * latency_score
        + 0.20 * stability_score;

    (throughput_score, latency_score, stability_score, final_score)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_100k_tps_1ms_latency() {
        let (t, l, s, f) = compute_score(100_000, 1_000_000, 0.0, true);
        assert!((t - 83.3).abs() < 1.0, "throughput_score={}", t);
        assert!((l - 100.0).abs() < 0.1, "latency_score={}", l);
        assert!((s - 100.0).abs() < 0.1, "stability_score={}", s);
        assert!(f > 90.0, "final_score={}", f);
    }

    #[test]
    fn test_score_correctness_fail() {
        let (_, _, _, f) = compute_score(1_000_000, 100_000, 0.0, false);
        assert_eq!(f, 0.0);
    }

    #[test]
    fn test_score_high_error_rate() {
        let (_, _, _, f) = compute_score(100_000, 1_000_000, 0.02, true);
        assert_eq!(f, 0.0);
    }

    #[test]
    fn test_score_1m_tps() {
        let (t, _, _, _) = compute_score(1_000_000, 100_000, 0.0, true);
        assert!((t - 100.0).abs() < 0.1, "throughput_score={}", t);
    }
}