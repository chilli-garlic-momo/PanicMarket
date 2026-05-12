-- Submissions table
CREATE TABLE IF NOT EXISTS submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_name VARCHAR(255) NOT NULL,
    language VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'queued',
    -- queued | building | built | deploying | testing | completed | failed
    minio_key VARCHAR(500) NOT NULL,
    image_ref VARCHAR(500),
    build_log TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tests table
CREATE TABLE IF NOT EXISTS tests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID NOT NULL REFERENCES submissions(id),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    -- pending | running | completed | failed
    
    -- Performance metrics
    max_tps BIGINT,
    p50_latency_ns BIGINT,
    p99_latency_ns BIGINT,
    p999_latency_ns BIGINT,
    total_orders BIGINT,
    successful_orders BIGINT,
    failed_orders BIGINT,
    error_rate DOUBLE PRECISION,
    
    -- Scoring
    throughput_score DOUBLE PRECISION,
    latency_score DOUBLE PRECISION,
    stability_score DOUBLE PRECISION,
    final_score DOUBLE PRECISION,
    
    -- Correctness (Phase 2, pre-populated as pass for MVP)
    correctness_passed BOOLEAN DEFAULT TRUE,
    correctness_score DOUBLE PRECISION DEFAULT 100.0,
    
    -- Engine deployment info
    engine_endpoint VARCHAR(500),
    engine_container_id VARCHAR(255),
    
    -- Timeline events (JSON array)
    timeline JSONB DEFAULT '[]'::jsonb,
    
    failure_reason VARCHAR(500),
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- Leaderboard view
CREATE OR REPLACE VIEW leaderboard AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.final_score DESC, t.completed_at ASC) AS rank,
    s.team_name,
    s.id AS submission_id,
    t.id AS test_id,
    t.final_score AS score,
    t.max_tps,
    ROUND((t.p99_latency_ns / 1000000.0)::numeric, 3) AS p99_latency_ms,
    t.error_rate,
    t.correctness_passed,
    t.completed_at AS timestamp
FROM tests t
JOIN submissions s ON s.id = t.submission_id
WHERE t.status = 'completed'
  AND t.final_score IS NOT NULL
ORDER BY t.final_score DESC, t.completed_at ASC;

-- Telemetry samples (rolling window, for real-time metrics)
CREATE TABLE IF NOT EXISTS telemetry_samples (
    id BIGSERIAL PRIMARY KEY,
    test_id UUID NOT NULL REFERENCES tests(id),
    sampled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tps BIGINT NOT NULL,
    p99_latency_ns BIGINT,
    error_count BIGINT,
    phase VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_telemetry_test_id ON telemetry_samples(test_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_sampled_at ON telemetry_samples(sampled_at);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER submissions_updated_at
    BEFORE UPDATE ON submissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER tests_updated_at
    BEFORE UPDATE ON tests
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();