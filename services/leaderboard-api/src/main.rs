use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};
use uuid::Uuid;

const BROADCAST_CAPACITY: usize = 100;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    broadcast_tx: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://benchmark:benchmark_secret@localhost:5432/benchmark".to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    let (broadcast_tx, _) = broadcast::channel::<String>(BROADCAST_CAPACITY);

    let state = Arc::new(AppState {
        pool,
        broadcast_tx: broadcast_tx.clone(),
    });

    // Background task: poll DB for leaderboard updates every 2s
    let state_clone = state.clone();
    tokio::spawn(async move {
        poll_leaderboard(state_clone).await;
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(serve_frontend))
        .route("/api/leaderboard", get(get_leaderboard))
        .route("/ws/leaderboard", get(ws_handler))
        .route("/notify", post(notify_update))
        .with_state(state)
        .layer(cors);

    let addr = "0.0.0.0:9092";
    info!("Leaderboard API listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn serve_frontend() -> Html<String> {
    Html(FRONTEND_HTML.to_string())
}

async fn get_leaderboard(State(state): State<Arc<AppState>>) -> Json<Value> {
    match fetch_leaderboard(&state.pool, 100).await {
        Ok(entries) => Json(json!({"entries": entries, "total": entries.len()})),
        Err(e) => {
            warn!("Failed to fetch leaderboard: {}", e);
            Json(json!({"entries": [], "error": e.to_string()}))
        }
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.broadcast_tx.subscribe();
    let (mut sender, mut receiver) = socket.split();

    // Send initial full leaderboard
    if let Ok(entries) = fetch_leaderboard(&state.pool, 100).await {
        let msg = json!({
            "type": "full_update",
            "entries": entries,
        });
        if sender.send(Message::Text(msg.to_string())).await.is_err() {
            return;
        }
    }

    // Forward broadcast updates to client
    let send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    if sender.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Handle client pings/pongs
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Close(_) => break,
                Message::Ping(data) => {
                    // axum handles pong automatically
                }
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }
}

async fn notify_update(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let test_id = payload["test_id"].as_str().unwrap_or("");
    let score = payload["score"].as_f64().unwrap_or(0.0);

    // Fetch fresh full leaderboard
    if let Ok(entries) = fetch_leaderboard(&state.pool, 100).await {
        let msg = json!({
            "type": "full_update",
            "entries": entries,
            "trigger": {
                "test_id": test_id,
                "score": score,
            }
        });

        let _ = state.broadcast_tx.send(msg.to_string());
    }

    Json(json!({"status": "notified"}))
}

async fn poll_leaderboard(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        interval.tick().await;
        if let Ok(entries) = fetch_leaderboard(&state.pool, 100).await {
            let msg = json!({
                "type": "full_update",
                "entries": entries,
            });
            let _ = state.broadcast_tx.send(msg.to_string());
        }
    }
}

async fn fetch_leaderboard(pool: &PgPool, limit: i64) -> Result<Vec<Value>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT
            ROW_NUMBER() OVER (ORDER BY t.final_score DESC, t.completed_at ASC) AS rank,
            s.team_name,
            s.id AS submission_id,
            t.id AS test_id,
            COALESCE(t.final_score, 0.0) AS score,
            t.max_tps,
            ROUND(COALESCE(t.p99_latency_ns, 0) / 1000000.0::numeric, 3) AS p99_latency_ms,
            COALESCE(t.error_rate, 0.0) AS error_rate,
            COALESCE(t.correctness_passed, true) AS correctness_passed,
            t.completed_at AS timestamp
        FROM tests t
        JOIN submissions s ON s.id = t.submission_id
        WHERE t.status = 'completed'
          AND t.final_score IS NOT NULL
        ORDER BY t.final_score DESC, t.completed_at ASC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| json!({
        "rank": r.rank,
        "team_name": r.team_name,
        "submission_id": r.submission_id,
        "test_id": r.test_id,
        "score": r.score,
        "max_tps": r.max_tps,
        "p99_latency_ms": r.p99_latency_ms,
        "error_rate": r.error_rate,
        "correctness": if r.correctness_passed.unwrap_or(true) { "pass" } else { "fail" },
        "timestamp": r.timestamp,
    })).collect())
}

// Embedded frontend HTML
const FRONTEND_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Engine Benchmark - Leaderboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0a0f1e; color: #e2e8f0; min-height: 100vh; }
        header { background: linear-gradient(135deg, #1a1f35 0%, #0d1117 100%); padding: 24px 40px; border-bottom: 1px solid #1e3a5f; }
        header h1 { font-size: 24px; font-weight: 700; color: #60a5fa; letter-spacing: -0.5px; }
        header p { color: #94a3b8; font-size: 13px; margin-top: 4px; }
        .status-bar { background: #0d1117; padding: 10px 40px; font-size: 12px; color: #64748b; display: flex; gap: 24px; border-bottom: 1px solid #1e3a5f; }
        .status-bar .live { color: #22c55e; display: flex; align-items: center; gap: 6px; }
        .status-bar .live::before { content: ''; width: 8px; height: 8px; background: #22c55e; border-radius: 50%; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
        main { padding: 32px 40px; max-width: 1400px; margin: 0 auto; }
        h2 { font-size: 18px; font-weight: 600; color: #93c5fd; margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; background: #0d1117; border: 1px solid #1e3a5f; border-radius: 12px; overflow: hidden; }
        thead { background: #1a1f35; }
        th { padding: 14px 16px; text-align: left; font-size: 11px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.8px; }
        td { padding: 14px 16px; font-size: 14px; border-top: 1px solid #1e3a5f; }
        tr:hover td { background: #111827; }
        .rank { font-weight: 700; font-size: 18px; color: #64748b; width: 60px; }
        .rank-1 { color: #fbbf24; }
        .rank-2 { color: #94a3b8; }
        .rank-3 { color: #cd7c2f; }
        .score { font-weight: 700; font-size: 20px; color: #60a5fa; }
        .team { font-weight: 600; color: #e2e8f0; }
        .metric { font-family: 'JetBrains Mono', monospace; color: #a78bfa; }
        .badge { display: inline-flex; align-items: center; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; }
        .badge-pass { background: #052e16; color: #22c55e; border: 1px solid #166534; }
        .badge-fail { background: #450a0a; color: #ef4444; border: 1px solid #991b1b; }
        .empty { text-align: center; padding: 80px; color: #374151; }
        .empty h3 { font-size: 20px; margin-bottom: 8px; }
        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }
        .stat-card { background: #0d1117; border: 1px solid #1e3a5f; border-radius: 10px; padding: 20px; }
        .stat-card .label { font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
        .stat-card .value { font-size: 28px; font-weight: 700; color: #60a5fa; }
        .stat-card .sub { font-size: 12px; color: #374151; margin-top: 4px; }
    </style>
</head>
<body>
    <header>
        <h1>⚡ Trading Engine Benchmark</h1>
        <p>Real-time performance leaderboard · Updated live</p>
    </header>
    <div class="status-bar">
        <span class="live" id="ws-status">Connecting...</span>
        <span id="last-update">--</span>
        <span id="entry-count">0 entries</span>
    </div>
    <main>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="label">Top Score</div>
                <div class="value" id="top-score">--</div>
                <div class="sub">out of 100</div>
            </div>
            <div class="stat-card">
                <div class="label">Best TPS</div>
                <div class="value" id="best-tps">--</div>
                <div class="sub">orders/second</div>
            </div>
            <div class="stat-card">
                <div class="label">Best Latency</div>
                <div class="value" id="best-latency">--</div>
                <div class="sub">p99 ms</div>
            </div>
            <div class="stat-card">
                <div class="label">Submissions</div>
                <div class="value" id="total-submissions">--</div>
                <div class="sub">total entries</div>
            </div>
        </div>

        <h2>🏆 Rankings</h2>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Team</th>
                    <th>Score</th>
                    <th>Max TPS</th>
                    <th>p99 Latency</th>
                    <th>Error Rate</th>
                    <th>Correctness</th>
                    <th>Submitted</th>
                </tr>
            </thead>
            <tbody id="leaderboard-body">
                <tr>
                    <td colspan="8" class="empty">
                        <h3>No submissions yet</h3>
                        <p>Submit your trading engine to appear on the leaderboard</p>
                    </td>
                </tr>
            </tbody>
        </table>
    </main>

    <script>
        let ws;
        let reconnectDelay = 1000;

        function connect() {
            const wsUrl = `ws://${location.hostname}:9092/ws/leaderboard`;
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                document.getElementById('ws-status').textContent = 'Live';
                document.getElementById('ws-status').style.color = '#22c55e';
                reconnectDelay = 1000;
            };

            ws.onmessage = (e) => {
                const data = JSON.parse(e.data);
                if (data.type === 'full_update') {
                    updateLeaderboard(data.entries);
                } else if (data.type === 'incremental_update') {
                    fetchAndUpdate();
                }
                document.getElementById('last-update').textContent =
                    `Last update: ${new Date().toLocaleTimeString()}`;
            };

            ws.onclose = () => {
                document.getElementById('ws-status').textContent = 'Reconnecting...';
                document.getElementById('ws-status').style.color = '#f59e0b';
                setTimeout(connect, reconnectDelay);
                reconnectDelay = Math.min(reconnectDelay * 2, 10000);
            };

            ws.onerror = () => ws.close();
        }

        async function fetchAndUpdate() {
            const resp = await fetch('/api/leaderboard');
            const data = await resp.json();
            updateLeaderboard(data.entries || []);
        }

        function updateLeaderboard(entries) {
            const tbody = document.getElementById('leaderboard-body');

            document.getElementById('entry-count').textContent =
                `${entries.length} entries`;

            if (entries.length === 0) {
                tbody.innerHTML = `<tr><td colspan="8" class="empty">
                    <h3>No submissions yet</h3>
                    <p>Submit your trading engine to appear on the leaderboard</p>
                </td></tr>`;
                return;
            }

            // Update stats
            if (entries[0]) {
                document.getElementById('top-score').textContent =
                    (entries[0].score || 0).toFixed(1);
                document.getElementById('total-submissions').textContent = entries.length;
            }

            const allTps = entries.map(e => e.max_tps || 0).filter(v => v > 0);
            if (allTps.length > 0) {
                document.getElementById('best-tps').textContent =
                    Math.max(...allTps).toLocaleString();
            }

            const allLatency = entries.map(e => e.p99_latency_ms || 9999).filter(v => v > 0);
            if (allLatency.length > 0) {
                document.getElementById('best-latency').textContent =
                    Math.min(...allLatency).toFixed(2) + 'ms';
            }

            tbody.innerHTML = entries.map(e => {
                const rankClass = e.rank <= 3 ? `rank-${e.rank}` : '';
                const rankEmoji = e.rank === 1 ? '🥇' : e.rank === 2 ? '🥈' : e.rank === 3 ? '🥉' : '';
                const correctnessBadge = e.correctness === 'pass'
                    ? '<span class="badge badge-pass">✓ PASS</span>'
                    : '<span class="badge badge-fail">✗ FAIL</span>';
                const ts = e.timestamp ? new Date(e.timestamp).toLocaleString() : '--';
                const tps = e.max_tps ? e.max_tps.toLocaleString() : '--';
                const lat = e.p99_latency_ms != null ? e.p99_latency_ms.toFixed(3) + 'ms' : '--';
                const errRate = e.error_rate != null ? (e.error_rate * 100).toFixed(3) + '%' : '--';

                return `<tr>
                    <td class="rank ${rankClass}">${rankEmoji}${e.rank}</td>
                    <td class="team">${escapeHtml(e.team_name)}</td>
                    <td class="score">${(e.score || 0).toFixed(2)}</td>
                    <td class="metric">${tps}</td>
                    <td class="metric">${lat}</td>
                    <td class="metric">${errRate}</td>
                    <td>${correctnessBadge}</td>
                    <td style="color:#475569;font-size:12px">${ts}</td>
                </tr>`;
            }).join('');
        }

        function escapeHtml(str) {
            return String(str)
                .replace(/&/g,'&amp;')
                .replace(/</g,'&lt;')
                .replace(/>/g,'&gt;')
                .replace(/"/g,'&quot;');
        }

        // Initial fetch + WebSocket
        fetchAndUpdate();
        connect();

        // Heartbeat fallback: refresh every 10s even without WS updates
        setInterval(fetchAndUpdate, 10000);
    </script>
</body>
</html>
"#;