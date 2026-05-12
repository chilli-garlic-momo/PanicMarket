//! Example Trading Engine - Reference Implementation
//! This is the starter template contestants can build upon.

use axum::{
    response::Json,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};

mod orderbook;
use orderbook::OrderBook;

#[derive(Clone)]
struct AppState {
    orderbook: Arc<Mutex<OrderBook>>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("trading_engine=info")
        .init();

    let state = AppState {
        orderbook: Arc::new(Mutex::new(OrderBook::new())),
    };

    // HTTP server for health check
    let http_state = state.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(health_check));

        let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
        info!("HTTP health check on :8080/health");
        axum::serve(listener, app).await.unwrap();
    });

    // WebSocket server for trading
    let ws_listener = TcpListener::bind("0.0.0.0:8080").await;
    // Note: In production, we'd use a single port with path routing.
    // For simplicity: health on HTTP /health, trading on WS /trading
    // Using a unified Axum setup:

    let state_clone = state.clone();
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/trading", get(|ws: axum::extract::WebSocketUpgrade| async move {
            ws.on_upgrade(|socket| handle_ws_connection(socket, state_clone))
        }));

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    info!("Trading Engine listening on 0.0.0.0:8080");
    info!("  Health: GET  http://0.0.0.0:8080/health");
    info!("  Trading: WS  ws://0.0.0.0:8080/trading");

    axum::serve(listener, app).await.unwrap();
}

async fn health_check() -> Json<Value> {
    Json(json!({"status": "ready"}))
}

async fn handle_ws_connection(
    socket: axum::extract::ws::WebSocket,
    state: AppState,
) {
    use axum::extract::ws::Message as AxumMsg;
    let (mut sender, mut receiver) = socket.split();

    while let Some(Ok(msg)) = receiver.next().await {
        let text = match msg {
            AxumMsg::Text(t) => t,
            AxumMsg::Close(_) => break,
            AxumMsg::Ping(d) => {
                let _ = sender.send(AxumMsg::Pong(d)).await;
                continue;
            }
            _ => continue,
        };

        let response = process_message(&text, &state.orderbook).await;

        if let Some(resp) = response {
            if sender.send(AxumMsg::Text(resp.to_string())).await.is_err() {
                break;
            }
        }
    }
}

async fn process_message(text: &str, orderbook: &Arc<Mutex<OrderBook>>) -> Option<Value> {
    let msg: Value = serde_json::from_str(text).ok()?;
    let msg_type = msg["type"].as_str()?;
    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    match msg_type {
        "new_order" => {
            let order_id = msg["order_id"].as_str()?.to_string();
            let order_type = msg["order_type"].as_str()?;
            let side = msg["side"].as_str()?;
            let symbol = msg["symbol"].as_str()?.to_string();
            let quantity = msg["quantity"].as_u64()?;
            let price = msg["price"].as_f64(); // None for market orders

            // Basic validation
            if quantity == 0 {
                return Some(json!({
                    "type": "order_rejected",
                    "order_id": order_id,
                    "reason": "invalid_quantity",
                    "timestamp": timestamp,
                }));
            }

            if order_type == "limit" && (price.is_none() || price.unwrap() <= 0.0) {
                return Some(json!({
                    "type": "order_rejected",
                    "order_id": order_id,
                    "reason": "invalid_price",
                    "timestamp": timestamp,
                }));
            }

            let mut book = orderbook.lock().await;
            let fills = book.add_order(
                order_id.clone(),
                order_type == "market",
                side == "buy",
                symbol,
                quantity,
                price,
            );

            // For simplicity in starter: send accepted + any fills
            // A full implementation would send multiple messages
            if fills.is_empty() {
                Some(json!({
                    "type": "order_accepted",
                    "order_id": order_id,
                    "timestamp": timestamp,
                }))
            } else {
                let total_filled: u64 = fills.iter().map(|f| f.quantity).sum();
                let remaining = quantity.saturating_sub(total_filled);
                let fill_price = fills[0].price;
                Some(json!({
                    "type": "order_filled",
                    "order_id": order_id,
                    "fill_price": fill_price,
                    "fill_quantity": total_filled,
                    "remaining_quantity": remaining,
                    "timestamp": timestamp,
                }))
            }
        }

        "cancel_order" => {
            let order_id = msg["order_id"].as_str()?.to_string();
            let mut book = orderbook.lock().await;

            if book.cancel_order(&order_id) {
                Some(json!({
                    "type": "order_canceled",
                    "order_id": order_id,
                    "timestamp": timestamp,
                }))
            } else {
                Some(json!({
                    "type": "order_rejected",
                    "order_id": order_id,
                    "reason": "order_not_found",
                    "timestamp": timestamp,
                }))
            }
        }

        _ => None,
    }
}