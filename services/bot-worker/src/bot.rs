//! Bot fleet management
//! Each bot maintains a WebSocket connection to the engine

use crate::metrics::TestMetrics;
use crate::order_gen::OrderGenerator;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub struct BotFleet {
    bot_count: usize,
    target_tps: u64,
    engine_endpoint: String,
    metrics: Arc<TestMetrics>,
    stop_flag: Arc<AtomicBool>,
}

impl BotFleet {
    pub fn new(
        bot_count: usize,
        target_tps: u64,
        engine_endpoint: String,
        metrics: Arc<TestMetrics>,
    ) -> Self {
        Self {
            bot_count,
            target_tps,
            engine_endpoint,
            metrics,
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn run(&self, duration_secs: u64) {
        info!(
            "Starting {} bots targeting {} TPS for {}s",
            self.bot_count, self.target_tps, duration_secs
        );

        let ws_url = self.engine_endpoint
            .replace("http://", "ws://")
            .replace("https://", "wss://");
        let ws_url = format!("{}/trading", ws_url);

        // Per-bot TPS = total_tps / bot_count
        let tps_per_bot = (self.target_tps as f64 / self.bot_count as f64).ceil() as u64;
        let interval_micros = 1_000_000 / tps_per_bot.max(1);

        let mut handles = Vec::new();

        for bot_id in 0..self.bot_count {
            let ws_url = ws_url.clone();
            let metrics = self.metrics.clone();
            let stop_flag = self.stop_flag.clone();
            let interval = Duration::from_micros(interval_micros);

            let handle = tokio::spawn(async move {
                run_single_bot(
                    bot_id,
                    ws_url,
                    metrics,
                    stop_flag,
                    interval,
                    duration_secs,
                ).await;
            });

            handles.push(handle);

            // Stagger bot starts to avoid thundering herd
            if bot_id % 10 == 9 {
                sleep(Duration::from_millis(50)).await;
            }
        }

        // Auto-stop after duration
        let stop_flag = self.stop_flag.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(duration_secs)).await;
            stop_flag.store(true, Ordering::Relaxed);
        });

        // Wait for all bots to complete
        for handle in handles {
            let _ = handle.await;
        }

        info!("All bots completed");
    }

    pub async fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        // Give bots time to wind down
        sleep(Duration::from_secs(2)).await;
    }
}

async fn run_single_bot(
    bot_id: usize,
    ws_url: String,
    metrics: Arc<TestMetrics>,
    stop_flag: Arc<AtomicBool>,
    interval: Duration,
    max_duration: u64,
) {
    let mut retries = 0u32;
    const MAX_RETRIES: u32 = 5;

    'reconnect: while !stop_flag.load(Ordering::Relaxed) && retries < MAX_RETRIES {
        match connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                retries = 0;
                debug!("Bot {} connected to {}", bot_id, ws_url);

                let (mut write, mut read) = ws_stream.split();
                let mut gen = OrderGenerator::new();
                let mut pending_orders: VecDeque<(String, Instant)> = VecDeque::new();
                let mut rng = rand::thread_rng();

                // Pending cancel orders (order_id, scheduled_at)
                let mut cancel_queue: Vec<(String, Instant)> = Vec::new();

                let start = Instant::now();
                let mut next_send = Instant::now();

                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        break 'reconnect;
                    }

                    if start.elapsed().as_secs() >= max_duration {
                        break 'reconnect;
                    }

                    let now = Instant::now();

                    // Send orders due
                    if now >= next_send {
                        // Check for pending cancels
                        let mut i = 0;
                        while i < cancel_queue.len() {
                            if now >= cancel_queue[i].1 {
                                let (order_id, _) = cancel_queue.remove(i);
                                let cancel_msg = gen.generate_cancel_order(&order_id);
                                let send_ts = Instant::now();

                                if write.send(Message::Text(cancel_msg.to_string())).await.is_err() {
                                    break 'reconnect;
                                }
                                pending_orders.push_back((format!("cancel:{}", order_id), send_ts));
                            } else {
                                i += 1;
                            }
                        }

                        // Send new order
                        let order = gen.generate_new_order();
                        let order_id = order["order_id"].as_str().unwrap_or("").to_string();
                        let is_limit = order["order_type"].as_str() == Some("limit");
                        let send_ts = Instant::now();

                        match write.send(Message::Text(order.to_string())).await {
                            Ok(_) => {
                                metrics.record_sent();
                                pending_orders.push_back((order_id.clone(), send_ts));

                                // Schedule cancel with 20% probability for limit orders
                                if is_limit && rng.gen::<f64>() < 0.20 {
                                    let cancel_delay_ms: u64 = rng.gen_range(100..=5000);
                                    cancel_queue.push((
                                        order_id,
                                        now + Duration::from_millis(cancel_delay_ms),
                                    ));
                                }
                            }
                            Err(e) => {
                                debug!("Bot {} send error: {}", bot_id, e);
                                break; // Reconnect
                            }
                        }

                        next_send = now + interval;
                    }

                    // Read responses (non-blocking)
                    let read_timeout = tokio::time::timeout(
                        Duration::from_micros(100),
                        read.next(),
                    );

                    match read_timeout.await {
                        Ok(Some(Ok(msg))) => {
                            if let Message::Text(text) = msg {
                                process_response(&text, &mut pending_orders, &metrics);
                            }
                        }
                        Ok(Some(Err(e))) => {
                            debug!("Bot {} recv error: {}", bot_id, e);
                            break; // Reconnect
                        }
                        Ok(None) => {
                            // Connection closed
                            break;
                        }
                        Err(_) => {
                            // Timeout - normal, continue
                        }
                    }

                    // Check for timed-out orders (>30s = error)
                    let timeout_threshold = Duration::from_secs(30);
                    while let Some((_, ts)) = pending_orders.front() {
                        if now.duration_since(*ts) > timeout_threshold {
                            pending_orders.pop_front();
                            metrics.record_error();
                        } else {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                retries += 1;
                warn!("Bot {} connection failed (attempt {}): {}", bot_id, retries, e);
                if retries < MAX_RETRIES {
                    sleep(Duration::from_secs(retries as u64)).await;
                }
            }
        }
    }

    debug!("Bot {} exited", bot_id);
}

fn process_response(
    text: &str,
    pending: &mut VecDeque<(String, Instant)>,
    metrics: &Arc<TestMetrics>,
) {
    let Ok(msg): Result<Value, _> = serde_json::from_str(text) else {
        metrics.record_error();
        return;
    };

    let msg_type = msg["type"].as_str().unwrap_or("");
    let order_id = msg["order_id"].as_str().unwrap_or("");

    // Find matching pending order to measure latency
    let latency = if let Some(pos) = pending.iter().position(|(id, _)| id == order_id) {
        let (_, ts) = pending.remove(pos).unwrap();
        let latency_ns = ts.elapsed().as_nanos() as u64;
        Some(latency_ns)
    } else {
        None
    };

    match msg_type {
        "order_accepted" => {
            if let Some(ns) = latency {
                metrics.record_success(ns);
            }
        }
        "order_filled" => {
            if let Some(ns) = latency {
                metrics.record_success(ns);
            }
        }
        "order_canceled" => {
            if let Some(ns) = latency {
                metrics.record_success(ns);
            }
        }
        "order_rejected" => {
            // Rejected is valid behavior - count as processed but track separately
            if let Some(ns) = latency {
                metrics.record_success(ns); // Engine responded correctly
            }
        }
        _ => {
            metrics.record_error();
        }
    }
}