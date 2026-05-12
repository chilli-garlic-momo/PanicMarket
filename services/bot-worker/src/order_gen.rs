//! Order generator following spec distributions

use rand::Rng;
use serde_json::{json, Value};
use uuid::Uuid;

pub struct OrderGenerator {
    rng: rand::rngs::ThreadRng,
}

impl OrderGenerator {
    pub fn new() -> Self {
        Self { rng: rand::thread_rng() }
    }

    pub fn generate_new_order(&mut self) -> Value {
        let symbol = self.pick_symbol();
        let side = if self.rng.gen::<f64>() < 0.5 { "buy" } else { "sell" };
        let is_market = self.rng.gen::<f64>() < 0.30; // 30% market orders

        let base_price = match symbol {
            "BTCUSD" => 50000.0_f64,
            "ETHUSD" => 3000.0_f64,
            "SOLUSD" => 100.0_f64,
            _ => 100.0_f64,
        };

        // ±2.5% variance
        let variance = base_price * 0.025;
        let price = base_price + (self.rng.gen::<f64>() * 2.0 - 1.0) * variance;
        let price = (price * 100.0).round() / 100.0; // 2 decimal places

        // Exponential distribution (mean=10, min=1, max=100)
        let quantity = self.gen_exponential_quantity();

        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if is_market {
            json!({
                "type": "new_order",
                "order_id": Uuid::new_v4().to_string(),
                "order_type": "market",
                "side": side,
                "symbol": symbol,
                "quantity": quantity,
                "price": null,
                "timestamp": timestamp_ns,
            })
        } else {
            json!({
                "type": "new_order",
                "order_id": Uuid::new_v4().to_string(),
                "order_type": "limit",
                "side": side,
                "symbol": symbol,
                "quantity": quantity,
                "price": price,
                "timestamp": timestamp_ns,
            })
        }
    }

    pub fn generate_cancel_order(&mut self, order_id: &str) -> Value {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        json!({
            "type": "cancel_order",
            "order_id": order_id,
            "timestamp": timestamp_ns,
        })
    }

    fn pick_symbol(&mut self) -> &'static str {
        let r = self.rng.gen::<f64>();
        if r < 0.50 {
            "BTCUSD"
        } else if r < 0.80 {
            "ETHUSD"
        } else {
            "SOLUSD"
        }
    }

    fn gen_exponential_quantity(&mut self) -> u64 {
        // Exponential distribution: mean=10, clamped [1, 100]
        let u: f64 = self.rng.gen();
        let exp = (-10.0_f64 * u.ln()).round() as u64;
        exp.clamp(1, 100)
    }
}