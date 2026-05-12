//! Reference Order Book Implementation
//! Price-Time priority, supports limit and market orders

use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Debug, Clone)]
pub struct Order {
    pub id: String,
    pub is_buy: bool,
    pub symbol: String,
    pub quantity: u64,
    pub price: Option<f64>, // None = market order
    pub remaining: u64,
    pub seq: u64, // For time priority
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub price: f64,
    pub quantity: u64,
}

pub struct OrderBook {
    // Buy orders: price (descending) → queue of orders
    bids: BTreeMap<OrderedFloat, VecDeque<Order>>,
    // Sell orders: price (ascending) → queue of orders
    asks: BTreeMap<OrderedFloat, VecDeque<Order>>,
    // Order lookup for cancellation
    order_map: HashMap<String, (bool, f64)>, // (is_buy, price)
    sequence: u64,
}

// Wrapper for f64 that implements Ord
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_map: HashMap::new(),
            sequence: 0,
        }
    }

    pub fn add_order(
        &mut self,
        id: String,
        is_market: bool,
        is_buy: bool,
        symbol: String,
        quantity: u64,
        price: Option<f64>,
    ) -> Vec<Fill> {
        // Prevent duplicate order IDs
        if self.order_map.contains_key(&id) {
            return vec![];
        }

        self.sequence += 1;
        let mut order = Order {
            id: id.clone(),
            is_buy,
            symbol,
            quantity,
            price,
            remaining: quantity,
            seq: self.sequence,
        };

        let fills = self.match_order(&mut order, is_market);

        // If limit order has remaining quantity, add to book
        if !is_market && order.remaining > 0 {
            if let Some(p) = order.price {
                let price_key = OrderedFloat(p);
                if is_buy {
                    self.bids
                        .entry(price_key)
                        .or_insert_with(VecDeque::new)
                        .push_back(order.clone());
                } else {
                    self.asks
                        .entry(price_key)
                        .or_insert_with(VecDeque::new)
                        .push_back(order.clone());
                }
                self.order_map.insert(id, (is_buy, p));
            }
        }

        fills
    }

    fn match_order(&mut self, incoming: &mut Order, is_market: bool) -> Vec<Fill> {
        let mut fills = Vec::new();

        if incoming.is_buy {
            // Buy order matches against asks (lowest price first)
            let mut to_remove = Vec::new();

            for (&ask_price, queue) in self.asks.iter_mut() {
                if incoming.remaining == 0 {
                    break;
                }

                // Price check: limit orders only cross if ask <= bid price
                if !is_market {
                    if let Some(limit_price) = incoming.price {
                        if ask_price.0 > limit_price {
                            break;
                        }
                    }
                }

                let mut i = 0;
                while i < queue.len() && incoming.remaining > 0 {
                    let resting = &mut queue[i];
                    let fill_qty = incoming.remaining.min(resting.remaining);

                    fills.push(Fill {
                        price: ask_price.0,
                        quantity: fill_qty,
                    });

                    incoming.remaining -= fill_qty;
                    resting.remaining -= fill_qty;

                    if resting.remaining == 0 {
                        self.order_map.remove(&resting.id);
                        queue.remove(i);
                    } else {
                        i += 1;
                    }
                }

                if queue.is_empty() {
                    to_remove.push(ask_price);
                }
            }

            for price in to_remove {
                self.asks.remove(&price);
            }
        } else {
            // Sell order matches against bids (highest price first)
            let mut to_remove = Vec::new();

            // BTreeMap is ascending, so we iterate in reverse for bids
            let bid_prices: Vec<OrderedFloat> = self.bids.keys().cloned().collect();

            for ask_price in bid_prices.into_iter().rev() {
                if incoming.remaining == 0 {
                    break;
                }

                if !is_market {
                    if let Some(limit_price) = incoming.price {
                        if ask_price.0 < limit_price {
                            break;
                        }
                    }
                }

                if let Some(queue) = self.bids.get_mut(&ask_price) {
                    let mut i = 0;
                    while i < queue.len() && incoming.remaining > 0 {
                        let resting = &mut queue[i];
                        let fill_qty = incoming.remaining.min(resting.remaining);

                        fills.push(Fill {
                            price: ask_price.0,
                            quantity: fill_qty,
                        });

                        incoming.remaining -= fill_qty;
                        resting.remaining -= fill_qty;

                        if resting.remaining == 0 {
                            self.order_map.remove(&resting.id);
                            queue.remove(i);
                        } else {
                            i += 1;
                        }
                    }

                    if queue.is_empty() {
                        to_remove.push(ask_price);
                    }
                }
            }

            for price in to_remove {
                self.bids.remove(&price);
            }
        }

        fills
    }

    pub fn cancel_order(&mut self, order_id: &str) -> bool {
        if let Some((is_buy, price)) = self.order_map.remove(order_id) {
            let price_key = OrderedFloat(price);
            let book = if is_buy { &mut self.bids } else { &mut self.asks };

            if let Some(queue) = book.get_mut(&price_key) {
                if let Some(pos) = queue.iter().position(|o| o.id == order_id) {
                    queue.remove(pos);
                    if queue.is_empty() {
                        book.remove(&price_key);
                    }
                    return true;
                }
            }
        }
        false
    }
}