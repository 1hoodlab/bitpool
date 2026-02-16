use serde_json::json;

use crate::domain::entities::{Block, BlockHeader, MessageKind, ScheduledDelivery};
use crate::domain::repositories::{DomainRepoError, EventRepository};

use super::types::{MempoolTx, NodeTip};
use super::DeterministicEngineCore;

impl<R: EventRepository> DeterministicEngineCore<R> {
    pub(super) async fn handle_delivery(
        &mut self,
        delivery: &ScheduledDelivery,
    ) -> Result<(), DomainRepoError> {
        match delivery.kind {
            MessageKind::Tx => {
                let tx_id = delivery
                    .payload
                    .get("tx_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if !tx_id.is_empty() {
                    let tx = MempoolTx {
                        tx_id: tx_id.clone(),
                        fee_sats: delivery
                            .payload
                            .get("fee_sats")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                        weight: delivery
                            .payload
                            .get("weight")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32,
                        created_at_ms: delivery
                            .payload
                            .get("created_at_ms")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(self.sim_time_ms),
                        origin_node: delivery.from.clone(),
                    };
                    let inserted = self.insert_mempool_tx(tx);
                    if inserted {
                        self.emit_event(
                            "node.tx_received.v1",
                            json!({
                                "node": delivery.to,
                                "from": delivery.from,
                                "tx_id": tx_id,
                            }),
                            false,
                        )
                        .await?;
                    }
                }
            }
            MessageKind::Block => {
                let block_id = delivery
                    .payload
                    .get("block_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let height = delivery
                    .payload
                    .get("height")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let parent_hash = delivery
                    .payload
                    .get("parent_hash")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let miner_node_id = delivery
                    .payload
                    .get("miner_node_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let difficulty = delivery
                    .payload
                    .get("difficulty")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1);
                let nonce = delivery
                    .payload
                    .get("nonce")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let tx_ids = delivery
                    .payload
                    .get("tx_ids")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(ToString::to_string))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                // Reconstruct block from payload (simplified for simulation)
                let header = BlockHeader {
                    block_id: block_id.clone(),
                    height,
                    parent_hash,
                    miner_node_id,
                    timestamp_ms: self.sim_time_ms, // Approximation
                    difficulty,
                    nonce,
                    total_weight: 0,
                    total_fees_sats: 0,
                };
                let block = Block { header, tx_ids };

                self.process_block(block).await?;

                self.emit_event(
                    "node.block_received.v1",
                    json!({
                        "node": delivery.to,
                        "from": delivery.from,
                        "block_id": block_id,
                        "height": height,
                    }),
                    false,
                )
                .await?;
                self.update_tip(&delivery.to, NodeTip { block_id, height })
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
}
