use crate::domain::entities::{BlockHeader, BlockIndexEntry};
use std::collections::HashMap;

pub struct ConsensusService;

impl ConsensusService {
    /// Returns true if the new block creates a chain with more work than the current tip.
    pub fn is_better_chain(
        new_block: &BlockHeader,
        current_tip: &BlockHeader,
        block_index: &HashMap<String, BlockIndexEntry>,
    ) -> bool {
        let new_work = block_index
            .get(&new_block.block_id)
            .map(|e| e.cumulative_work)
            .unwrap_or(0);

        let current_work = block_index
            .get(&current_tip.block_id)
            .map(|e| e.cumulative_work)
            .unwrap_or(0);

        if new_work > current_work {
            return true;
        }

        // Tie-breaker: deterministic rule (e.g., lower hash or first seen)
        // For simulation, we stick to existing tip on ties to avoid thrashing
        false
    }

    /// Finds the common ancestor (fork point) between two block hashes.
    /// Efficiently traces back by height first, then walks back together.
    pub fn find_fork_point(
        tip_a: &str,
        tip_b: &str,
        block_index: &HashMap<String, BlockIndexEntry>,
    ) -> Option<String> {
        let mut curr_a = tip_a.to_string();
        let mut curr_b = tip_b.to_string();

        while curr_a != curr_b {
            let entry_a = block_index.get(&curr_a)?;
            let entry_b = block_index.get(&curr_b)?;

            if entry_a.header.height > entry_b.header.height {
                curr_a = entry_a.header.parent_hash.clone();
            } else if entry_b.header.height > entry_a.header.height {
                curr_b = entry_b.header.parent_hash.clone();
            } else {
                // Same height, but different blocks. Move both up.
                curr_a = entry_a.header.parent_hash.clone();
                curr_b = entry_b.header.parent_hash.clone();
            }
        }

        Some(curr_a)
    }

    /// Calculates the cumulative work for a block given its parent's work.
    pub fn calculate_cumulative_work(header: &BlockHeader, parent_work: u128) -> u128 {
        // In a real PoW, this would be parent_work + difficulty
        // For simulation, we can use height or a simple difficulty sum
        parent_work + (header.difficulty as u128)
    }

    pub fn calculate_next_difficulty(
        parent_header: &BlockHeader,
        block_index: &HashMap<String, BlockIndexEntry>,
        target_block_time_ms: u64,
        retarget_window: i64,
    ) -> u64 {
        let height = parent_header.height + 1;
        if height % retarget_window != 0 {
            return parent_header.difficulty;
        }

        // Retarget logic
        let mut curr = parent_header;
        // Go back retarget_window - 1 blocks to find the start of the window
        for _ in 0..(retarget_window - 1) {
            if let Some(entry) = block_index.get(&curr.parent_hash) {
                curr = &entry.header;
            } else {
                // If we hit genesis or end of chain before window size, just return current difficulty
                return parent_header.difficulty;
            }
        }

        let start_header = curr;
        let actual_time_ms = parent_header
            .timestamp_ms
            .saturating_sub(start_header.timestamp_ms);
        let target_time_ms = target_block_time_ms * (retarget_window as u64);

        if actual_time_ms == 0 {
            return parent_header.difficulty * 4; // Max increase
        }

        let new_difficulty =
            (parent_header.difficulty as u128 * target_time_ms as u128) / actual_time_ms as u128;

        // Clamp (max 4x change)
        let max_diff = parent_header.difficulty * 4;
        let min_diff = parent_header.difficulty / 4;

        let clamped = new_difficulty.max(min_diff as u128).min(max_diff as u128);

        clamped.max(1) as u64
    }
}
