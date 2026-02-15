use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DifficultyState {
    pub current_target: u64,
    pub last_retarget_height: i64,
    pub last_retarget_time_ms: i64,
}

impl Default for DifficultyState {
    fn default() -> Self {
        Self {
            current_target: 1, // Simplified difficulty
            last_retarget_height: 0,
            last_retarget_time_ms: 0,
        }
    }
}
