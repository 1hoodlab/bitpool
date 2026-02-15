#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifficultyState {
    pub height_anchor: i64,
    pub target: String,
    pub window_n: i32,
    pub anchor_time_ms: i64,
}
