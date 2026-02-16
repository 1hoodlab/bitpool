use crate::domain::entities::NodeSendCommand;
use crate::helper::deterministic_rng::{hash_str64, splitmix64};

pub(super) fn stable_seed_for_command(
    base_seed: u64,
    cmd: &NodeSendCommand,
    schedule_counter: u64,
) -> u64 {
    let mut seed = splitmix64(base_seed ^ schedule_counter);
    seed ^= hash_str64(&cmd.from);
    seed = splitmix64(seed ^ hash_str64(&cmd.to));
    seed = splitmix64(seed ^ (cmd.kind.rank() as u64));
    seed
}
