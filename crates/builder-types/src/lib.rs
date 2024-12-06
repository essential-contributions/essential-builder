use essential_types::ContentAddress;
use serde::{Deserialize, Serialize};

/// A failed attempt at applying a solution set at a particular location within a particular block.
///
/// The builder stores these in order to provide solution set submitters feedback in the case that a
/// solution set could not be applied trivially and did not make it into a block.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct SolutionSetFailure<'a> {
    /// The number of the block in which the builder attempted to apply the solution set.
    pub attempt_block_num: i64,
    /// The address of the block in which the builder attempted to apply the solution set.
    pub attempt_block_addr: ContentAddress,
    /// The solution set index within the block at which the builder attempted to apply the solution set.
    pub attempt_solution_set_ix: u32,
    /// An error message describing why the builder failed to apply the solution set.
    pub err_msg: std::borrow::Cow<'a, str>,
}
