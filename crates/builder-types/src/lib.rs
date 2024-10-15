use essential_types::ContentAddress;
use serde::{Deserialize, Serialize};

/// A failed attempt at applying a solution at a particular location within a particular block.
///
/// The builder stores these in order to provide solution submitters feedback in the case that a
/// solution could not be applied trivially and did not make it into a block.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct SolutionFailure<'a> {
    /// The number of the block in which the builder attempted to apply the solution.
    pub attempt_block_num: i64,
    /// The address of the block in which the builder attempted to apply the solution.
    pub attempt_block_addr: ContentAddress,
    /// The solution index within the block at which the builder attempted to apply the solution.
    pub attempt_solution_ix: u32,
    /// An error message describing why the builder failed to apply the solution.
    pub err_msg: std::borrow::Cow<'a, str>,
}
