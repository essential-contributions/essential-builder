//! Helpers for constructing temp views into state mutations proposed by sequences of solutions.

pub(crate) use mutations::Mutations;
pub(crate) use view::{pre_and_post_view, View};

type SolutionSetIx = usize;

mod mutations;
mod view;
