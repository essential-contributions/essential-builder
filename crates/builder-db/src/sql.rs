//! Provides the SQL statements used by `essential-builder-db` via `const` `str`s.

/// Short-hand for including an SQL string from the `sql/` subdir at compile time.
macro_rules! include_sql_str {
    ($subpath:expr) => {
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/sql/", $subpath))
    };
}

/// Short-hand for declaring a `const` SQL str and presenting the SQL via the doc comment.
macro_rules! decl_const_sql_str {
    ($name:ident, $subpath:expr) => {
        /// ```sql
        #[doc = include_sql_str!($subpath)]
        /// ```
        pub const $name: &str = include_sql_str!($subpath);
    };
}

/// Table creation statements.
pub mod create {
    decl_const_sql_str!(SOLUTION, "create/solution.sql");
    decl_const_sql_str!(SUBMISSION, "create/submission.sql");
    decl_const_sql_str!(SOLUTION_FAILURE, "create/solution_failure.sql");
}

/// Statements for deleting rows from tables.
pub mod delete {
    decl_const_sql_str!(SOLUTION, "delete/solution.sql");
    decl_const_sql_str!(
        OLDEST_SOLUTION_FAILURES,
        "delete/oldest_solution_failures.sql"
    );
}

/// Statements for inserting rows into the tables.
pub mod insert {
    decl_const_sql_str!(SOLUTION, "insert/solution.sql");
    decl_const_sql_str!(SUBMISSION, "insert/submission.sql");
    decl_const_sql_str!(SOLUTION_FAILURE, "insert/solution_failure.sql");
}

/// Statements for making queries.
pub mod query {
    decl_const_sql_str!(GET_SOLUTION, "query/get_solution.sql");
    decl_const_sql_str!(LIST_SOLUTIONS, "query/list_solutions.sql");
    decl_const_sql_str!(LIST_SUBMISSIONS, "query/list_submissions.sql");
    decl_const_sql_str!(
        LATEST_SOLUTION_FAILURES,
        "query/latest_solution_failures.sql"
    );
}

pub mod table {
    use super::create;

    /// A table's name along with its create statement.
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
    pub struct Table {
        /// The name of the table as declared in the create statement.
        pub name: &'static str,
        /// The table's create statement.
        pub create: &'static str,
    }

    impl Table {
        const fn new(name: &'static str, create: &'static str) -> Self {
            Self { name, create }
        }
    }

    pub const SOLUTION: Table = Table::new("solution", create::SOLUTION);
    pub const SUBMISSION: Table = Table::new("submission", create::SUBMISSION);
    pub const SOLUTION_FAILURE: Table = Table::new("solution_failure", create::SOLUTION_FAILURE);

    /// All tables in a list. Useful for initialisation and testing.
    pub const ALL: &[Table] = &[SOLUTION, SUBMISSION, SOLUTION_FAILURE];
}
