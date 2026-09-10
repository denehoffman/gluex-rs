use super::*;

#[derive(Clone)]
struct Query(ExecutionOptions);

impl TerminalQuery for Query {
    type Error = ExecutionError;

    fn execution_options(&self) -> &ExecutionOptions {
        &self.0
    }

    fn with_execution_options(&self, options: ExecutionOptions) -> Self {
        Self(options)
    }

    fn interruption_error(&self, failure: ExecutionError) -> Self::Error {
        failure
    }
}

#[test]
fn stream_budget_counts_cumulative_active_work_but_not_idle_time() {
    let query = Query(ExecutionOptions::timeout(Duration::from_millis(1_000)));
    let mut budget = StreamBudget::new(query.execution_options());

    budget
        .execute(&query, |_| {
            std::thread::sleep(Duration::from_millis(600));
            Ok(())
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(1_100));
    let failure = budget
        .execute(&query, |_| {
            std::thread::sleep(Duration::from_millis(600));
            Ok(())
        })
        .unwrap_err();

    assert_eq!(failure, ExecutionError::Timeout);
}
