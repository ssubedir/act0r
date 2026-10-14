use super::{Engine, Error, SearchService, TaskService, ports::*};
use crate::domain::{benchmark::*, task::FinalTests, *};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
pub struct BenchmarkService {
    engine: Engine,
    tasks: TaskService,
    search: Arc<SearchService>,
    store: Arc<dyn BenchmarkStore>,
    workspaces: Arc<dyn CandidateWorkspaces>,
}
impl BenchmarkService {
    pub fn new(
        engine: Engine,
        tasks: TaskService,
        search: Arc<SearchService>,
        store: Arc<dyn BenchmarkStore>,
        workspaces: Arc<dyn CandidateWorkspaces>,
    ) -> Self {
        Self {
            engine,
            tasks,
            search,
            store,
            workspaces,
        }
    }
    pub async fn start(
        &self,
        config: BenchmarkConfig,
        base: PathBuf,
    ) -> Result<BenchmarkId, Error> {
        config.validate()?;
        let mut inputs = Vec::new();
        for case in &config.cases {
            let (baseline, final_tests) = self
                .workspaces
                .freeze_case(
                    base.join(&case.source),
                    base.join(&case.final_tests_source),
                    TaskId::new(),
                    TaskId::new(),
                )
                .await?;
            inputs.push(FrozenCase {
                baseline,
                final_tests,
            });
        }
        let job = Benchmark::new(config, inputs, self.engine.clock.now_ms())?;
        let id = job.record().id;
        self.store.create_benchmark(job).await?;
        Ok(id)
    }
    pub async fn status(&self, id: BenchmarkId) -> Result<Benchmark, Error> {
        self.store.benchmark(id).await
    }
}
impl WorkCoordinator for BenchmarkService {
    fn advance(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            for id in self.store.active_benchmarks().await? {
                let job = self.store.benchmark(id).await?;
                match job.next_step() {
                    Some(BenchmarkStep::Begin(task_id)) => {
                        self.store
                            .begin_trial(id, task_id, self.engine.clock.now_ms())
                            .await?;
                        return Ok(true);
                    }
                    Some(BenchmarkStep::Prepare(task_id)) => {
                        let trial = job.trial(task_id)?;
                        let input = &job.record().inputs[trial.case_index as usize];
                        let cfg = job
                            .record()
                            .config
                            .task_config(trial.case_index as usize, trial.candidates)?;
                        let final_tests = FinalTests {
                            snapshot: input.final_tests.clone(),
                            test_command: job.record().config.final_test_command.clone(),
                        };
                        let prepared = if trial.strategy == BenchmarkStrategy::BoundedSearch {
                            let snapshot = self
                                .workspaces
                                .branch_snapshot(input.baseline.clone(), task_id)
                                .await;
                            match snapshot {
                                Ok(snapshot) => self
                                    .search
                                    .prepare(
                                        trial
                                            .search_id
                                            .ok_or(crate::domain::DomainError::InvalidState)?,
                                        job.record()
                                            .config
                                            .search_config(trial.case_index as usize)?,
                                        snapshot,
                                        trial.run_ids[0],
                                        Some(final_tests),
                                        self.engine.clock.now_ms(),
                                    )
                                    .await
                                    .map(|(search, task, root)| (task, vec![root], Some(search))),
                                Err(error) => Err(error),
                            }
                        } else {
                            self.tasks
                                .prepare_trial(
                                    task_id,
                                    cfg,
                                    input.baseline.clone(),
                                    trial.run_ids.clone(),
                                    final_tests,
                                    self.engine.clock.now_ms(),
                                )
                                .await
                                .map(|(task, runs)| (task, runs, None))
                        };
                        match prepared {
                            Ok((task, runs, search)) => {
                                self.store.activate_trial(id, task, runs, search).await?
                            }
                            Err(error) => {
                                self.store
                                    .fail_preparation(
                                        id,
                                        task_id,
                                        error.to_string(),
                                        self.engine.clock.now_ms(),
                                    )
                                    .await?
                            }
                        }
                        return Ok(true);
                    }
                    None => {}
                }
            }
            Ok(false)
        })
    }
    fn has_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            for id in self.store.active_benchmarks().await? {
                if self.store.benchmark(id).await?.next_step().is_some() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}
