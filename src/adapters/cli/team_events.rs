//! Read-only terminal streaming and local reconnect checkpoints.
use crate::{
    application::{Error, TeamEventPage, TeamEventService},
    domain::{MessageId, TeamId, timeline::*},
};
use clap::Args;
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Args)]
pub(super) struct PageOptions {
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=512))]
    limit: u32,
    /// Stored event JSON plus its compact envelope, excluding page metadata.
    #[arg(long, default_value_t = 262144, value_parser = clap::value_parser!(u32).range(1..=16777216))]
    max_bytes: u32,
}
impl PageOptions {
    pub(super) fn query(&self, after: Option<EventCursor>) -> EventQuery {
        EventQuery {
            after,
            limit: self.limit,
            max_bytes: self.max_bytes,
        }
    }
}

pub(super) fn print_page(page: &TeamEventPage, json_output: bool) -> Result<(), Error> {
    let mut output = std::io::stdout().lock();
    if json_output {
        serde_json::to_writer_pretty(&mut output, page)?;
        writeln!(output)?;
        output.flush()?;
    } else {
        emit(&mut output, page, false)?;
    }
    Ok(())
}
fn emit(output: &mut impl Write, page: &TeamEventPage, json_output: bool) -> Result<(), Error> {
    if json_output {
        serde_json::to_writer(&mut *output, page)?;
        writeln!(output)?;
    } else {
        writeln!(
            output,
            "Team: {} ({})\nGoal: {}",
            serde_json::to_string(&page.team.name)?,
            page.team.id,
            page.root_task_id
        )?;
        for item in &page.events {
            // Escape terminal controls and bound previews; JSON output retains the complete event.
            let payload = serde_json::to_string(&item.event.payload)?;
            let mut preview: String = payload.chars().take(240).collect();
            if payload.chars().count() > 240 {
                preview.push('…');
            }
            writeln!(
                output,
                "{} {} member={} actor={} task={} run={} sequence={} kind={} payload={}",
                item.position,
                item.event.at,
                serde_json::to_string(&item.member)?,
                item.actor_id,
                item.task_id,
                item.event.run_id,
                item.event.sequence,
                serde_json::to_string(&item.event.kind)?,
                preview
            )?;
        }
        if let Some(event) = &page.oversized_event {
            writeln!(
                output,
                "Event {} at {} requires --max-bytes {}; cursor retained before it.",
                event.event_id, event.position, event.required_bytes
            )?;
        }
        writeln!(
            output,
            "Cursor: {}\nMore: {}",
            page.next_cursor, page.has_more
        )?;
    }
    output.flush()?;
    Ok(())
}

pub(super) async fn watch(
    service: &TeamEventService,
    team: TeamId,
    task: MessageId,
    mut query: EventQuery,
    cursor_file: Option<PathBuf>,
    poll_ms: u64,
    json_output: bool,
) -> Result<(), Error> {
    let checkpoint = cursor_file.as_deref().map(CursorFile::open).transpose()?;
    if let Some(file) = &checkpoint {
        query.after = file.load()?;
    }
    watch_until(
        service,
        (team, task),
        query,
        Duration::from_millis(poll_ms),
        (
            &mut std::io::stdout().lock(),
            json_output,
            checkpoint.as_ref(),
        ),
        crate::adapters::tokio_worker::shutdown_signal(),
    )
    .await
}

async fn watch_until(
    service: &TeamEventService,
    scope: (TeamId, MessageId),
    mut query: EventQuery,
    poll: Duration,
    (output, json_output, checkpoint): (&mut impl Write, bool, Option<&CursorFile>),
    shutdown: impl std::future::Future<Output = std::io::Result<()>>,
) -> Result<(), Error> {
    tokio::pin!(shutdown);
    let mut initial = true;
    loop {
        let page = tokio::select! {
            biased;
            result = &mut shutdown => { result?; return Ok(()); }
            result = service.read(scope.0, scope.1, query) => result?,
        };
        if initial || !page.events.is_empty() || page.oversized_event.is_some() {
            emit(output, &page, json_output)?;
            if let Some(file) = checkpoint {
                file.save(page.next_cursor)?;
            }
            initial = false;
        }
        query.after = Some(page.next_cursor);
        if let Some(event) = page.oversized_event {
            return Err(Error::Unavailable(format!(
                "event {} requires --max-bytes {} (maximum {}); reconnect using the last emitted cursor",
                event.event_id, event.required_bytes, MAX_EVENT_PAGE_BYTES
            )));
        }
        if !page.has_more {
            tokio::select! {
                biased;
                result = &mut shutdown => { result?; return Ok(()); }
                _ = tokio::time::sleep(poll) => {}
            }
        }
    }
}

struct CursorFile {
    path: PathBuf,
    _lease: File,
}
fn reject_link(path: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if crate::adapters::workspace::link(&metadata) || !metadata.is_file() => Err(
            Error::Unavailable("cursor or lock must be a regular file".into()),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
impl CursorFile {
    fn open(path: &Path) -> Result<Self, Error> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = fs::canonicalize(parent)?;
        let name = path
            .file_name()
            .ok_or_else(|| Error::Unavailable("cursor file requires a filename".into()))?;
        let path = parent.join(name);
        reject_link(&path)?;
        let mut lock = path.as_os_str().to_owned();
        lock.push(".lock");
        let lock = PathBuf::from(lock);
        reject_link(&lock)?;
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::Unavailable(
                    "another watcher owns this cursor file".into(),
                ));
            }
            Err(fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        Ok(Self { path, _lease: file })
    }
    fn load(&self) -> Result<Option<EventCursor>, Error> {
        reject_link(&self.path)?;
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut text = String::new();
        file.take(257).read_to_string(&mut text)?;
        if text.len() > 256 {
            return Err(Error::Unavailable("cursor file exceeds 256 bytes".into()));
        }
        Ok(Some(text.trim().parse()?))
    }
    fn save(&self, cursor: EventCursor) -> Result<(), Error> {
        reject_link(&self.path)?;
        let mut temp =
            tempfile::NamedTempFile::new_in(self.path.parent().expect("canonical parent"))?;
        writeln!(temp, "{cursor}")?;
        temp.as_file().sync_all()?;
        temp.persist(&self.path).map_err(|e| Error::Io(e.error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::{OversizedTimelineEvent, TeamTimelineEvent, ports::*},
        domain::{EventId, EventStreamId, HistoryEvent, RunId, teams::*},
    };
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Timeline {
        calls: AtomicUsize,
        team: TeamId,
        task: MessageId,
        stream: EventStreamId,
    }
    impl TeamTaskEventsPort for Timeline {
        fn team_task_events(
            &self,
            team: TeamId,
            task: MessageId,
            query: EventQuery,
        ) -> PortFuture<'_, TeamEventPage> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let cursor = EventCursor::new(self.stream, team, task, 1)?;
                Ok(TeamEventPage {
                    team: TeamSummary {
                        id: team,
                        name: "fixture".into(),
                        status: TeamStatus::Ready,
                        error: None,
                    },
                    requested_task_id: task,
                    root_task_id: task,
                    query,
                    events: vec![TeamTimelineEvent {
                        position: 1,
                        task_id: task,
                        actor_id: crate::domain::AgentId::new(),
                        member: Some("planner".into()),
                        event: HistoryEvent {
                            id: EventId::new(),
                            run_id: RunId::new(),
                            sequence: 1,
                            kind: "run_started".into(),
                            payload_version: 1,
                            payload: json!({}),
                            activity_id: None,
                            attempt_id: None,
                            at: 1,
                        },
                    }],
                    event_bytes: 500,
                    snapshot_position: 2,
                    next_cursor: cursor,
                    has_more: true,
                    oversized_event: Some(OversizedTimelineEvent {
                        position: 2,
                        event_id: EventId::new(),
                        required_bytes: 10000,
                    }),
                })
            })
        }
    }
    fn fixture() -> (Arc<Timeline>, TeamEventService) {
        let port = Arc::new(Timeline {
            calls: AtomicUsize::new(0),
            team: TeamId::new(),
            task: MessageId::new(),
            stream: EventStreamId::new(),
        });
        (port.clone(), TeamEventService::new(port))
    }
    struct FailedOutput {
        fail_flush: bool,
    }
    impl Write for FailedOutput {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.fail_flush {
                Ok(bytes.len())
            } else {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }
    #[tokio::test]
    async fn failed_output_or_flush_never_advances_the_saved_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let file = CursorFile::open(&dir.path().join("watch.cursor")).unwrap();
        let (port, service) = fixture();
        let original = EventCursor::new(port.stream, port.team, port.task, 0).unwrap();
        file.save(original).unwrap();
        for fail_flush in [false, true] {
            let result = watch_until(
                &service,
                (port.team, port.task),
                EventQuery::default(),
                Duration::ZERO,
                (&mut FailedOutput { fail_flush }, true, Some(&file)),
                std::future::pending(),
            )
            .await;
            assert!(result.is_err());
            assert_eq!(file.load().unwrap(), Some(original));
        }
    }
    #[tokio::test]
    async fn oversized_event_stops_after_flushing_and_checkpointing_prior_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watch.cursor");
        let file = CursorFile::open(&path).unwrap();
        let (port, service) = fixture();
        let mut output = Vec::new();
        let result = watch_until(
            &service,
            (port.team, port.task),
            EventQuery::default(),
            Duration::ZERO,
            (&mut output, true, Some(&file)),
            std::future::pending(),
        )
        .await;
        assert!(
            matches!(result,Err(Error::Unavailable(message)) if message.contains("--max-bytes 10000"))
        );
        assert_eq!(port.calls.load(Ordering::SeqCst), 1);
        assert_eq!(file.load().unwrap().unwrap().position(), 1);
        let page: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(page["events"].as_array().unwrap().len(), 1);
        assert_eq!(page["oversized_event"]["position"], 2);
        // Replacement works repeatedly, and a second writer cannot clobber an active checkpoint.
        file.save(file.load().unwrap().unwrap()).unwrap();
        assert!(CursorFile::open(&path).is_err());
        drop(file);
        assert!(CursorFile::open(&path).unwrap().load().unwrap().is_some());
    }
    #[tokio::test]
    async fn ready_shutdown_does_not_read_emit_or_create_a_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watch.cursor");
        let file = CursorFile::open(&path).unwrap();
        let (port, service) = fixture();
        let mut output = Vec::new();
        watch_until(
            &service,
            (port.team, port.task),
            EventQuery::default(),
            Duration::ZERO,
            (&mut output, true, Some(&file)),
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert_eq!(port.calls.load(Ordering::SeqCst), 0);
        assert!(output.is_empty());
        assert!(!path.exists());
    }
    #[test]
    fn oversized_and_malformed_checkpoint_files_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watch.cursor");
        for text in ["".to_owned(), "invalid".to_owned(), "a".repeat(257)] {
            fs::write(&path, &text).unwrap();
            assert!(CursorFile::open(&path).unwrap().load().is_err());
            assert_eq!(fs::read_to_string(&path).unwrap(), text);
        }
    }
}
