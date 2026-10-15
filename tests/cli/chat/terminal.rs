//! Exercise real terminal input through ConPTY on Windows and a PTY on Unix.
use super::Provider;
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Terminal {
    child: Box<dyn Child + Send + Sync>,
    master: Box<dyn MasterPty + Send>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    output: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    bytes: Vec<u8>,
}
impl Terminal {
    fn start(db: &Path, endpoint: &str, prompt: Option<&str>) -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 40,
                cols: 100,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_act0r"));
        command.args([
            "--db",
            db.to_str().unwrap(),
            "chat",
            "--model",
            "terminal-fixture",
            "--endpoint",
            endpoint,
            "--action-format",
            "function-calling",
        ]);
        if let Some(prompt) = prompt {
            command.arg(prompt);
        }
        for (name, _) in std::env::vars_os() {
            if name
                .to_string_lossy()
                .to_ascii_uppercase()
                .ends_with("_API_KEY")
            {
                command.env_remove(name);
            }
        }
        command.env_remove("ACT0R_MCP_CONFIG");
        command.env_remove("ACT0R_MCP_FILESYSTEM_CONFIG");
        command.env("TERM", "xterm-256color");
        let child = pair.slave.spawn_command(command).unwrap();
        drop(pair.slave);
        let writer = Arc::new(Mutex::new(pair.master.take_writer().unwrap()));
        let mut reader = pair.master.try_clone_reader().unwrap();
        let replies = writer.clone();
        let (sender, output) = tokio::sync::mpsc::unbounded_channel();
        std::thread::spawn(move || {
            let mut buffer = [0; 4096];
            let mut matched = 0;
            while let Ok(count) = reader.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                // A minimal terminal-emulator reply for cursor-position requests.
                for byte in &buffer[..count] {
                    let query = b"\x1b[6n";
                    matched = if *byte == query[matched] {
                        matched + 1
                    } else {
                        usize::from(*byte == query[0])
                    };
                    if matched == query.len() {
                        let _ = replies.lock().unwrap().write_all(b"\x1b[1;1R");
                        matched = 0;
                    }
                }
                if sender.send(buffer[..count].to_vec()).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            master: pair.master,
            writer,
            output,
            bytes: Vec::new(),
        }
    }
    fn send(&self, text: &str) {
        let mut writer = self.writer.lock().unwrap();
        writer.write_all(text.as_bytes()).unwrap();
        writer.flush().unwrap();
    }
    fn paste(&self, text: &str) {
        // Windows terminal frontends send CR for Enter; an LF-only VT byte is not
        // a Windows console key event. Model that transport conversion here.
        #[cfg(windows)]
        let text = text.replace("\r\n", "\n").replace('\n', "\r\n");
        self.send(&format!("\x1b[200~{text}\x1b[201~"));
    }
    async fn wait(&mut self, text: &str) {
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if String::from_utf8_lossy(&self.bytes).contains(text) {
                    break;
                }
                self.bytes.extend(
                    self.output
                        .recv()
                        .await
                        .expect("terminal closed before the expected output"),
                );
                assert!(
                    self.bytes.len() < 1024 * 1024,
                    "terminal output exceeded the fixture bound"
                );
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "terminal did not show {text:?}: {}",
            String::from_utf8_lossy(&self.bytes)
        );
    }
    async fn finish(&mut self) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                while let Ok(bytes) = self.output.try_recv() {
                    self.bytes.extend(bytes);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("terminal child did not exit");
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn contains_text(value: &Value, text: &str) -> bool {
    match value {
        Value::String(value) => value == text,
        Value::Object(fields) => fields.values().any(|value| contains_text(value, text)),
        Value::Array(values) => values.iter().any(|value| contains_text(value, text)),
        _ => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiline_paste_is_edited_then_submitted_as_one_request_and_one_answer() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("paste.db");
    let model = Provider::new(vec![
        json!({"kind":"ask_human","question":"Keep both lines?"}),
        json!({"kind":"finish","summary":"The multiline answer was saved."}),
    ])
    .await;
    let mut terminal = Terminal::start(&db, &model.endpoint, None);
    terminal.wait("act0r").await;
    terminal.wait("\x1b[?2004h").await;
    let text = "Inspect these lines:\n/retry\n🦀";
    terminal.paste("Inspect these lines:\r\n/retry\r\n🦀");
    terminal.wait("🦀").await;
    assert_eq!(model.calls(), 0, "pasting must not submit work");
    terminal.send("\x13");
    terminal.wait("Keep both lines?").await;
    assert_eq!(model.calls(), 1);
    let prompt: Value = serde_json::from_str(
        model.requests.lock().unwrap()[0]["messages"][1]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(
        contains_text(&prompt, text),
        "multiline goal was split or changed: {prompt}"
    );
    terminal.paste("Keep both.\n/retry");
    terminal.send("\x1b\r");
    terminal.wait("The multiline answer was saved.").await;
    let prompt: Value = serde_json::from_str(
        model.requests.lock().unwrap()[1]["messages"][1]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(
        contains_text(&prompt, "Keep both.\n/retry"),
        "multiline answer was split or interpreted as a command: {prompt}"
    );
    terminal.send("/exit\x13");
    terminal.finish().await;
    assert_eq!(model.calls(), 2);
    let (_, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let sessions = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .conversation_sessions(Default::default())
        .await
        .unwrap();
    assert_eq!(sessions.items.len(), 1);
    let conversation = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .continuous_conversation(sessions.items[0].session_id, None, None)
        .await
        .unwrap();
    assert_eq!(conversation.turns.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draft_survives_background_output_editing_and_resize_then_ctrl_c_releases_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("draft.db");
    let gate = Arc::new(tokio::sync::Notify::new());
    let model = Provider::with_first_gate(
        vec![
            json!({"kind":"ask_human","question":"Draft survived activity?"}),
            json!({"kind":"finish","summary":"Edited draft received."}),
        ],
        Some(gate.clone()),
    )
    .await;
    let mut terminal = Terminal::start(&db, &model.endpoint, Some("Ask before finishing"));
    terminal.wait("Requesting model decision").await;
    terminal.send("Keep API");
    terminal.wait("Keep API").await;
    terminal
        .master
        .resize(PtySize {
            rows: 30,
            cols: 72,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    gate.notify_one();
    terminal.wait("Draft survived activity?").await;
    terminal.send("\x01Please \x05\rwith tests\x13");
    terminal.wait("Edited draft received.").await;
    let prompt: Value = serde_json::from_str(
        model.requests.lock().unwrap()[1]["messages"][1]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(
        contains_text(&prompt, "Please Keep API\nwith tests"),
        "draft or newline editing was lost: {prompt}"
    );
    terminal.send("\x03");
    terminal.finish().await;
    let (engine, _) = act0r::bootstrap::local_app(&db).await.unwrap();
    let _owner = engine.worker().await.unwrap();
    assert_eq!(model.calls(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctrl_d_leaves_an_unsent_draft_without_admitting_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("eof.db");
    let model = Provider::new(vec![]).await;
    let mut terminal = Terminal::start(&db, &model.endpoint, None);
    terminal.wait("\x1b[?2004h").await;
    terminal.send("discard this\x15\x04");
    terminal.finish().await;
    assert_eq!(model.calls(), 0);
    let (engine, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let _owner = engine.worker().await.unwrap();
    assert!(
        agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .conversation_sessions(Default::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
}
