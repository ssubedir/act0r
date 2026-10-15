//! Terminal editing is an adapter concern. Pipes and JSON retain line-oriented input.
use reedline::{
    EditCommand, Emacs, ExternalPrinter, FileBackedHistory, KeyCode, KeyModifiers, Prompt,
    PromptEditMode, PromptHistorySearch, Reedline, ReedlineEvent, Signal,
    default_emacs_keybindings,
};
use std::{
    borrow::Cow,
    io::{self, BufRead, IsTerminal, Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::mpsc;

const MAX_INPUT_BYTES: usize = 65_536;

#[derive(Clone, Default)]
pub(super) struct TerminalOutput(Arc<Mutex<Option<ExternalPrinter<String>>>>);
impl TerminalOutput {
    pub(super) fn write(&self, text: &str) -> io::Result<()> {
        let mut text = text.to_owned();
        loop {
            let sink = self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(printer) = sink.as_ref() {
                match printer.sender().try_send(text) {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        let full = error.is_full();
                        text = error.into_inner();
                        if !full {
                            return Err(io::ErrorKind::BrokenPipe.into());
                        }
                    }
                }
                // Let the editor print or detach; never block its cleanup under the lock.
                drop(sink);
                thread::sleep(Duration::from_millis(1));
            } else {
                let mut stdout = io::stdout().lock();
                writeln!(stdout, "{text}")?;
                return stdout.flush();
            }
        }
    }
}

pub(super) struct Input {
    receiver: mpsc::Receiver<io::Result<String>>,
    editor: Option<(Arc<AtomicBool>, JoinHandle<()>)>,
    exiting: Arc<AtomicBool>,
}
impl Input {
    pub(super) fn new(allow_editor: bool, output: TerminalOutput) -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        let exiting = Arc::new(AtomicBool::new(false));
        let interactive = allow_editor
            && io::stdin().is_terminal()
            && io::stdout().is_terminal()
            && io::stderr().is_terminal()
            && std::env::var_os("TERM").is_none_or(|term| term != "dumb");
        let editor = if interactive {
            let printer = ExternalPrinter::new(256);
            let stopped = Arc::new(AtomicBool::new(false));
            let signal = stopped.clone();
            let exit = exiting.clone();
            let history = FileBackedHistory::new(100).map_err(io::Error::other)?;
            let mut keys = default_emacs_keybindings();
            keys.add_binding(
                KeyModifiers::NONE,
                KeyCode::Enter,
                ReedlineEvent::Edit(vec![EditCommand::InsertNewline]),
            );
            keys.add_binding(KeyModifiers::ALT, KeyCode::Enter, ReedlineEvent::Submit);
            keys.add_binding(
                KeyModifiers::CONTROL,
                KeyCode::Char('s'),
                ReedlineEvent::Submit,
            );
            keys.add_binding(
                KeyModifiers::CONTROL,
                KeyCode::Char('j'),
                ReedlineEvent::Edit(vec![EditCommand::InsertNewline]),
            );
            let reader = Reedline::create()
                .with_edit_mode(Box::new(Emacs::new(keys)))
                .with_history(Box::new(history))
                .disable_validator()
                .disable_hints()
                .with_ansi_colors(false)
                .use_bracketed_paste(true)
                .with_external_printer(printer.clone())
                .with_break_signal(signal.clone())
                .with_poll_interval(Duration::from_millis(25));
            *output
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(printer.clone());
            // The cleanup guard outlives the editor, including on errors or unwinding.
            let cleanup = Cleanup {
                output,
                printer: printer.clone(),
            };
            let handle = thread::Builder::new()
                .name("chat-terminal".into())
                .spawn(move || {
                    let _cleanup = cleanup;
                    let mut reader = reader;
                    while !signal.load(Ordering::Relaxed) {
                        let result = match reader.read_line(&ChatPrompt) {
                            Ok(Signal::Success(text)) => bounded(text),
                            Ok(Signal::CtrlC | Signal::CtrlD) => {
                                exit.store(true, Ordering::Relaxed);
                                let _ = sender.try_send(Ok("/exit".into()));
                                break;
                            }
                            Ok(Signal::ExternalBreak(_)) => break,
                            Ok(_) => continue,
                            Err(error) => Err(io::Error::other(error)),
                        };
                        match sender.try_send(result) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(input)) => {
                                // Keep drawing output even when the application has not yet
                                // consumed its previous submission. Preserve this draft.
                                let message = match input {
                                    Ok(text) => {
                                        reader.run_edit_commands(&[EditCommand::InsertString(text)]);
                                        "Previous input is still pending; this draft was kept. Send it again when ready.".to_owned()
                                    }
                                    Err(error) => error.to_string(),
                                };
                                let _ = printer.sender().try_send(message);
                            }
                            Err(_) => break,
                        }
                    }
                    drop(reader);
                })?;
            Some((stopped, handle))
        } else {
            // A blocked pipe read must not hold up worker shutdown. This thread is process-local.
            thread::Builder::new()
                .name("chat-stdin".into())
                .spawn(move || {
                    let mut stdin = io::stdin().lock();
                    while let Some(result) = read_line(&mut stdin) {
                        let failed = result.is_err();
                        if sender.blocking_send(result).is_err() || failed {
                            break;
                        }
                    }
                })?;
            None
        };
        Ok(Self {
            receiver,
            editor,
            exiting,
        })
    }
    pub(super) fn interactive(&self) -> bool {
        self.editor.is_some()
    }
    pub(super) async fn recv(&mut self) -> Option<io::Result<String>> {
        if self.exiting.load(Ordering::Relaxed) {
            return Some(Ok("/exit".into()));
        }
        let input = self.receiver.recv().await;
        if self.exiting.load(Ordering::Relaxed) {
            Some(Ok("/exit".into()))
        } else {
            input
        }
    }
}
impl Drop for Input {
    fn drop(&mut self) {
        // Release a reader waiting to hand off input before waiting for terminal restoration.
        self.receiver.close();
        if let Some((stopped, reader)) = self.editor.take() {
            stopped.store(true, Ordering::Relaxed);
            let _ = reader.join();
        }
    }
}
struct Cleanup {
    output: TerminalOutput,
    printer: ExternalPrinter<String>,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let mut sink = self
            .output
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *sink = None;
        let mut stdout = io::stdout().lock();
        while let Some(text) = self.printer.get_line() {
            let _ = writeln!(stdout, "{text}");
        }
        let _ = stdout.flush();
    }
}
fn bounded(text: String) -> io::Result<String> {
    if text.len() > MAX_INPUT_BYTES {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input exceeds 65536 UTF-8 bytes",
        ))
    } else {
        Ok(text)
    }
}
fn read_line(input: &mut impl BufRead) -> Option<io::Result<String>> {
    let mut text = String::new();
    match input
        .take((MAX_INPUT_BYTES + 3) as u64)
        .read_line(&mut text)
    {
        Ok(0) => None,
        Ok(_) if text.len() > MAX_INPUT_BYTES + 2 => Some(bounded(text)),
        Ok(_) => Some(bounded(text.trim_end_matches(['\r', '\n']).to_owned())),
        Err(error) => Some(Err(error)),
    }
}
struct ChatPrompt;
impl Prompt for ChatPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed("act0r")
    }
    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }
    fn render_prompt_indicator(&self, _: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("> ")
    }
    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("... ")
    }
    fn render_prompt_history_search_indicator(&self, _: PromptHistorySearch) -> Cow<'_, str> {
        Cow::Borrowed("search> ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn pipes_keep_individual_lines_and_utf8_bounds_while_terminal_messages_allow_newlines() {
        let mut input = Cursor::new("one\r\ntwo\n🦀\n");
        for line in ["one", "two", "🦀"] {
            assert_eq!(read_line(&mut input).unwrap().unwrap(), line);
        }
        assert!(read_line(&mut input).is_none());
        let text = format!("first\n/retry\n{}", "🦀".repeat(50));
        assert_eq!(bounded(text.clone()).unwrap(), text);
        for end in ["", "\n", "\r\n"] {
            let mut input = Cursor::new(format!("{}{end}", "a".repeat(MAX_INPUT_BYTES)));
            assert_eq!(
                read_line(&mut input).unwrap().unwrap().len(),
                MAX_INPUT_BYTES
            );
        }
        assert!(
            read_line(&mut Cursor::new("a".repeat(MAX_INPUT_BYTES + 1)))
                .unwrap()
                .is_err()
        );
        assert!(bounded("🦀".repeat(MAX_INPUT_BYTES / 4 + 1)).is_err());
    }
}
