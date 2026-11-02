//! Report the selected thread from the terminal client, never from the shared app server.
//!
//! A foreground process match for this TUI's PID establishes the terminal;
//! subsequent updates follow that terminal, without trusting inherited pane hints.

use serde_json::Value;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const DEADLINE: Duration = Duration::from_millis(500);
const REFRESH: Duration = Duration::from_secs(3);
const MAX_RESPONSE_BYTES: u64 = 2 * 1024 * 1024;

pub(crate) struct HerdrSessionReporter {
    selected: watch::Sender<Option<String>>,
    task: JoinHandle<()>,
}

impl HerdrSessionReporter {
    pub(crate) fn from_env() -> Option<Self> {
        if std::env::var("HERDR_ENV").ok().as_deref() != Some("1") {
            return None;
        }
        let socket = PathBuf::from(std::env::var_os("HERDR_SOCKET_PATH")?);
        if socket.as_os_str().is_empty() {
            return None;
        }
        Some(Self::start(socket, std::process::id()))
    }

    fn start(socket: PathBuf, pid: u32) -> Self {
        let (selected, receiver) = watch::channel(None);
        let task = tokio::spawn(run(socket, pid, receiver));
        Self { selected, task }
    }

    pub(crate) fn select(&self, thread: Option<String>) {
        self.selected.send_if_modified(|current| {
            if *current == thread {
                false
            } else {
                *current = thread;
                true
            }
        });
    }

    pub(crate) async fn shutdown(mut self) {
        // Dropping the last sender asks the worker to clear its registration. This is
        // bounded even when Herdr has exited; the UI never waits on socket IO in a draw.
        drop(self.selected);
        if tokio::time::timeout(Duration::from_secs(2), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Owner {
    pane: String,
    terminal: String,
    started_at: String,
}

async fn request(socket: &std::path::Path, method: &str, params: Value) -> Option<Value> {
    tokio::time::timeout(DEADLINE, async {
        let mut stream = codex_uds::UnixStream::connect(socket).await.ok()?;
        #[cfg(windows)]
        stream.ensure_non_elevated_peer().ok()?;
        let id = uuid::Uuid::new_v4().to_string();
        let mut packet =
            serde_json::to_vec(&json!({"id": id, "method": method, "params": params})).ok()?;
        packet.push(b'\n');
        stream.write_all(&packet).await.ok()?;
        let mut response = Vec::new();
        BufReader::new(stream.take(MAX_RESPONSE_BYTES + 1))
            .read_until(b'\n', &mut response)
            .await
            .ok()?;
        if response.len() as u64 > MAX_RESPONSE_BYTES || response.last() != Some(&b'\n') {
            return None;
        }
        let response: Value = serde_json::from_slice(&response).ok()?;
        if response.get("id")?.as_str()? != id || response.get("error").is_some() {
            return None;
        }
        response
            .get("result")
            .filter(|value| value.is_object())
            .cloned()
    })
    .await
    .ok()
    .flatten()
}

fn process_stamp(info: &Value, pane: &str, pid: u32) -> Option<String> {
    if info.get("pane_id")?.as_str()? != pane {
        return None;
    }
    let mut matches = info
        .get("foreground_processes")?
        .as_array()?
        .iter()
        .filter(|process| process.get("pid").and_then(Value::as_u64) == Some(u64::from(pid)));
    let process = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    let stamp = process.get("started_at")?.as_str()?;
    (!stamp.is_empty()).then(|| stamp.to_owned())
}

async fn discover(socket: &std::path::Path, pid: u32, terminal: Option<&str>) -> Option<Owner> {
    let snapshot = request(socket, "session.snapshot", json!({})).await?;
    let agents = snapshot.get("snapshot")?.get("agents")?.as_array()?;
    let mut owner = None;
    for agent in agents {
        if agent.get("agent").and_then(Value::as_str) != Some("codex") {
            continue;
        }
        let Some(pane) = agent.get("pane_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(candidate_terminal) = agent.get("terminal_id").and_then(Value::as_str) else {
            continue;
        };
        if terminal.is_some_and(|terminal| terminal != candidate_terminal) {
            continue;
        }
        let info = request(socket, "pane.process_info", json!({"pane_id": pane})).await?;
        let Some(stamp) = info
            .get("process_info")
            .and_then(|info| process_stamp(info, pane, pid))
        else {
            continue;
        };
        if owner.is_some() {
            return None;
        }
        owner = Some(Owner {
            pane: pane.to_owned(),
            terminal: candidate_terminal.to_owned(),
            started_at: stamp,
        });
    }
    owner
}

fn report_params(owner: &Owner, pid: u32, seq: u64, thread: Option<&str>) -> Value {
    let mut params = json!({
        "pane_id": owner.pane,
        "source": "herdr:codex-client",
        "agent": "codex",
        "session_start_source": if thread.is_some() { "select" } else { "clear" },
        "seq": seq,
        "client_process_id": pid,
        "client_process_started_at": owner.started_at,
        "client_terminal_id": owner.terminal,
    });
    if let Some(thread) = thread {
        params["agent_session_id"] = json!(thread);
    }
    params
}

async fn run(socket: PathBuf, pid: u32, mut selected: watch::Receiver<Option<String>>) {
    let mut terminal = None;
    let mut seq = 0_u64;
    loop {
        let closing = selected.has_changed().is_err();
        let thread = if closing {
            None
        } else {
            selected.borrow_and_update().clone()
        };
        if let Some(owner) = discover(&socket, pid, terminal.as_deref()).await {
            terminal = Some(owner.terminal.clone());
            // Coalesce a switch that happened while discovering. A single worker serializes
            // reports, so a slow response cannot reorder older and newer selections.
            if !closing && selected.has_changed().unwrap_or(true) {
                continue;
            }
            let Some(next_seq) = seq.checked_add(1) else {
                return;
            };
            seq = next_seq;
            let _ = request(
                &socket,
                "pane.report_agent_session",
                report_params(&owner, pid, seq, thread.as_deref()),
            )
            .await;
        }
        if closing {
            return;
        }
        tokio::select! {
            _ = selected.changed() => {},
            _ = tokio::time::sleep(REFRESH) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;

    #[test]
    fn proof_requires_exact_pane_pid_and_nonempty_generation() {
        let info = json!({"pane_id":"p2", "foreground_processes":[{"pid":73,"started_at":"generation-1"}]});
        assert_eq!(
            process_stamp(&info, "p2", 73).as_deref(),
            Some("generation-1")
        );
        assert_eq!(process_stamp(&info, "p1", 73), None);
        assert_eq!(process_stamp(&info, "p2", 74), None);
        assert_eq!(
            process_stamp(
                &json!({"pane_id":"p2", "foreground_processes":[{"pid":73}]}),
                "p2",
                73
            ),
            None
        );
        assert_eq!(
            process_stamp(
                &json!({"pane_id":"p2", "foreground_processes":[{"pid":73,"started_at":""}]}),
                "p2",
                73
            ),
            None
        );
        assert_eq!(
            process_stamp(
                &json!({"pane_id":"p2", "foreground_processes":[{"pid":73,"started_at":"a"},{"pid":73,"started_at":"a"}]}),
                "p2",
                73
            ),
            None
        );
    }

    // An isolated socket exercises the actual wire format and background worker.
    // The first pane deliberately belongs to another client of a shared server.
    struct Server {
        _directory: tempfile::TempDir,
        socket: PathBuf,
        pane: Arc<Mutex<String>>,
        reports: mpsc::UnboundedReceiver<Value>,
        task: JoinHandle<()>,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Server {
        async fn start() -> Self {
            let directory = tempfile::tempdir().expect("tempdir");
            let socket = directory.path().join("herdr.sock");
            let mut listener = codex_uds::UnixListener::bind(&socket).await.expect("bind");
            let pane = Arc::new(Mutex::new("p2".to_owned()));
            let current_pane = Arc::clone(&pane);
            let (sender, reports) = mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                loop {
                    let stream = listener.accept().await.expect("accept");
                    let mut stream = BufReader::new(stream);
                    let mut line = String::new();
                    stream.read_line(&mut line).await.expect("read");
                    let request: Value = serde_json::from_str(&line).expect("request");
                    let pane = current_pane.lock().await.clone();
                    let result = match request["method"].as_str().expect("method") {
                        "session.snapshot" => json!({"snapshot":{"agents":[
                            {"pane_id":"wrong","terminal_id":"other-terminal","agent":"codex"},
                            {"pane_id":pane,"terminal_id":"own-terminal","agent":"codex"}
                        ]}}),
                        "pane.process_info" => {
                            let requested_pane =
                                request["params"]["pane_id"].as_str().expect("pane");
                            json!({"process_info":{"pane_id":requested_pane,"foreground_processes":[
                                {"pid":if requested_pane == pane {73} else {74},"started_at":"generation-1"}
                            ]}})
                        }
                        "pane.report_agent_session" => {
                            sender
                                .send(request["params"].clone())
                                .expect("report receiver");
                            json!({"type":"ok"})
                        }
                        method => panic!("unexpected method {method}"),
                    };
                    let mut response =
                        serde_json::to_vec(&json!({"id":request["id"],"result":result}))
                            .expect("serialize");
                    response.push(b'\n');
                    let _ = stream.get_mut().write_all(&response).await;
                }
            });
            Self {
                _directory: directory,
                socket,
                pane,
                reports,
                task,
            }
        }

        async fn report(&mut self, thread: Option<&str>) -> Value {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let report = self.reports.recv().await.expect("report");
                    if report.get("agent_session_id").and_then(Value::as_str) == thread {
                        return report;
                    }
                }
            })
            .await
            .expect("report deadline")
        }
    }

    #[tokio::test]
    async fn selection_switch_clear_and_exit_follow_the_client_process() {
        let mut server = Server::start().await;
        let reporter = HerdrSessionReporter::start(server.socket.clone(), 73);
        reporter.select(Some("root-thread".to_owned()));
        let root = server.report(Some("root-thread")).await;
        assert_eq!(root["pane_id"], "p2");
        assert_eq!(root["client_terminal_id"], "own-terminal");
        assert_eq!(root["client_process_id"], 73);
        assert_eq!(root["client_process_started_at"], "generation-1");
        assert_eq!(root["source"], "herdr:codex-client");
        assert_eq!(root["session_start_source"], "select");
        reporter.select(Some("fork-thread".to_owned()));
        let fork = server.report(Some("fork-thread")).await;
        assert!(fork["seq"].as_u64() > root["seq"].as_u64());
        reporter.select(None);
        let clear = server.report(None).await;
        assert_eq!(clear["session_start_source"], "clear");
        assert!(clear["seq"].as_u64() > fork["seq"].as_u64());
        reporter.select(Some("resumed-thread".to_owned()));
        server.report(Some("resumed-thread")).await;
        reporter.shutdown().await;
        assert_eq!(server.report(None).await["session_start_source"], "clear");
    }

    #[tokio::test]
    async fn heartbeat_repairs_lost_registration_and_follows_terminal_moves() {
        let mut server = Server::start().await;
        let reporter = HerdrSessionReporter::start(server.socket.clone(), 73);
        reporter.select(Some("same-thread".to_owned()));
        let first = server.report(Some("same-thread")).await;
        *server.pane.lock().await = "moved-pane".to_owned();
        let refreshed = server.report(Some("same-thread")).await;
        assert_eq!(refreshed["pane_id"], "moved-pane");
        assert_eq!(refreshed["client_terminal_id"], first["client_terminal_id"]);
        assert!(refreshed["seq"].as_u64() > first["seq"].as_u64());
        reporter.shutdown().await;
    }

    #[tokio::test]
    async fn missing_process_proof_never_registers() {
        let mut server = Server::start().await;
        assert_eq!(discover(&server.socket, 999, None).await, None);
        let reporter = HerdrSessionReporter::start(server.socket.clone(), 999);
        reporter.select(Some("unowned".to_owned()));
        reporter.shutdown().await;
        assert!(server.reports.try_recv().is_err());
    }

    #[tokio::test]
    async fn unavailable_server_does_not_block_shutdown() {
        let directory = tempfile::tempdir().expect("tempdir");
        let reporter = HerdrSessionReporter::start(directory.path().join("missing.sock"), 73);
        reporter.select(Some("thread".to_owned()));
        tokio::time::timeout(Duration::from_secs(3), reporter.shutdown())
            .await
            .expect("bounded shutdown");
    }

    #[tokio::test]
    async fn rejects_mismatched_and_truncated_responses() {
        for response in [
            b"{\"id\":\"wrong\",\"result\":{}}\n".as_slice(),
            b"{\"id\":\"wrong\",\"result\":{}}".as_slice(),
        ] {
            let directory = tempfile::tempdir().expect("tempdir");
            let socket = directory.path().join("bad.sock");
            let mut listener = codex_uds::UnixListener::bind(&socket).await.expect("bind");
            let server = tokio::spawn(async move {
                let stream = listener.accept().await.expect("accept");
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                stream.read_line(&mut line).await.expect("request");
                stream.get_mut().write_all(response).await.expect("reply");
            });
            assert!(
                request(&socket, "session.snapshot", json!({}))
                    .await
                    .is_none()
            );
            server.await.expect("server");
        }
    }

    #[tokio::test]
    async fn silent_server_has_a_bounded_request_deadline() {
        let directory = tempfile::tempdir().expect("tempdir");
        let socket = directory.path().join("silent.sock");
        let mut listener = codex_uds::UnixListener::bind(&socket).await.expect("bind");
        let server = tokio::spawn(async move {
            let _stream = listener.accept().await.expect("accept");
            std::future::pending::<()>().await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            request(&socket, "session.snapshot", json!({})),
        )
        .await
        .expect("deadline");
        assert!(result.is_none());
        server.abort();
    }
}
