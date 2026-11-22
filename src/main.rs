//! pi-bridge — an MCP server that drives the Pi coding agent over its RPC protocol.
//!
//! Pi (`@earendil-works/pi-coding-agent`) speaks newline-delimited JSON on stdin/stdout in
//! `--mode rpc`: commands in, `response` records and session events out. This bridge wraps one
//! Pi subprocess per session and exposes it as MCP tools, so any MCP client (Hermes, an editor,
//! another agent) can start work, watch it, steer it and cancel it instead of blocking on a
//! one-shot `pi -p` call.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify, mpsc};

const SERVER_NAME: &str = "pi-bridge";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: &str = "2025-06-18";
const DEFAULT_RUN_TIMEOUT_SECS: u64 = 900;

/// Live view of one Pi session, updated by the reader task.
struct Live {
    status: Status,
    last_event: String,
    assistant_text: String,
    turns: u64,
    started: Instant,
    last_error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Running,
    Idle,
    Exited,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Running => "running",
            Status::Idle => "idle",
            Status::Exited => "exited",
        }
    }
}

struct Session {
    id: String,
    cwd: String,
    pid: Option<u32>,
    writer: mpsc::Sender<Value>,
    live: std::sync::Arc<Mutex<Live>>,
    turn_done: std::sync::Arc<Notify>,
    child: Mutex<Child>,
}

type Registry = std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Session>>>>;

fn next_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("pi-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Pull plain text out of a Pi message's `content`, which is either a string or a list of
/// typed parts (`{"type": "text", "text": ...}`).
fn extract_text(message: &Value) -> String {
    let content = message.get("content").cloned().unwrap_or(Value::Null);
    match content {
        Value::String(text) => text,
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

/// Parse `KEY=VALUE` lines (ignoring comments and blanks) from an env file.
///
/// The gateway does not always export its own `.env` to MCP children, and putting a real key in
/// `config.yaml` is off-limits, so `PI_ENV_FILE` points at the file instead and the child receives
/// the values without the secret ever reaching config.
fn read_env_file(path: &str) -> HashMap<String, String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let key = key.trim().trim_start_matches("export ").trim();
            if key.is_empty() {
                return None;
            }
            Some((
                key.to_string(),
                value
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string(),
            ))
        })
        .collect()
}

/// Resolve the Pi binary.
///
/// The gateway runs MCP children with a curated PATH that does **not** include an nvm bin
/// directory, which is exactly where `pi` gets installed on macOS (`~/.nvm/versions/node/*/bin`).
/// Trusting PATH alone therefore produced `failed to start pi` inside Hermes while working fine
/// in a login shell. `PI_BIN` wins, then PATH, then the usual install locations.
fn resolve_pi_bin() -> Result<String> {
    if let Ok(explicit) = std::env::var("PI_BIN") {
        if !explicit.trim().is_empty() {
            return Ok(explicit);
        }
    }
    if let Some(found) = search_path("pi") {
        return Ok(found);
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(format!("{home}/.nvm/versions/node")) {
        let mut versions: Vec<std::path::PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .collect();
        versions.sort();
        versions.reverse();
        for version in versions {
            candidates.push(version.join("bin/pi"));
        }
    }
    for path in [
        "/opt/homebrew/bin/pi".to_string(),
        "/usr/local/bin/pi".to_string(),
        format!("{home}/.local/bin/pi"),
        format!("{home}/.bun/bin/pi"),
        format!("{home}/.pi/bin/pi"),
    ] {
        candidates.push(std::path::PathBuf::from(path));
    }
    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.to_string_lossy().to_string());
        }
    }
    Err(anyhow!(
        "pi binary not found. Looked on PATH and in: {}. Set PI_BIN to the absolute path \
         (e.g. PI_BIN={home}/.nvm/versions/node/<version>/bin/pi).",
        candidates
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn search_path(bin: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().to_string());
        }
    }
    None
}

async fn spawn_session(cwd: &str, model_args: &[String]) -> Result<std::sync::Arc<Session>> {
    let pi_bin = resolve_pi_bin()?;
    let mut cmd = Command::new(&pi_bin);
    cmd.arg("--mode")
        .arg("rpc")
        .arg("--no-session")
        .args(model_args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    // `pi` is a `#!/usr/bin/env node` script, so its own bin directory has to be on the child's
    // PATH: resolving the binary but leaving PATH alone still failed with
    // "env: node: No such file or directory" under a curated environment.
    if let Some(dir) = std::path::Path::new(&pi_bin).parent() {
        let existing = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_string());
        let joined = format!("{}:{existing}", dir.display());
        cmd.env("PATH", joined);
    }

    if let Ok(env_file) = std::env::var("PI_ENV_FILE") {
        for (key, value) in read_env_file(&env_file) {
            cmd.env(key, value);
        }
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {pi_bin} --mode rpc in {cwd}"))?;

    let pid = child.id();
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("pi stdin unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("pi stdout unavailable"))?;
    let stderr = child.stderr.take();

    let id = next_id();
    let live = std::sync::Arc::new(Mutex::new(Live {
        status: Status::Running,
        last_event: "spawned".into(),
        assistant_text: String::new(),
        turns: 0,
        started: Instant::now(),
        last_error: None,
    }));
    let turn_done = std::sync::Arc::new(Notify::new());

    // Writer task: one JSON record per line, serialized so concurrent callers cannot interleave.
    let (tx, mut rx) = mpsc::channel::<Value>(64);
    tokio::spawn(async move {
        while let Some(record) = rx.recv().await {
            let line = format!("{record}\n");
            if stdin.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            if stdin.flush().await.is_err() {
                break;
            }
        }
    });

    // Reader task: fold every stdout record into the live view.
    let live_reader = live.clone();
    let notify_reader = turn_done.clone();
    let tx_reader = tx.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(record) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            let kind = record
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();

            if kind == "response"
                && record.get("command").and_then(Value::as_str) == Some("get_last_assistant_text")
            {
                if let Some(text) = record.pointer("/data/text").and_then(Value::as_str) {
                    if !text.trim().is_empty() {
                        live_reader.lock().await.assistant_text = text.to_string();
                    }
                }
                continue;
            }

            // `message_end` carries the assistant's reply before `turn_end` fires, so recording it
            // here removes the race between the turn ending and the text response arriving.
            if kind == "message_end" {
                let message = record.get("message").cloned().unwrap_or(Value::Null);
                if message.get("role").and_then(Value::as_str) == Some("assistant") {
                    let text = extract_text(&message);
                    if !text.trim().is_empty() {
                        live_reader.lock().await.assistant_text = text;
                    }
                }
            }

            {
                let mut live = live_reader.lock().await;
                live.last_event = kind.clone();
                // Only the events that actually begin or end work may move the status; response
                // acks and queue chatter must not flip an idle session back to running.
                match kind.as_str() {
                    "turn_end" | "agent_end" => {
                        if kind == "turn_end" {
                            live.turns += 1;
                        }
                        live.status = Status::Idle;
                    }
                    "agent_start" | "turn_start" => live.status = Status::Running,
                    _ => {}
                }
                if let Some(err) = record.get("error").and_then(Value::as_str) {
                    live.last_error = Some(err.to_string());
                }
            }

            if kind == "turn_end" || kind == "agent_end" {
                // Ask Pi for the turn's final text; the response lands in the reader above.
                let request =
                    json!({"id": format!("text-{}", next_id()), "type": "get_last_assistant_text"});
                let _ = tx_reader.send(request).await;
                notify_reader.notify_waiters();
            }
        }
        live_reader.lock().await.status = Status::Exited;
        notify_reader.notify_waiters();
    });

    if let Some(stderr) = stderr {
        let live_stderr = live.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                live_stderr.lock().await.last_error = Some(line);
            }
        });
    }

    Ok(std::sync::Arc::new(Session {
        id,
        cwd: cwd.to_string(),
        pid,
        writer: tx,
        live,
        turn_done,
        child: Mutex::new(child),
    }))
}

async fn wait_for_turn(session: &Session, timeout: Duration) -> Result<String> {
    let deadline = Instant::now() + timeout;
    loop {
        {
            let live = session.live.lock().await;
            if live.turns > 0 && live.status != Status::Running {
                return Ok(live.assistant_text.clone());
            }
            if live.status == Status::Exited {
                return Err(anyhow!("pi session exited before finishing the turn"));
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(anyhow!("timed out waiting for pi to finish the turn"));
        }
        let _ = tokio::time::timeout(remaining, session.turn_done.notified()).await;
    }
}

// ── tools ────────────────────────────────────────────────────────────────────────

fn tool_definitions() -> Value {
    json!([
        {
            "name": "pi_run",
            "description": "Run one prompt to completion in a repository with the Pi coding agent and return its final answer. Blocks until the turn ends or the timeout fires.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "description": "Task for the agent. Be specific about files, tests and acceptance criteria."},
                    "cwd": {"type": "string", "description": "Repository or working directory Pi should operate in (absolute path)."},
                    "model": {"type": "string", "description": "Optional model pattern, e.g. deepseek-v4-pro or provider/model."},
                    "timeout_secs": {"type": "integer", "description": "Seconds to wait before giving up. Default 900."}
                },
                "required": ["prompt", "cwd"]
            }
        },
        {
            "name": "pi_start",
            "description": "Start a prompt in the background and return a session id immediately. Use pi_status to follow it and pi_say to steer it.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "cwd": {"type": "string"},
                    "model": {"type": "string"}
                },
                "required": ["prompt", "cwd"]
            }
        },
        {
            "name": "pi_status",
            "description": "Report status, elapsed time, last event and the latest assistant text for one session (or every session when session is omitted).",
            "inputSchema": {
                "type": "object",
                "properties": {"session": {"type": "string"}}
            }
        },
        {
            "name": "pi_say",
            "description": "Send another message to a running or idle session: a steering message while it works, or a follow-up prompt after it stops.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session": {"type": "string"},
                    "message": {"type": "string"}
                },
                "required": ["session", "message"]
            }
        },
        {
            "name": "pi_cancel",
            "description": "Abort the current turn of a session.",
            "inputSchema": {
                "type": "object",
                "properties": {"session": {"type": "string"}},
                "required": ["session"]
            }
        },
        {
            "name": "pi_stop",
            "description": "Abort the turn, then terminate the Pi subprocess and forget the session.",
            "inputSchema": {
                "type": "object",
                "properties": {"session": {"type": "string"}},
                "required": ["session"]
            }
        }
    ])
}

fn model_args(model: Option<&str>) -> Vec<String> {
    match model.filter(|m| !m.trim().is_empty()) {
        Some(m) => vec!["--model".to_string(), m.to_string()],
        None => Vec::new(),
    }
}

async fn status_json(session: &Session) -> Value {
    let live = session.live.lock().await;
    json!({
        "session": session.id,
        "cwd": session.cwd,
        "pid": session.pid,
        "status": live.status.as_str(),
        "turns": live.turns,
        "elapsed_secs": live.started.elapsed().as_secs(),
        "last_event": live.last_event,
        "last_error": live.last_error,
        "assistant_text": live.assistant_text
    })
}

fn text_result(text: impl Into<String>) -> Result<Value> {
    Ok(json!({"content": [{"type": "text", "text": text.into()}]}))
}

fn error_result(text: impl Into<String>) -> Result<Value> {
    Ok(json!({"content": [{"type": "text", "text": text.into()}], "isError": true}))
}

async fn call_tool(registry: Registry, name: &str, args: &Value) -> Result<Value> {
    let str_arg = |key: &str| args.get(key).and_then(Value::as_str).map(str::to_string);

    match name {
        "pi_run" => {
            let (Some(prompt), Some(cwd)) = (str_arg("prompt"), str_arg("cwd")) else {
                return error_result("prompt and cwd are required");
            };
            let session = spawn_session(&cwd, &model_args(str_arg("model").as_deref())).await?;
            registry
                .lock()
                .await
                .insert(session.id.clone(), session.clone());
            let request = json!({"id": next_id(), "type": "prompt", "message": prompt});
            session.writer.send(request).await?;
            let timeout_secs = args
                .get("timeout_secs")
                .and_then(Value::as_u64)
                .unwrap_or(DEFAULT_RUN_TIMEOUT_SECS);
            match wait_for_turn(&session, Duration::from_secs(timeout_secs)).await {
                Ok(text) => {
                    let status = status_json(&session).await;
                    let body = if text.trim().is_empty() {
                        "(no assistant text)"
                    } else {
                        &text
                    };
                    text_result(format!(
                        "{body}\n\n[session {}] status={} turns={} elapsed={}s",
                        session.id,
                        status["status"].as_str().unwrap_or("?"),
                        status["turns"],
                        status["elapsed_secs"]
                    ))
                }
                Err(err) => {
                    let status = serde_json::to_string_pretty(&status_json(&session).await)?;
                    error_result(format!("{err}\n{status}"))
                }
            }
        }
        "pi_start" => {
            let (Some(prompt), Some(cwd)) = (str_arg("prompt"), str_arg("cwd")) else {
                return error_result("prompt and cwd are required");
            };
            let session = spawn_session(&cwd, &model_args(str_arg("model").as_deref())).await?;
            registry
                .lock()
                .await
                .insert(session.id.clone(), session.clone());
            let request = json!({"id": next_id(), "type": "prompt", "message": prompt});
            session.writer.send(request).await?;
            text_result(format!(
                "started {} in {} (pid {})\nfollow with pi_status session={}",
                session.id,
                session.cwd,
                session.pid.map(|p| p.to_string()).unwrap_or_default(),
                session.id
            ))
        }
        "pi_status" => {
            let sessions = registry.lock().await;
            if let Some(id) = str_arg("session") {
                return match sessions.get(&id) {
                    Some(session) => {
                        text_result(serde_json::to_string_pretty(&status_json(session).await)?)
                    }
                    None => error_result(format!("unknown session: {id}")),
                };
            }
            if sessions.is_empty() {
                return text_result("no sessions");
            }
            let ids: Vec<String> = sessions.keys().cloned().collect();
            let mut out = Vec::new();
            for id in ids {
                if let Some(session) = sessions.get(&id) {
                    out.push(status_json(session).await);
                }
            }
            text_result(serde_json::to_string_pretty(&Value::Array(out))?)
        }
        "pi_say" => {
            let (Some(id), Some(message)) = (str_arg("session"), str_arg("message")) else {
                return error_result("session and message are required");
            };
            let session = registry.lock().await.get(&id).cloned();
            let Some(session) = session else {
                return error_result(format!("unknown session: {id}"));
            };
            session
                .writer
                .send(json!({"id": next_id(), "type": "prompt", "message": message}))
                .await?;
            text_result(format!("sent to {id}"))
        }
        "pi_cancel" => {
            let Some(id) = str_arg("session") else {
                return error_result("session is required");
            };
            let session = registry.lock().await.get(&id).cloned();
            let Some(session) = session else {
                return error_result(format!("unknown session: {id}"));
            };
            session
                .writer
                .send(json!({"id": next_id(), "type": "abort"}))
                .await?;
            text_result(format!("abort requested for {id}"))
        }
        "pi_stop" => {
            let Some(id) = str_arg("session") else {
                return error_result("session is required");
            };
            let session = registry.lock().await.remove(&id);
            let Some(session) = session else {
                return error_result(format!("unknown session: {id}"));
            };
            let _ = session
                .writer
                .send(json!({"id": next_id(), "type": "abort"}))
                .await;
            let _ = session.child.lock().await.kill().await;
            text_result(format!("stopped {id}"))
        }
        other => error_result(format!("unknown tool: {other}")),
    }
}

// ── MCP stdio transport ──────────────────────────────────────────────────────────

fn result_envelope(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_envelope(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[tokio::main]
async fn main() -> Result<()> {
    let registry: Registry = std::sync::Arc::new(Mutex::new(HashMap::new()));
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(64);

    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = out_rx.recv().await {
            let line = format!("{message}\n");
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        // Notifications carry no id and expect no reply.
        if id.is_null() {
            continue;
        }

        let registry = registry.clone();
        let out_tx = out_tx.clone();
        tokio::spawn(async move {
            let response = match method.as_str() {
                "initialize" => result_envelope(
                    &id,
                    json!({
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {"tools": {"listChanged": false}},
                        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION}
                    }),
                ),
                "ping" => result_envelope(&id, json!({})),
                "tools/list" => result_envelope(&id, json!({"tools": tool_definitions()})),
                "tools/call" => {
                    let name = request
                        .pointer("/params/name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let args = request
                        .pointer("/params/arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    match call_tool(registry, &name, &args).await {
                        Ok(result) => result_envelope(&id, result),
                        Err(err) => error_envelope(&id, -32603, &err.to_string()),
                    }
                }
                other => error_envelope(&id, -32601, &format!("method not found: {other}")),
            };
            let _ = out_tx.send(response).await;
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_text_from_string_content() {
        let message = json!({"role": "assistant", "content": "hello"});
        assert_eq!(extract_text(&message), "hello");
    }

    #[test]
    fn extracts_text_from_part_list() {
        let message = json!({
            "role": "assistant",
            "content": [{"type": "text", "text": "hello "}, {"type": "image"}, {"type": "text", "text": "world"}]
        });
        assert_eq!(extract_text(&message), "hello world");
    }

    #[test]
    fn missing_content_is_empty() {
        assert_eq!(extract_text(&json!({"role": "assistant"})), "");
        assert_eq!(
            extract_text(&json!({"role": "assistant", "content": 42})),
            ""
        );
    }

    #[test]
    fn env_file_parsing_skips_comments_and_quotes() {
        let dir = std::env::temp_dir().join("pi-bridge-env-test");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(".env");
        std::fs::write(
            &file,
            "# comment\nexport DEEPSEEK_API_KEY=\"abc\"\n\nEMPTY=\n",
        )
        .unwrap();
        let parsed = read_env_file(file.to_str().unwrap());
        assert_eq!(
            parsed.get("DEEPSEEK_API_KEY").map(String::as_str),
            Some("abc")
        );
        assert_eq!(parsed.get("EMPTY").map(String::as_str), Some(""));
        assert!(!parsed.contains_key("# comment"));
        assert!(read_env_file("/nonexistent/file").is_empty());
    }

    #[test]
    fn model_args_are_empty_without_a_model() {
        assert!(model_args(None).is_empty());
        assert!(model_args(Some("  ")).is_empty());
        assert_eq!(
            model_args(Some("deepseek-v4-pro")),
            vec!["--model".to_string(), "deepseek-v4-pro".to_string()]
        );
    }
}
