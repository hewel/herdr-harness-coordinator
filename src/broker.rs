//! Versioned JSONL broker over a local Unix socket.

use std::{
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    task::JoinSet,
};

use crate::{
    contract::SCHEMA_VERSION,
    core::{
        ActorContext, Coordinator, CoordinatorCommand, CoordinatorError, CoordinatorQuery,
        ErrorCategory,
    },
};

/// Maximum accepted request or emitted response frame.
pub const MAX_BROKER_FRAME_BYTES: usize = 1024 * 1024;

/// One versioned broker request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerRequest {
    /// Must equal the public contract version.
    pub schema_version: u32,
    /// Caller-selected correlation returned unchanged.
    pub request_id: String,
    /// Authenticated Core operation.
    pub operation: BrokerOperation,
}

/// Operations transported without exposing `SQLite` or provider protocols.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[expect(
    clippy::large_enum_variant,
    reason = "the versioned wire shape remains direct and serde-stable across command variants"
)]
pub enum BrokerOperation {
    /// Execute one state-changing Core command.
    Execute {
        /// Authenticated actor.
        actor: ActorContext,
        /// Command payload.
        command: CoordinatorCommand,
    },
    /// Execute one read-only Core query.
    Query {
        /// Authenticated actor.
        actor: ActorContext,
        /// Query payload.
        query: CoordinatorQuery,
    },
}

/// One versioned broker response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerResponse {
    /// Public contract version.
    pub schema_version: u32,
    /// Correlation copied from the request when decodable.
    pub request_id: Option<String>,
    /// Successful command or query value.
    pub result: Option<Value>,
    /// Stable error, absent on success.
    pub error: Option<BrokerErrorBody>,
}

/// Serializable stable Core or transport failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerErrorBody {
    /// Stable category shared with the Core.
    pub category: ErrorCategory,
    /// Bounded human-readable diagnostic.
    pub message: String,
}

/// Broker bind, framing, or connection failure.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Unix socket operation failed.
    #[error("broker socket `{path}` failed during {phase:?}: {source}")]
    Io {
        /// Socket path.
        path: PathBuf,
        /// Transport phase that determines whether request bytes may have been written.
        phase: BrokerIoPhase,
        /// Underlying I/O failure.
        source: io::Error,
    },
    /// A response exceeded the public framing limit.
    #[error("broker response exceeds the {MAX_BROKER_FRAME_BYTES}-byte limit")]
    ResponseTooLarge,
}

/// Unix broker phase used to distinguish retry-safe connection failures from ambiguity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerIoPhase {
    Bind,
    Permissions,
    Accept,
    Connect,
    Encode,
    Write,
    Read,
    Decode,
}

impl BrokerError {
    fn io(path: &Path, phase: BrokerIoPhase, source: io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            phase,
            source,
        }
    }

    /// True only when the socket connection failed before request bytes could be written.
    #[must_use]
    pub fn is_retry_safe_connect(&self) -> bool {
        matches!(
            self,
            Self::Io {
                phase: BrokerIoPhase::Connect,
                ..
            }
        )
    }
}

/// Bound local broker that owns a Coordinator reference.
pub struct BrokerServer {
    coordinator: Arc<Coordinator>,
    listener: UnixListener,
    socket_path: PathBuf,
}

impl BrokerServer {
    /// Binds a new owner-only Unix socket. Existing paths are never overwritten.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the parent directory, bind, or permissions fail.
    pub async fn bind(
        coordinator: Arc<Coordinator>,
        socket_path: impl AsRef<Path>,
    ) -> Result<Self, BrokerError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| BrokerError::io(parent, BrokerIoPhase::Bind, source))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .map_err(|source| BrokerError::io(&socket_path, BrokerIoPhase::Bind, source))?;
        tokio::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|source| BrokerError::io(&socket_path, BrokerIoPhase::Permissions, source))?;
        Ok(Self {
            coordinator,
            listener,
            socket_path,
        })
    }

    /// Serves connections until the task is cancelled or an accept error occurs.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when accepting a connection fails.
    pub async fn serve(self) -> Result<(), BrokerError> {
        let mut clients = JoinSet::new();
        loop {
            let (stream, _) = self.listener.accept().await.map_err(|source| {
                BrokerError::io(&self.socket_path, BrokerIoPhase::Accept, source)
            })?;
            let coordinator = Arc::clone(&self.coordinator);
            clients.spawn(async move {
                let _ = serve_connection(coordinator, stream).await;
            });
            while clients.try_join_next().is_some() {}
        }
    }
}

/// Sends one request over a fresh local connection.
///
/// # Errors
///
/// Returns [`BrokerError`] for connection, framing, or response decoding failure.
pub async fn call(
    socket_path: &Path,
    request: &BrokerRequest,
) -> Result<BrokerResponse, BrokerError> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|source| BrokerError::io(socket_path, BrokerIoPhase::Connect, source))?;
    let mut frame = serde_json::to_vec(request).map_err(|source| {
        BrokerError::io(
            socket_path,
            BrokerIoPhase::Encode,
            io::Error::new(io::ErrorKind::InvalidData, source),
        )
    })?;
    frame.push(b'\n');
    if frame.len() > MAX_BROKER_FRAME_BYTES {
        return Err(BrokerError::ResponseTooLarge);
    }
    stream
        .write_all(&frame)
        .await
        .map_err(|source| BrokerError::io(socket_path, BrokerIoPhase::Write, source))?;
    let mut reader = BufReader::new(stream);
    let mut response = Vec::new();
    reader
        .read_until(b'\n', &mut response)
        .await
        .map_err(|source| BrokerError::io(socket_path, BrokerIoPhase::Read, source))?;
    if response.len() > MAX_BROKER_FRAME_BYTES {
        return Err(BrokerError::ResponseTooLarge);
    }
    serde_json::from_slice(&response).map_err(|source| {
        BrokerError::io(
            socket_path,
            BrokerIoPhase::Decode,
            io::Error::new(io::ErrorKind::InvalidData, source),
        )
    })
}

/// Retries only provably unwritten socket-connect failures for a bounded handoff window.
///
/// Write, read, and decode failures are returned immediately because provider or Core effects may
/// already exist and blind replay would violate delivery safety.
///
/// # Errors
///
/// Returns the final connect failure after `max_wait`, or any non-connect transport failure
/// immediately without replaying the request.
pub async fn call_with_connect_retry(
    socket_path: &Path,
    request: &BrokerRequest,
    max_wait: std::time::Duration,
) -> Result<BrokerResponse, BrokerError> {
    let started = tokio::time::Instant::now();
    loop {
        match call(socket_path, request).await {
            Ok(response) => return Ok(response),
            Err(error)
                if error.is_retry_safe_connect()
                    && tokio::time::Instant::now().duration_since(started) < max_wait =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn serve_connection(coordinator: Arc<Coordinator>, stream: UnixStream) -> io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    loop {
        let mut frame = Vec::new();
        let read = reader.read_until(b'\n', &mut frame).await?;
        if read == 0 {
            return Ok(());
        }
        let response = if frame.len() > MAX_BROKER_FRAME_BYTES {
            BrokerResponse::error(
                None,
                ErrorCategory::InvalidInput,
                "broker frame exceeds 1 MiB",
            )
        } else {
            handle_frame(&coordinator, &frame).await
        };
        let mut encoded = serde_json::to_vec(&response)
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
        if encoded.len() + 1 > MAX_BROKER_FRAME_BYTES {
            encoded = serde_json::to_vec(&BrokerResponse::error(
                response.request_id,
                ErrorCategory::StorageFailure,
                "broker response exceeds 1 MiB",
            ))
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
        }
        encoded.push(b'\n');
        write.write_all(&encoded).await?;
        if frame.len() > MAX_BROKER_FRAME_BYTES {
            return Ok(());
        }
    }
}

async fn handle_frame(coordinator: &Coordinator, frame: &[u8]) -> BrokerResponse {
    let request: BrokerRequest = match serde_json::from_slice(frame) {
        Ok(request) => request,
        Err(error) => {
            return BrokerResponse::error(None, ErrorCategory::InvalidInput, error.to_string());
        }
    };
    if request.schema_version != SCHEMA_VERSION {
        return BrokerResponse::error(
            Some(request.request_id),
            ErrorCategory::UnsupportedVersion,
            "broker schema_version must equal 1",
        );
    }
    let request_id = Some(request.request_id);
    let outcome = match request.operation {
        BrokerOperation::Execute { actor, command } => coordinator
            .execute(actor, command)
            .await
            .and_then(|value| serde_json::to_value(value).map_err(CoordinatorError::storage)),
        BrokerOperation::Query { actor, query } => coordinator
            .query(actor, query)
            .await
            .and_then(|value| serde_json::to_value(value).map_err(CoordinatorError::storage)),
    };
    match outcome {
        Ok(result) => BrokerResponse {
            schema_version: SCHEMA_VERSION,
            request_id,
            result: Some(result),
            error: None,
        },
        Err(error) => BrokerResponse::core_error(request_id, error),
    }
}

impl BrokerResponse {
    fn error(
        request_id: Option<String>,
        category: ErrorCategory,
        message: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            request_id,
            result: None,
            error: Some(BrokerErrorBody {
                category,
                message: message.into().chars().take(4096).collect(),
            }),
        }
    }

    fn core_error(request_id: Option<String>, error: CoordinatorError) -> Self {
        Self::error(request_id, error.category, error.message)
    }
}
