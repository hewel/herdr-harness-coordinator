//! Immutable file admission into Coordinator-owned storage.

use std::{
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::contract::AttachmentId;

/// Default maximum admitted Attachment size: 64 MiB.
pub const DEFAULT_MAX_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;

const COPY_BUFFER_BYTES: usize = 64 * 1024;
// Linux `O_NOFOLLOW`. Herdr's MVP plugin target is Linux.
const O_NOFOLLOW: i32 = 0o400_000;

/// Immutable metadata for one file copied into Coordinator-owned storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentMetadata {
    /// Generated durable Attachment identity.
    pub id: AttachmentId,
    /// Lowercase SHA-256 digest of the stored bytes.
    pub digest: String,
    /// Exact stored byte count.
    pub size_bytes: u64,
    /// Caller-declared media type.
    pub media_type: String,
    /// Caller-declared original file name.
    pub original_name: String,
    /// Path relative to the Coordinator state directory.
    pub storage_path: PathBuf,
}

/// Attachment admission or integrity failure.
#[derive(Debug, Error)]
pub enum AttachmentError {
    /// The source is not an admissible readable regular file.
    #[error("invalid Attachment source `{path}`: {reason}")]
    InvalidSource {
        /// Rejected source path.
        path: PathBuf,
        /// Stable human-readable rejection reason.
        reason: &'static str,
    },
    /// The source exceeded the configured limit while being streamed.
    #[error("Attachment exceeds the configured {limit}-byte limit")]
    TooLarge {
        /// Configured maximum byte count.
        limit: u64,
    },
    /// Stored content is absent, malformed, or differs from its metadata.
    #[error("Attachment {id} is corrupt: {reason}")]
    Corrupt {
        /// Attachment whose integrity check failed.
        id: AttachmentId,
        /// Integrity failure detail.
        reason: String,
    },
    /// A filesystem operation failed.
    #[error("failed to {operation} `{path}`: {source}")]
    Io {
        /// Operation being attempted.
        operation: &'static str,
        /// Path involved in the operation.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
}

impl AttachmentError {
    fn io(operation: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }

    fn corrupt(id: AttachmentId, reason: impl Into<String>) -> Self {
        Self::Corrupt {
            id,
            reason: reason.into(),
        }
    }
}

/// Filesystem-backed immutable Attachment store.
#[derive(Debug, Clone)]
pub struct AttachmentStore {
    state_dir: PathBuf,
    max_bytes: u64,
}

impl AttachmentStore {
    /// Creates a store using the 64 MiB default admission limit.
    #[must_use]
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self::with_max_bytes(state_dir, DEFAULT_MAX_ATTACHMENT_BYTES)
    }

    /// Creates a store with a caller-selected maximum admitted byte count.
    #[must_use]
    pub fn with_max_bytes(state_dir: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            state_dir: state_dir.into(),
            max_bytes,
        }
    }

    /// Copies a readable regular file into immutable Coordinator-owned storage.
    ///
    /// The final source path is opened with `O_NOFOLLOW`. Content is streamed
    /// through a state-directory temporary file, hashed, fsynced, and then
    /// atomically renamed into the Attachment directory.
    ///
    /// # Errors
    ///
    /// Returns [`AttachmentError`] when the source is inadmissible, exceeds the
    /// configured limit, or a required filesystem operation fails.
    pub async fn admit(
        &self,
        source: &Path,
        media_type: &str,
        original_name: &str,
    ) -> Result<AttachmentMetadata, AttachmentError> {
        let attachments_dir = self.state_dir.join("attachments");
        let temporary_dir = self.state_dir.join("tmp");
        create_dir(&attachments_dir).await?;
        create_dir(&temporary_dir).await?;

        let source_metadata = fs::symlink_metadata(source)
            .await
            .map_err(|error| AttachmentError::io("inspect source", source, error))?;
        validate_source_metadata(source, &source_metadata)?;
        if source_metadata.len() > self.max_bytes {
            return Err(AttachmentError::TooLarge {
                limit: self.max_bytes,
            });
        }

        let mut input = OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW)
            .open(source)
            .await
            .map_err(|error| map_source_open_error(source, error))?;
        let opened_metadata = input
            .metadata()
            .await
            .map_err(|error| AttachmentError::io("inspect open source", source, error))?;
        validate_opened_source(source, &source_metadata, &opened_metadata)?;

        let id = AttachmentId::new();
        let temporary_path = temporary_dir.join(format!("attachment-{id}.tmp"));
        let storage_path = PathBuf::from("attachments").join(id.to_string());
        let published_path = self.state_dir.join(&storage_path);
        let result = self
            .copy_and_publish(&mut input, source, &temporary_path, &published_path)
            .await;
        if result.is_err() {
            let _ = fs::remove_file(&temporary_path).await;
        }
        let (digest, size_bytes) = result?;

        Ok(AttachmentMetadata {
            id,
            digest,
            size_bytes,
            media_type: media_type.to_owned(),
            original_name: original_name.to_owned(),
            storage_path,
        })
    }

    /// Recomputes stored size and SHA-256 and compares them with durable metadata.
    ///
    /// # Errors
    ///
    /// Returns [`AttachmentError::Corrupt`] when stored content is missing,
    /// non-regular, symlinked, unreadable, or differs from its recorded metadata.
    pub async fn verify(&self, attachment: &AttachmentMetadata) -> Result<(), AttachmentError> {
        let expected_path = PathBuf::from("attachments").join(attachment.id.to_string());
        if attachment.storage_path != expected_path {
            return Err(AttachmentError::corrupt(
                attachment.id,
                "storage path does not match Attachment identity",
            ));
        }
        let path = self.state_dir.join(&attachment.storage_path);
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|error| AttachmentError::corrupt(attachment.id, error.to_string()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(AttachmentError::corrupt(
                attachment.id,
                "stored path is not a regular file",
            ));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW)
            .open(&path)
            .await
            .map_err(|error| AttachmentError::corrupt(attachment.id, error.to_string()))?;
        let (actual_digest, actual_size) = hash_reader(&mut file, u64::MAX)
            .await
            .map_err(|error| AttachmentError::corrupt(attachment.id, error.to_string()))?;
        if actual_size != attachment.size_bytes {
            return Err(AttachmentError::corrupt(
                attachment.id,
                format!(
                    "size mismatch: expected {}, found {actual_size}",
                    attachment.size_bytes
                ),
            ));
        }
        if actual_digest != attachment.digest {
            return Err(AttachmentError::corrupt(
                attachment.id,
                "SHA-256 digest mismatch",
            ));
        }
        Ok(())
    }

    async fn copy_and_publish(
        &self,
        input: &mut File,
        source_path: &Path,
        temporary_path: &Path,
        published_path: &Path,
    ) -> Result<(String, u64), AttachmentError> {
        let mut temporary = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary_path)
            .await
            .map_err(|error| {
                AttachmentError::io("create temporary Attachment", temporary_path, error)
            })?;
        let (digest, size_bytes) = copy_and_hash(
            input,
            &mut temporary,
            self.max_bytes,
            source_path,
            temporary_path,
        )
        .await?;
        temporary.sync_all().await.map_err(|error| {
            AttachmentError::io("fsync temporary Attachment", temporary_path, error)
        })?;
        drop(temporary);
        fs::rename(temporary_path, published_path)
            .await
            .map_err(|error| AttachmentError::io("publish Attachment", published_path, error))?;
        sync_directory(published_path.parent().unwrap_or(&self.state_dir))?;
        Ok((digest, size_bytes))
    }
}

async fn create_dir(path: &Path) -> Result<(), AttachmentError> {
    fs::create_dir_all(path)
        .await
        .map_err(|error| AttachmentError::io("create directory", path, error))
}

fn validate_source_metadata(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<(), AttachmentError> {
    if metadata.file_type().is_symlink() {
        return Err(invalid_source(path, "final path is a symlink"));
    }
    if !metadata.is_file() {
        return Err(invalid_source(path, "source is not a regular file"));
    }
    if metadata.permissions().mode() & 0o444 == 0 {
        return Err(invalid_source(
            path,
            "source has no readable permission bits",
        ));
    }
    Ok(())
}

fn validate_opened_source(
    path: &Path,
    before: &std::fs::Metadata,
    opened: &std::fs::Metadata,
) -> Result<(), AttachmentError> {
    if !opened.is_file() {
        return Err(invalid_source(path, "opened source is not a regular file"));
    }
    if before.dev() != opened.dev() || before.ino() != opened.ino() {
        return Err(invalid_source(path, "source changed during admission"));
    }
    Ok(())
}

fn invalid_source(path: &Path, reason: &'static str) -> AttachmentError {
    AttachmentError::InvalidSource {
        path: path.to_path_buf(),
        reason,
    }
}

fn map_source_open_error(path: &Path, error: io::Error) -> AttachmentError {
    match error.kind() {
        io::ErrorKind::PermissionDenied => invalid_source(path, "source is not readable"),
        _ => AttachmentError::io("open source without following symlink", path, error),
    }
}

async fn copy_and_hash(
    input: &mut File,
    output: &mut File,
    max_bytes: u64,
    source_path: &Path,
    temporary_path: &Path,
) -> Result<(String, u64), AttachmentError> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = input
            .read(&mut buffer)
            .await
            .map_err(|error| AttachmentError::io("read source", source_path, error))?;
        if read == 0 {
            break;
        }
        size = size.saturating_add(read as u64);
        if size > max_bytes {
            return Err(AttachmentError::TooLarge { limit: max_bytes });
        }
        hasher.update(&buffer[..read]);
        output.write_all(&buffer[..read]).await.map_err(|error| {
            AttachmentError::io("write temporary Attachment", temporary_path, error)
        })?;
    }
    Ok((hex::encode(hasher.finalize()), size))
}

async fn hash_reader(input: &mut File, max_bytes: u64) -> io::Result<(String, u64)> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = input.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        size = size.saturating_add(read as u64);
        if size > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "file exceeds size limit",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    Ok((hex::encode(hasher.finalize()), size))
}

fn sync_directory(path: &Path) -> Result<(), AttachmentError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| AttachmentError::io("fsync Attachment directory", path, error))
}
