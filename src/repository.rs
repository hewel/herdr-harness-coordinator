//! Read-only Git repository discovery and advisory observation.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::contract::{
    GitStatusEntryV1, ObservedFileType, ScopeClassification, ScopeClassificationV1,
    UntrackedPathV1, WriteScopeV1,
};

/// Canonical identity used to distinguish one Git worktree from another.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepositoryIdentity {
    /// Canonical root of this worktree.
    pub worktree_root: PathBuf,
    /// Canonical common directory shared by linked worktrees.
    pub git_common_dir: PathBuf,
}

/// A read-only handle to one canonical non-bare Git worktree.
#[derive(Debug, Clone)]
pub struct GitRepository {
    identity: RepositoryIdentity,
}

/// Immutable Git-visible state captured at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositorySnapshot {
    /// Canonical repository identity.
    pub identity: RepositoryIdentity,
    /// Current commit, or `None` for an unborn branch.
    pub head: Option<String>,
    /// Current branch, or `None` for detached HEAD.
    pub branch: Option<String>,
    /// SHA-256 of the current index file, or the empty input when no index exists.
    pub index_digest: String,
    /// Normalized porcelain-v2 status records.
    pub status_entries: Vec<GitStatusEntryV1>,
    /// Raw `git diff --binary --cached` output.
    pub staged_diff: Vec<u8>,
    /// Raw `git diff --binary` output.
    pub unstaged_diff: Vec<u8>,
    /// Metadata and content digests for visible untracked paths.
    pub untracked: Vec<UntrackedPathV1>,
    /// Visible ignored path names. Content changes to existing ignored files are not observed.
    pub ignored_paths: Vec<PathBuf>,
    fingerprints: BTreeMap<PathBuf, String>,
}

/// Paths that changed after a baseline and their advisory scope classifications.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineComparison {
    /// Sorted repository-relative paths whose Git-visible state differs.
    pub changed_paths: Vec<PathBuf>,
    /// One classification for every changed path.
    pub scope_classifications: Vec<ScopeClassificationV1>,
}

/// A failure to discover or observe a repository without mutating it.
#[derive(Debug, Error)]
pub enum RepositoryError {
    /// The selected root cannot be resolved.
    #[error("cannot canonicalize repository root `{path}`: {source}")]
    Canonicalize {
        /// User-selected repository root.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// The selected path is not exactly the discovered worktree root.
    #[error("selected repository root `{selected}` differs from Git worktree root `{discovered}`")]
    RootMismatch {
        /// Canonical selected path.
        selected: PathBuf,
        /// Canonical Git root.
        discovered: PathBuf,
    },
    /// Git rejected a discovery or observation command.
    #[error("Git command `{command}` failed with exit code {exit_code:?}: {diagnostics}")]
    Git {
        /// Sanitized command description.
        command: String,
        /// Process exit status when available.
        exit_code: Option<i32>,
        /// Bounded stderr diagnostics.
        diagnostics: String,
    },
    /// A Git or filesystem path cannot be represented by the UTF-8-only MVP contract.
    #[error("repository contains a non-UTF-8 path")]
    NonUtf8Path,
    /// Git output expected to be textual was not valid UTF-8.
    #[error("Git command `{command}` returned non-UTF-8 text")]
    NonUtf8Output {
        /// Sanitized command description.
        command: String,
    },
    /// Bare repositories do not have a Worker-owned worktree.
    #[error("bare Git repositories are not supported")]
    BareRepository,
    /// Filesystem evidence could not be read.
    #[error("cannot inspect repository path `{path}`: {source}")]
    Io {
        /// Path being inspected.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// Porcelain v2 returned a record outside the supported documented grammar.
    #[error("invalid Git porcelain-v2 status record: {0}")]
    InvalidStatus(String),
}

impl GitRepository {
    /// Discovers a canonical, non-bare Git worktree rooted exactly at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] for non-Git paths, bare repositories, root mismatches,
    /// non-UTF-8 paths, or failed Git discovery commands.
    pub fn open(root: &Path) -> Result<Self, RepositoryError> {
        let selected = fs::canonicalize(root).map_err(|source| RepositoryError::Canonicalize {
            path: root.to_path_buf(),
            source,
        })?;
        path_as_utf8(&selected)?;

        let bare = git_text(&selected, ["rev-parse", "--is-bare-repository"])?;
        if bare.trim() == "true" {
            return Err(RepositoryError::BareRepository);
        }

        let discovered_text = git_text(
            &selected,
            ["rev-parse", "--path-format=absolute", "--show-toplevel"],
        )?;
        let discovered = canonicalize_git_path(discovered_text.trim_end())?;
        if selected != discovered {
            return Err(RepositoryError::RootMismatch {
                selected,
                discovered,
            });
        }

        let common_dir = git_text(
            &discovered,
            ["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let git_common_dir = canonicalize_git_path(common_dir.trim_end())?;

        Ok(Self {
            identity: RepositoryIdentity {
                worktree_root: discovered,
                git_common_dir,
            },
        })
    }

    /// Returns the canonical identity used for leases and Holds.
    #[must_use]
    pub fn identity(&self) -> &RepositoryIdentity {
        &self.identity
    }

    /// Captures current Git-visible state using read-only commands and filesystem reads.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when Git evidence or a visible path cannot be read safely.
    pub fn observe(&self) -> Result<RepositorySnapshot, RepositoryError> {
        let root = &self.identity.worktree_root;
        let status_output = git_bytes(
            root,
            [
                "status",
                "--porcelain=v2",
                "-z",
                "--branch",
                "--untracked-files=all",
                "--ignored=matching",
                "--no-renames",
            ],
        )?;
        let parsed = parse_status(&status_output)?;
        let staged_diff = git_bytes(
            root,
            [
                "diff",
                "--binary",
                "--cached",
                "--no-ext-diff",
                "--no-textconv",
                "--",
            ],
        )?;
        let unstaged_diff = git_bytes(
            root,
            ["diff", "--binary", "--no-ext-diff", "--no-textconv", "--"],
        )?;
        let index_path = git_text(
            root,
            ["rev-parse", "--path-format=absolute", "--git-path", "index"],
        )?;
        let index_digest = digest_optional_file(Path::new(index_path.trim_end()))?;
        let untracked = parsed
            .untracked_paths
            .iter()
            .map(|path| inspect_untracked(root, path))
            .collect::<Result<Vec<_>, _>>()?;
        let fingerprints = build_fingerprints(
            root,
            &parsed.status_entries,
            &untracked,
            &parsed.ignored_paths,
        )?;

        Ok(RepositorySnapshot {
            identity: self.identity.clone(),
            head: parsed.head,
            branch: parsed.branch,
            index_digest,
            status_entries: parsed.status_entries,
            staged_diff,
            unstaged_diff,
            untracked,
            ignored_paths: parsed.ignored_paths,
            fingerprints,
        })
    }
}

impl RepositorySnapshot {
    /// Compares this checkpoint to `baseline` and classifies every changed path.
    ///
    /// Rename/copy records contribute both their source and destination paths. An exact-file
    /// scope matches only that path; a subtree matches on path-component boundaries.
    #[must_use]
    pub fn compare_to(&self, baseline: &Self, scopes: &[WriteScopeV1]) -> BaselineComparison {
        let paths = self
            .fingerprints
            .keys()
            .chain(baseline.fingerprints.keys())
            .collect::<BTreeSet<_>>();
        let changed_paths = paths
            .into_iter()
            .filter(|path| self.fingerprints.get(*path) != baseline.fingerprints.get(*path))
            .cloned()
            .collect::<Vec<_>>();
        let scope_classifications = changed_paths
            .iter()
            .map(|path| classify_path(path, scopes))
            .collect();

        BaselineComparison {
            changed_paths,
            scope_classifications,
        }
    }
}

#[derive(Debug)]
struct ParsedStatus {
    head: Option<String>,
    branch: Option<String>,
    status_entries: Vec<GitStatusEntryV1>,
    untracked_paths: Vec<PathBuf>,
    ignored_paths: Vec<PathBuf>,
}

fn parse_status(bytes: &[u8]) -> Result<ParsedStatus, RepositoryError> {
    let mut records = bytes.split(|byte| *byte == 0).peekable();
    let mut parsed = ParsedStatus {
        head: None,
        branch: None,
        status_entries: Vec::new(),
        untracked_paths: Vec::new(),
        ignored_paths: Vec::new(),
    };

    while let Some(record) = records.next() {
        if record.is_empty() {
            continue;
        }
        let record = std::str::from_utf8(record).map_err(|_| RepositoryError::NonUtf8Path)?;
        if let Some(value) = record.strip_prefix("# branch.oid ") {
            parsed.head = (value != "(initial)").then(|| value.to_owned());
        } else if let Some(value) = record.strip_prefix("# branch.head ") {
            parsed.branch = (value != "(detached)").then(|| value.to_owned());
        } else if let Some(path) = record.strip_prefix("? ") {
            let path = relative_path(path)?;
            parsed.untracked_paths.push(path.clone());
            parsed.status_entries.push(GitStatusEntryV1 {
                path,
                index_status: "?".to_owned(),
                worktree_status: "?".to_owned(),
                original_path: None,
            });
        } else if let Some(path) = record.strip_prefix("! ") {
            parsed
                .ignored_paths
                .push(relative_path(path.trim_end_matches('/'))?);
        } else if record.starts_with("1 ") || record.starts_with("u ") {
            parsed.status_entries.push(parse_ordinary_status(record)?);
        } else if record.starts_with("2 ") {
            let original = records
                .next()
                .ok_or_else(|| RepositoryError::InvalidStatus(record.to_owned()))?;
            let original =
                std::str::from_utf8(original).map_err(|_| RepositoryError::NonUtf8Path)?;
            parsed
                .status_entries
                .push(parse_renamed_status(record, original)?);
        } else if !record.starts_with("# ") {
            return Err(RepositoryError::InvalidStatus(record.to_owned()));
        }
    }

    parsed
        .status_entries
        .sort_by(|left, right| left.path.cmp(&right.path));
    parsed.untracked_paths.sort();
    parsed.ignored_paths.sort();
    Ok(parsed)
}

fn parse_ordinary_status(record: &str) -> Result<GitStatusEntryV1, RepositoryError> {
    let field_count = if record.starts_with("1 ") { 9 } else { 11 };
    let fields = record.splitn(field_count, ' ').collect::<Vec<_>>();
    if fields.len() != field_count {
        return Err(RepositoryError::InvalidStatus(record.to_owned()));
    }
    let xy = fields[1].as_bytes();
    if xy.len() != 2 {
        return Err(RepositoryError::InvalidStatus(record.to_owned()));
    }
    Ok(GitStatusEntryV1 {
        path: relative_path(fields[field_count - 1])?,
        index_status: char::from(xy[0]).to_string(),
        worktree_status: char::from(xy[1]).to_string(),
        original_path: None,
    })
}

fn parse_renamed_status(record: &str, original: &str) -> Result<GitStatusEntryV1, RepositoryError> {
    let fields = record.splitn(10, ' ').collect::<Vec<_>>();
    if fields.len() != 10 || fields[1].len() != 2 {
        return Err(RepositoryError::InvalidStatus(record.to_owned()));
    }
    let xy = fields[1].as_bytes();
    Ok(GitStatusEntryV1 {
        path: relative_path(fields[9])?,
        index_status: char::from(xy[0]).to_string(),
        worktree_status: char::from(xy[1]).to_string(),
        original_path: Some(relative_path(original)?),
    })
}

fn build_fingerprints(
    root: &Path,
    entries: &[GitStatusEntryV1],
    untracked: &[UntrackedPathV1],
    ignored: &[PathBuf],
) -> Result<BTreeMap<PathBuf, String>, RepositoryError> {
    let untracked_by_path = untracked
        .iter()
        .map(|item| (&item.path, item))
        .collect::<BTreeMap<_, _>>();
    let mut result = BTreeMap::new();
    for entry in entries {
        let evidence = if let Some(item) = untracked_by_path.get(&entry.path) {
            format!(
                "untracked:{:?}:{}:{}",
                item.file_type,
                item.size.unwrap_or(0),
                item.digest.as_deref().unwrap_or("")
            )
        } else {
            let path_digest = digest_worktree_path(&root.join(&entry.path))?;
            let index = git_bytes(
                root,
                [
                    "ls-files",
                    "--stage",
                    "-z",
                    "--",
                    path_as_utf8(&entry.path)?,
                ],
            )?;
            format!(
                "{}:{}:{}:{}",
                entry.index_status,
                entry.worktree_status,
                hex::encode(index),
                path_digest
            )
        };
        result.insert(entry.path.clone(), evidence.clone());
        if let Some(original) = &entry.original_path {
            result.insert(original.clone(), format!("source:{evidence}"));
        }
    }
    for path in ignored {
        result.insert(path.clone(), "ignored-visible".to_owned());
    }
    Ok(result)
}

fn inspect_untracked(root: &Path, path: &Path) -> Result<UntrackedPathV1, RepositoryError> {
    let absolute = root.join(path);
    let metadata = fs::symlink_metadata(&absolute).map_err(|source| RepositoryError::Io {
        path: absolute.clone(),
        source,
    })?;
    let file_type = metadata.file_type();
    if file_type.is_file() {
        let bytes = fs::read(&absolute).map_err(|source| RepositoryError::Io {
            path: absolute.clone(),
            source,
        })?;
        Ok(UntrackedPathV1 {
            path: path.to_path_buf(),
            file_type: ObservedFileType::Regular,
            size: Some(metadata.len()),
            digest: Some(sha256(&bytes)),
        })
    } else {
        Ok(UntrackedPathV1 {
            path: path.to_path_buf(),
            file_type: if file_type.is_symlink() {
                ObservedFileType::Symlink
            } else {
                ObservedFileType::Other
            },
            size: None,
            digest: None,
        })
    }
}

fn digest_worktree_path(path: &Path) -> Result<String, RepositoryError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok("missing".to_owned());
        }
        Err(source) => {
            return Err(RepositoryError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if metadata.file_type().is_symlink() {
        let target = fs::read_link(path).map_err(|source| RepositoryError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let target = target.to_str().ok_or(RepositoryError::NonUtf8Path)?;
        Ok(format!("symlink:{}", sha256(target.as_bytes())))
    } else if metadata.is_file() {
        fs::read(path)
            .map(|bytes| format!("file:{}", sha256(&bytes)))
            .map_err(|source| RepositoryError::Io {
                path: path.to_path_buf(),
                source,
            })
    } else {
        Ok("other".to_owned())
    }
}

fn classify_path(path: &Path, scopes: &[WriteScopeV1]) -> ScopeClassificationV1 {
    let matched_scope = scopes.iter().find(|scope| match scope {
        WriteScopeV1::ExactFile { path: exact } => path == exact,
        WriteScopeV1::Subtree { path: subtree } => path == subtree || path.starts_with(subtree),
    });
    ScopeClassificationV1 {
        path: path.to_path_buf(),
        classification: if matched_scope.is_some() {
            ScopeClassification::InScope
        } else {
            ScopeClassification::OutOfScope
        },
        matched_scope: matched_scope.cloned(),
    }
}

fn git_text<I, S>(root: &Path, args: I) -> Result<String, RepositoryError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let (command, output) = run_git(root, args)?;
    String::from_utf8(output.stdout).map_err(|_| RepositoryError::NonUtf8Output { command })
}

fn git_bytes<I, S>(root: &Path, args: I) -> Result<Vec<u8>, RepositoryError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_git(root, args).map(|(_, output)| output.stdout)
}

fn run_git<I, S>(root: &Path, args: I) -> Result<(String, Output), RepositoryError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let arguments = args
        .into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect::<Vec<_>>();
    let description = format!(
        "git {}",
        arguments
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(&arguments)
        .env("LC_ALL", "C")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .map_err(|source| RepositoryError::Git {
            command: description.clone(),
            exit_code: None,
            diagnostics: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(RepositoryError::Git {
            command: description,
            exit_code: output.status.code(),
            diagnostics: String::from_utf8_lossy(&output.stderr)
                .trim()
                .chars()
                .take(2048)
                .collect(),
        });
    }
    Ok((description, output))
}

fn relative_path(value: &str) -> Result<PathBuf, RepositoryError> {
    if value.is_empty() {
        return Err(RepositoryError::InvalidStatus(value.to_owned()));
    }
    Ok(PathBuf::from(value))
}

fn path_as_utf8(path: &Path) -> Result<&str, RepositoryError> {
    path.to_str().ok_or(RepositoryError::NonUtf8Path)
}

fn canonicalize_git_path(path: impl AsRef<Path>) -> Result<PathBuf, RepositoryError> {
    let path = path.as_ref();
    let canonical = fs::canonicalize(path).map_err(|source| RepositoryError::Canonicalize {
        path: path.to_path_buf(),
        source,
    })?;
    path_as_utf8(&canonical)?;
    Ok(canonical)
}

fn digest_optional_file(path: &Path) -> Result<String, RepositoryError> {
    match fs::read(path) {
        Ok(bytes) => Ok(sha256(&bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(sha256(&[])),
        Err(source) => Err(RepositoryError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
