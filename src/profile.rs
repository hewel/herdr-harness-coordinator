//! Coordinator-owned explicit Worker launch profile registry.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::contract::{
    CodexApprovalPolicy, CodexSandboxMode, HarnessId, HarnessKind, HarnessLaunchProfileV1,
    HarnessLaunchProfileV2, HarnessLaunchProfileV3, Validate,
};

/// Version-neutral immutable launch profile contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchProfileSnapshot {
    /// Public schema version that produced this normalized view.
    pub schema_version: u32,
    /// Durable profile ID.
    pub id: HarnessId,
    /// Native Harness Kind.
    pub kind: HarnessKind,
    /// Absolute path or v2 bare command.
    pub executable: String,
    /// Optional native profile selection.
    pub provider_profile: Option<String>,
    /// Explicit model, when present in v1; always present in v2.
    pub model: Option<String>,
    /// Environment allowlist.
    pub inherit_env: Vec<String>,
    /// OMP configuration overlays.
    pub config_overlays: Vec<PathBuf>,
    /// Explicit Codex App Server approval policy, for v3 Codex profiles.
    pub codex_approval_policy: Option<CodexApprovalPolicy>,
    /// Explicit Codex App Server sandbox mode, for v3 Codex profiles.
    pub codex_sandbox_mode: Option<CodexSandboxMode>,
}

/// Exact profile selection retained with a Harness Session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLaunchProfile {
    /// Parsed, validated public profile.
    pub profile: LaunchProfileSnapshot,
    /// Absolute executable resolved for this new Harness Session.
    pub executable: PathBuf,
    /// Exact source file contents.
    pub snapshot: String,
    /// Lowercase SHA-256 of [`Self::snapshot`].
    pub digest: String,
    /// Explicitly inherited environment values that were present.
    pub environment: BTreeMap<String, String>,
}

/// Profile discovery or explicit-resolution failure.
#[derive(Debug, Error)]
pub enum ProfileError {
    /// Profile directory or file could not be read.
    #[error("cannot read launch profile path `{path}`: {source}")]
    Io {
        /// Failed path.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// TOML did not decode to the v1 profile contract.
    #[error("invalid launch profile TOML `{path}`: {source}")]
    Toml {
        /// Invalid file.
        path: PathBuf,
        /// TOML decoder error.
        source: toml::de::Error,
    },
    /// Typed profile validation failed.
    #[error("invalid launch profile `{path}`: {message}")]
    Validation {
        /// Invalid file.
        path: PathBuf,
        /// Contract failure.
        message: String,
    },
    /// Two files declared the same durable profile ID.
    #[error("duplicate launch profile ID `{0}`")]
    Duplicate(HarnessId),
    /// Caller selected no registered profile with this ID.
    #[error("launch profile `{0}` does not exist")]
    NotFound(HarnessId),
    /// Explicit Worker Kind differs from the selected profile.
    #[error("launch profile `{profile}` is not compatible with {actual:?}")]
    KindMismatch {
        /// Selected profile.
        profile: HarnessId,
        /// Requested Harness Kind.
        actual: HarnessKind,
    },
    /// Referenced executable or overlay is not a regular file.
    #[error("launch profile `{profile}` references a missing or non-regular file `{path}`")]
    MissingFile {
        /// Selected profile.
        profile: HarnessId,
        /// Invalid file reference.
        path: PathBuf,
    },
    /// A v2 bare executable was absent from the current `PATH`.
    #[error("launch profile `{profile}` cannot resolve executable `{executable}` through PATH")]
    ExecutableNotFound {
        /// Selected profile.
        profile: HarnessId,
        /// Bare executable name.
        executable: String,
    },
}

#[derive(Debug, Clone)]
struct RegistryEntry {
    profile: LaunchProfileSnapshot,
    snapshot: String,
    digest: String,
}

/// Immutable in-memory registry loaded from Coordinator-owned TOML files.
#[derive(Debug, Clone, Default)]
pub struct ProfileRegistry {
    entries: BTreeMap<HarnessId, RegistryEntry>,
}

impl ProfileRegistry {
    /// Loads every direct `*.toml` child in lexical path order.
    ///
    /// # Errors
    ///
    /// Returns [`ProfileError`] for unreadable, invalid, duplicate, or unsafe profiles.
    pub fn load(directory: &Path) -> Result<Self, ProfileError> {
        let read = fs::read_dir(directory).map_err(|source| ProfileError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let mut paths = read
            .map(|entry| {
                entry
                    .map(|value| value.path())
                    .map_err(|source| ProfileError::Io {
                        path: directory.to_path_buf(),
                        source,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        paths.retain(|path| {
            path.extension()
                .is_some_and(|extension| extension == "toml")
        });
        paths.sort();
        let mut entries = BTreeMap::new();
        for path in paths {
            let snapshot = fs::read_to_string(&path).map_err(|source| ProfileError::Io {
                path: path.clone(),
                source,
            })?;
            let profile = parse_launch_profile_snapshot(&snapshot).map_err(|message| {
                ProfileError::Validation {
                    path: path.clone(),
                    message,
                }
            })?;
            validate_files(&profile)?;
            let id = profile.id.clone();
            let digest = hex::encode(Sha256::digest(snapshot.as_bytes()));
            if entries
                .insert(
                    id.clone(),
                    RegistryEntry {
                        profile,
                        snapshot,
                        digest,
                    },
                )
                .is_some()
            {
                return Err(ProfileError::Duplicate(id));
            }
        }
        Ok(Self { entries })
    }

    /// Lists registered IDs without selecting or ranking one.
    #[must_use]
    pub fn ids(&self) -> Vec<HarnessId> {
        self.entries.keys().cloned().collect()
    }

    /// Resolves exactly the caller-selected ID and Kind.
    ///
    /// Environment is filtered to names explicitly declared by the profile.
    ///
    /// # Errors
    ///
    /// Returns [`ProfileError`] when the profile is absent or has another Kind.
    pub fn resolve<I, K, V>(
        &self,
        id: &HarnessId,
        kind: HarnessKind,
        environment: I,
    ) -> Result<ResolvedLaunchProfile, ProfileError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let entry = self
            .entries
            .get(id)
            .ok_or_else(|| ProfileError::NotFound(id.clone()))?;
        if entry.profile.kind != kind {
            return Err(ProfileError::KindMismatch {
                profile: id.clone(),
                actual: kind,
            });
        }
        resolve_entry(entry, environment)
    }

    /// Resolves one exact profile ID and accepts the Kind declared by that profile.
    ///
    /// # Errors
    ///
    /// Returns [`ProfileError`] when the selected profile is absent or cannot resolve.
    pub fn resolve_selected<I, K, V>(
        &self,
        id: &HarnessId,
        environment: I,
    ) -> Result<ResolvedLaunchProfile, ProfileError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let entry = self
            .entries
            .get(id)
            .ok_or_else(|| ProfileError::NotFound(id.clone()))?;
        resolve_entry(entry, environment)
    }
}

fn resolve_entry<I, K, V>(
    entry: &RegistryEntry,
    environment: I,
) -> Result<ResolvedLaunchProfile, ProfileError>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    let environment = environment
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect::<BTreeMap<_, _>>();
    let executable = resolve_executable(&entry.profile, &environment)?;
    let allow = entry
        .profile
        .inherit_env
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let environment = environment
        .into_iter()
        .filter(|(key, _)| allow.contains(key))
        .collect();
    Ok(ResolvedLaunchProfile {
        profile: entry.profile.clone(),
        executable,
        snapshot: entry.snapshot.clone(),
        digest: entry.digest.clone(),
        environment,
    })
}

fn validate_files(profile: &LaunchProfileSnapshot) -> Result<(), ProfileError> {
    let executable = Path::new(&profile.executable);
    let paths = profile
        .config_overlays
        .iter()
        .map(PathBuf::as_path)
        .chain(executable.is_absolute().then_some(executable));
    for path in paths {
        if !fs::metadata(path).is_ok_and(|metadata| metadata.is_file()) {
            return Err(ProfileError::MissingFile {
                profile: profile.id.clone(),
                path: path.to_path_buf(),
            });
        }
    }
    Ok(())
}

/// Decodes and validates either the immutable v1 or flexible v2 profile contract.
///
/// # Errors
///
/// Returns a validation diagnostic for malformed TOML or unsupported contract versions.
pub fn parse_launch_profile_snapshot(snapshot: &str) -> Result<LaunchProfileSnapshot, String> {
    let value: toml::Value = toml::from_str(snapshot).map_err(|error| error.to_string())?;
    match value
        .get("schema_version")
        .and_then(toml::Value::as_integer)
    {
        Some(1) => {
            let profile: HarnessLaunchProfileV1 =
                toml::from_str(snapshot).map_err(|error| error.to_string())?;
            profile.validate().map_err(|error| error.to_string())?;
            Ok(LaunchProfileSnapshot {
                schema_version: 1,
                id: profile.id,
                kind: profile.kind,
                executable: profile.executable.to_string_lossy().into_owned(),
                provider_profile: Some(profile.provider_profile),
                model: profile.model,
                inherit_env: profile.inherit_env,
                config_overlays: profile.config_overlays,
                codex_approval_policy: None,
                codex_sandbox_mode: None,
            })
        }
        Some(2) => {
            let profile: HarnessLaunchProfileV2 =
                toml::from_str(snapshot).map_err(|error| error.to_string())?;
            profile.validate().map_err(|error| error.to_string())?;
            Ok(LaunchProfileSnapshot {
                schema_version: 2,
                id: profile.id,
                kind: profile.kind,
                executable: profile.executable,
                provider_profile: profile.provider_profile,
                model: Some(profile.model),
                inherit_env: profile.inherit_env,
                config_overlays: profile.config_overlays,
                codex_approval_policy: None,
                codex_sandbox_mode: None,
            })
        }
        Some(3) => {
            let profile: HarnessLaunchProfileV3 =
                toml::from_str(snapshot).map_err(|error| error.to_string())?;
            profile.validate().map_err(|error| error.to_string())?;
            Ok(LaunchProfileSnapshot {
                schema_version: 3,
                id: profile.id,
                kind: profile.kind,
                executable: profile.executable,
                provider_profile: None,
                model: Some(profile.model),
                inherit_env: profile.inherit_env,
                config_overlays: Vec::new(),
                codex_approval_policy: Some(profile.approval_policy),
                codex_sandbox_mode: Some(profile.sandbox_mode),
            })
        }
        Some(version) => Err(format!(
            "unsupported launch profile schema version {version}"
        )),
        None => Err("launch profile schema_version is required".to_owned()),
    }
}

/// Resolves a normalized profile executable for one new Session.
///
/// # Errors
///
/// Returns [`ProfileError`] when an absolute file is missing or a bare command is absent from `PATH`.
pub fn resolve_executable(
    profile: &LaunchProfileSnapshot,
    environment: &BTreeMap<String, String>,
) -> Result<PathBuf, ProfileError> {
    let executable = PathBuf::from(&profile.executable);
    if executable.is_absolute() {
        return fs::metadata(&executable)
            .is_ok_and(|metadata| metadata.is_file())
            .then_some(executable.clone())
            .ok_or_else(|| ProfileError::MissingFile {
                profile: profile.id.clone(),
                path: executable,
            });
    }
    let path = environment
        .get("PATH")
        .map(String::as_str)
        .unwrap_or_default();
    std::env::split_paths(path)
        .map(|directory| directory.join(&profile.executable))
        .find(|candidate| fs::metadata(candidate).is_ok_and(|metadata| metadata.is_file()))
        .ok_or_else(|| ProfileError::ExecutableNotFound {
            profile: profile.id.clone(),
            executable: profile.executable.clone(),
        })
}
