use std::{
    fs,
    os::unix::{ffi::OsStringExt, fs::symlink},
    path::{Path, PathBuf},
    process::Command,
};

use herdr_harness_coordinator::{
    contract::{ObservedFileType, ScopeClassification, WriteScopeV1},
    repository::{GitRepository, RepositoryError},
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

struct TestRepository {
    directory: TempDir,
}

impl TestRepository {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary repository");
        git(directory.path(), ["init", "--quiet"]);
        git(directory.path(), ["config", "user.name", "Test User"]);
        git(
            directory.path(),
            ["config", "user.email", "test@example.invalid"],
        );
        fs::write(directory.path().join("tracked.txt"), "baseline\n").expect("tracked file");
        git(directory.path(), ["add", "tracked.txt"]);
        git(directory.path(), ["commit", "--quiet", "-m", "initial"]);
        Self { directory }
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }

    fn repository(&self) -> GitRepository {
        GitRepository::open(self.path()).expect("open test repository")
    }
}

#[test]
fn open_should_resolve_canonical_worktree_and_common_directory() {
    let repository = TestRepository::new();
    let opened = repository.repository();

    assert_eq!(
        opened.identity().worktree_root,
        fs::canonicalize(repository.path()).expect("canonical repository")
    );
    assert_eq!(
        opened.identity().git_common_dir,
        fs::canonicalize(repository.path().join(".git")).expect("canonical Git directory")
    );
}

#[test]
fn open_should_reject_non_git_directory() {
    let directory = tempfile::tempdir().expect("temporary directory");

    let error = GitRepository::open(directory.path()).expect_err("non-Git path must fail");

    assert!(matches!(error, RepositoryError::Git { .. }));
}

#[test]
fn open_should_reject_bare_repository() {
    let directory = tempfile::tempdir().expect("temporary directory");
    git(directory.path(), ["init", "--bare", "--quiet"]);

    let error = GitRepository::open(directory.path()).expect_err("bare repository must fail");

    assert!(matches!(error, RepositoryError::BareRepository));
}

#[test]
fn open_should_reject_a_selected_subdirectory() {
    let repository = TestRepository::new();
    let nested = repository.path().join("nested");
    fs::create_dir(&nested).expect("nested directory");

    let error = GitRepository::open(&nested).expect_err("root mismatch must fail");

    assert!(matches!(error, RepositoryError::RootMismatch { .. }));
}

#[test]
fn open_should_share_common_directory_but_not_identity_between_linked_worktrees() {
    let repository = TestRepository::new();
    let linked_parent = tempfile::tempdir().expect("linked worktree parent");
    let linked = linked_parent.path().join("linked");
    let linked_text = linked.to_str().expect("UTF-8 worktree path");
    git(
        repository.path(),
        ["worktree", "add", "--quiet", "-b", "linked", linked_text],
    );

    let main = repository.repository();
    let other = GitRepository::open(&linked).expect("open linked worktree");

    assert_eq!(
        main.identity().git_common_dir,
        other.identity().git_common_dir
    );
    assert_ne!(
        main.identity().worktree_root,
        other.identity().worktree_root
    );
}

#[test]
fn observe_should_capture_head_branch_status_diffs_untracked_and_ignored() {
    let repository = TestRepository::new();
    fs::write(repository.path().join(".gitignore"), "ignored.bin\n").expect("ignore file");
    fs::write(repository.path().join("tracked.txt"), b"staged\0binary\n").expect("staged content");
    git(repository.path(), ["add", "tracked.txt"]);
    fs::write(repository.path().join("tracked.txt"), b"unstaged\0binary\n")
        .expect("unstaged content");
    fs::write(repository.path().join("new.bin"), b"new\0bytes").expect("untracked file");
    fs::write(repository.path().join("ignored.bin"), b"ignored").expect("ignored file");
    symlink("tracked.txt", repository.path().join("new-link")).expect("untracked symlink");

    let snapshot = repository
        .repository()
        .observe()
        .expect("repository observation");
    let regular = snapshot
        .untracked
        .iter()
        .find(|item| item.path == Path::new("new.bin"))
        .expect("regular untracked evidence");
    let link = snapshot
        .untracked
        .iter()
        .find(|item| item.path == Path::new("new-link"))
        .expect("symlink evidence");

    assert!(snapshot.head.is_some());
    assert!(snapshot.branch.is_some());
    assert!(!snapshot.staged_diff.is_empty());
    assert!(!snapshot.unstaged_diff.is_empty());
    assert!(
        snapshot
            .status_entries
            .iter()
            .any(|entry| entry.path == Path::new("tracked.txt"))
    );
    assert_eq!(regular.file_type, ObservedFileType::Regular);
    assert_eq!(regular.size, Some(9));
    assert_eq!(
        regular.digest,
        Some(hex::encode(Sha256::digest(b"new\0bytes")))
    );
    assert_eq!(link.file_type, ObservedFileType::Symlink);
    assert!(
        snapshot
            .ignored_paths
            .contains(&PathBuf::from("ignored.bin"))
    );
}

#[test]
fn observe_should_represent_an_unborn_branch_without_a_head_commit() {
    let directory = tempfile::tempdir().expect("temporary repository");
    git(directory.path(), ["init", "--quiet"]);

    let snapshot = GitRepository::open(directory.path())
        .expect("open unborn repository")
        .observe()
        .expect("observe unborn repository");

    assert_eq!(snapshot.head, None);
    assert!(snapshot.branch.is_some());
}

#[test]
fn compare_to_should_preserve_unchanged_dirty_baseline_and_detect_later_edits() {
    let repository = TestRepository::new();
    fs::write(repository.path().join("tracked.txt"), "user edit\n").expect("baseline user edit");
    fs::write(repository.path().join("existing.txt"), "user file\n")
        .expect("baseline untracked file");
    let git_repository = repository.repository();
    let baseline = git_repository.observe().expect("dirty baseline");

    fs::write(repository.path().join("worker.txt"), "worker edit\n").expect("worker file");
    let checkpoint = git_repository.observe().expect("worker checkpoint");
    let first = checkpoint.compare_to(
        &baseline,
        &[WriteScopeV1::ExactFile {
            path: PathBuf::from("worker.txt"),
        }],
    );

    assert_eq!(first.changed_paths, vec![PathBuf::from("worker.txt")]);

    fs::write(repository.path().join("tracked.txt"), "user then worker\n")
        .expect("changed dirty file");
    let later = git_repository.observe().expect("later checkpoint");
    let second = later.compare_to(&baseline, &[]);

    assert!(second.changed_paths.contains(&PathBuf::from("tracked.txt")));
}

#[test]
fn compare_to_should_match_exact_and_subtree_scopes_on_component_boundaries() {
    let repository = TestRepository::new();
    let git_repository = repository.repository();
    let baseline = git_repository.observe().expect("baseline");
    fs::create_dir(repository.path().join("src")).expect("source directory");
    fs::write(repository.path().join("src/lib.rs"), "lib\n").expect("in-scope file");
    fs::write(repository.path().join("src-copy"), "copy\n").expect("out-of-scope file");
    fs::write(repository.path().join("README.md"), "readme\n").expect("exact file");

    let checkpoint = git_repository.observe().expect("checkpoint");
    let comparison = checkpoint.compare_to(
        &baseline,
        &[
            WriteScopeV1::Subtree {
                path: PathBuf::from("src"),
            },
            WriteScopeV1::ExactFile {
                path: PathBuf::from("README.md"),
            },
        ],
    );

    let classifications = comparison
        .scope_classifications
        .iter()
        .map(|item| (item.path.clone(), item.classification))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        classifications[Path::new("src/lib.rs")],
        ScopeClassification::InScope
    );
    assert_eq!(
        classifications[Path::new("README.md")],
        ScopeClassification::InScope
    );
    assert_eq!(
        classifications[Path::new("src-copy")],
        ScopeClassification::OutOfScope
    );
}

#[test]
fn compare_to_should_classify_both_sides_of_a_rename() {
    let repository = TestRepository::new();
    let git_repository = repository.repository();
    let baseline = git_repository.observe().expect("baseline");
    git(repository.path(), ["mv", "tracked.txt", "renamed.txt"]);

    let checkpoint = git_repository.observe().expect("checkpoint");
    let comparison = checkpoint.compare_to(
        &baseline,
        &[WriteScopeV1::ExactFile {
            path: PathBuf::from("renamed.txt"),
        }],
    );

    assert!(
        comparison
            .changed_paths
            .contains(&PathBuf::from("tracked.txt"))
    );
    assert!(
        comparison
            .changed_paths
            .contains(&PathBuf::from("renamed.txt"))
    );
    assert!(comparison.scope_classifications.iter().any(|item| {
        item.path == Path::new("tracked.txt")
            && item.classification == ScopeClassification::OutOfScope
    }));
}

#[test]
fn observe_should_reject_non_utf8_repository_paths() {
    let repository = TestRepository::new();
    let invalid = std::ffi::OsString::from_vec(vec![b'n', b'o', b'n', 0xff]);
    fs::write(repository.path().join(invalid), "content").expect("non-UTF-8 file");

    let error = repository
        .repository()
        .observe()
        .expect_err("non-UTF-8 path must fail");

    assert!(matches!(error, RepositoryError::NonUtf8Path));
}

fn git<I, S>(root: &Path, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .expect("run Git command");
    assert!(
        output.status.success(),
        "Git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
