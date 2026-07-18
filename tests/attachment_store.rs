use std::{
    fs::Permissions,
    os::unix::fs::{PermissionsExt, symlink},
};

use herdr_harness_coordinator::attachment::{AttachmentError, AttachmentStore};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn admission_copies_a_regular_file_and_records_immutable_metadata() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let source_dir = tempfile::tempdir().expect("source directory must exist");
    let source = source_dir.path().join("report.txt");
    let contents = b"verification passed\n";
    tokio::fs::write(&source, contents)
        .await
        .expect("source must be writable");
    let store = AttachmentStore::new(state.path());

    let attachment = store
        .admit(&source, "text/plain", "report.txt")
        .await
        .expect("regular file admission must succeed");

    assert_eq!(attachment.digest, hex::encode(Sha256::digest(contents)));
    assert_eq!(attachment.size_bytes, contents.len() as u64);
    assert_eq!(attachment.media_type, "text/plain");
    assert_eq!(attachment.original_name, "report.txt");
    assert_eq!(
        attachment.storage_path,
        std::path::PathBuf::from("attachments").join(attachment.id.to_string())
    );
    assert_eq!(
        tokio::fs::read(state.path().join(&attachment.storage_path))
            .await
            .expect("stored content must be readable"),
        contents
    );
    store
        .verify(&attachment)
        .await
        .expect("fresh attachment must verify");
}

#[tokio::test]
async fn configured_size_limit_rejects_oversized_input_without_publishing_it() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let source = state.path().join("large.bin");
    tokio::fs::write(&source, b"12345")
        .await
        .expect("source must be writable");
    let store = AttachmentStore::with_max_bytes(state.path(), 4);

    let error = store
        .admit(&source, "application/octet-stream", "large.bin")
        .await
        .expect_err("oversized input must fail");

    assert!(matches!(error, AttachmentError::TooLarge { limit: 4 }));
    let published = std::fs::read_dir(state.path().join("attachments"))
        .expect("attachment directory must exist")
        .count();
    assert_eq!(published, 0);
}

#[tokio::test]
async fn admission_rejects_a_final_symlink() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let source_dir = tempfile::tempdir().expect("source directory must exist");
    let target = source_dir.path().join("target.txt");
    let link = source_dir.path().join("link.txt");
    tokio::fs::write(&target, b"content")
        .await
        .expect("target must be writable");
    symlink(&target, &link).expect("symlink must be creatable");
    let store = AttachmentStore::new(state.path());

    let error = store
        .admit(&link, "text/plain", "link.txt")
        .await
        .expect_err("final symlink must fail");

    assert!(matches!(error, AttachmentError::InvalidSource { .. }));
}

#[tokio::test]
async fn admission_rejects_a_directory_and_an_unreadable_file() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let source_dir = tempfile::tempdir().expect("source directory must exist");
    let unreadable = source_dir.path().join("unreadable.txt");
    tokio::fs::write(&unreadable, b"content")
        .await
        .expect("source must be writable");
    std::fs::set_permissions(&unreadable, Permissions::from_mode(0o000))
        .expect("permissions must be changeable");
    let store = AttachmentStore::new(state.path());

    let directory_error = store
        .admit(source_dir.path(), "application/octet-stream", "directory")
        .await
        .expect_err("directory must fail");
    let unreadable_error = store
        .admit(&unreadable, "text/plain", "unreadable.txt")
        .await
        .expect_err("unreadable file must fail");

    assert!(matches!(
        directory_error,
        AttachmentError::InvalidSource { .. }
    ));
    assert!(matches!(
        unreadable_error,
        AttachmentError::InvalidSource { .. }
    ));
}

#[tokio::test]
async fn verification_detects_digest_mismatch() {
    let state = tempfile::tempdir().expect("state directory must exist");
    let source = state.path().join("evidence.log");
    tokio::fs::write(&source, b"original")
        .await
        .expect("source must be writable");
    let store = AttachmentStore::new(state.path());
    let attachment = store
        .admit(&source, "text/plain", "evidence.log")
        .await
        .expect("admission must succeed");
    tokio::fs::write(state.path().join(&attachment.storage_path), b"tampered")
        .await
        .expect("stored file must be writable for corruption test");

    let error = store
        .verify(&attachment)
        .await
        .expect_err("tampering must be detected");

    assert!(matches!(error, AttachmentError::Corrupt { .. }));
}
