use act0r::{
    Error,
    adapters::sqlite::SqliteStore,
    application::{ProjectProfileService, ports::ProjectProfileStorePort},
    domain::{project_profiles::*, *},
};
use std::sync::Arc;

fn settings(root: &std::path::Path, name: &str) -> ProjectSettings {
    ProjectSettings {
        name: name.into(),
        workspace: root.to_string_lossy().into_owned(),
        coding: None,
    }
}
fn save(settings: ProjectSettings, revision: u64) -> ProfileRequest {
    ProfileRequest {
        request_id: MessageId::new(),
        expected_revision: revision,
        action: ProfileAction::Save { settings },
    }
}
fn conflict(result: Result<ProfileReceipt, Error>) {
    assert!(matches!(
        result,
        Err(Error::Domain(DomainError::ProjectProfileConflict(_)))
    ));
}

#[test]
fn profile_validation_reuses_coding_permissions_and_bounds_configuration() {
    let root = tempfile::tempdir().unwrap();
    let valid = settings(root.path(), "  Rust 🦀  ");
    let id = ProjectProfileId::new();
    let initial = save(valid.clone(), 0).apply(id, None).unwrap();
    assert_eq!(initial.settings.name, "Rust 🦀");
    assert_eq!(initial.revision, 1);
    for name in [" ".to_string(), "bad\nname".into(), "🦀".repeat(65)] {
        let mut invalid = valid.clone();
        invalid.name = name;
        assert!(invalid.validate().is_err());
    }
    for path in [
        "relative".to_string(),
        "".into(),
        format!("{}\0", valid.workspace),
        "x".repeat(4097),
    ] {
        let mut invalid = valid.clone();
        invalid.workspace = path;
        assert!(invalid.validate().is_err());
    }
    let mut coding = valid.clone();
    coding.coding = Some(harness::HarnessCoding {
        coder_count: 1,
        writable_paths: vec!["src/".into()],
        test_command: tools::WorkspaceTestCommand {
            argv: vec!["cargo".into(), "test".into()],
            timeout_ms: 60000,
        },
        project_commands: [(
            "format".into(),
            tools::WorkspaceTestCommand {
                argv: vec!["cargo".into(), "fmt".into()],
                timeout_ms: 60000,
            },
        )]
        .into(),
    });
    coding.validate().unwrap();
    coding.coding.as_mut().unwrap().writable_paths = vec!["../secret".into()];
    assert!(coding.validate().is_err());
    let mut too_large = valid.clone();
    too_large.coding = Some(harness::HarnessCoding {
        coder_count: 1,
        writable_paths: vec!["src/".into()],
        test_command: tools::WorkspaceTestCommand {
            argv: vec!["runner".into(), "\u{1}".repeat(14000)],
            timeout_ms: 60000,
        },
        project_commands: Default::default(),
    });
    assert!(too_large.validate().is_err());
    let mut request = save(valid, 0);
    request.request_id = "00000000-0000-0000-0000-000000000000".parse().unwrap();
    assert!(request.validate().is_err());
    assert!(serde_json::from_value::<ProfileRequest>(serde_json::json!({"request_id":MessageId::new(),"expected_revision":0,"action":{"kind":"remove","settings":{}}})).is_err());
}

#[tokio::test]
async fn parallel_coding_profile_retains_worker_choice_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parallel-profile.db");
    let mut project = settings(root.path(), "Two coders");
    project.coding = Some(harness::HarnessCoding {
        coder_count: 2,
        writable_paths: vec!["src/".into()],
        test_command: tools::WorkspaceTestCommand {
            argv: vec!["cargo".into(), "test".into()],
            timeout_ms: 60000,
        },
        project_commands: Default::default(),
    });
    let id = ProjectProfileId::new();
    let decision = save(project.clone(), 0);
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let service = ProjectProfileService::new(store.clone());
    service.change(id, decision.clone()).await.unwrap();
    drop(service);
    store.close().await;
    drop(store);
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let service = ProjectProfileService::new(store.clone());
    assert_eq!(service.get(id).await.unwrap().settings, project);
    assert!(service.change(id, decision).await.unwrap().replayed);
    let mut invalid = project;
    invalid.coding.as_mut().unwrap().coder_count = 3;
    assert!(service.change(id, save(invalid, 1)).await.is_err());
    assert_eq!(
        service
            .get(id)
            .await
            .unwrap()
            .settings
            .coding
            .unwrap()
            .coder_count,
        2
    );
    drop(service);
    store.close().await;
}

#[tokio::test]
async fn concurrent_edits_exact_receipts_and_removed_identities_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("profiles.db");
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let service = ProjectProfileService::new(store.clone());
    let id = ProjectProfileId::new();
    let create = save(settings(root.path(), "Rust"), 0);
    assert!(!service.change(id, create.clone()).await.unwrap().replayed);
    let a = save(settings(root.path(), "First"), 1);
    let b = save(settings(root.path(), "Second"), 1);
    let (a, b) = tokio::join!(service.change(id, a), service.change(id, b));
    assert_ne!(a.is_ok(), b.is_ok());
    let original = service.change(id, create.clone()).await.unwrap();
    assert!(original.replayed);
    assert_eq!(original.profile.revision, 1);
    assert_eq!(service.get(id).await.unwrap().revision, 2);
    let mut changed = create.clone();
    changed.action = ProfileAction::Save {
        settings: settings(root.path(), "Different"),
    };
    conflict(service.change(id, changed).await);
    conflict(
        service
            .change(ProjectProfileId::new(), create.clone())
            .await,
    );
    let current = service.get(id).await.unwrap();
    conflict(
        service
            .change(
                ProjectProfileId::new(),
                save(
                    settings(root.path(), &current.settings.name.to_ascii_lowercase()),
                    0,
                ),
            )
            .await,
    );
    let remove = ProfileRequest {
        request_id: MessageId::new(),
        expected_revision: 2,
        action: ProfileAction::Remove {},
    };
    service.change(id, remove.clone()).await.unwrap();
    assert!(service.list().await.unwrap().items.is_empty());
    assert!(matches!(
        service.get(id).await,
        Err(Error::ProjectProfileNotFound(_))
    ));
    conflict(
        service
            .change(id, save(settings(root.path(), "Restored"), 3))
            .await,
    );
    let replacement = ProjectProfileId::new();
    service
        .change(
            replacement,
            save(settings(root.path(), &current.settings.name), 0),
        )
        .await
        .unwrap();
    drop(service);
    store.close().await;
    drop(store);
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let service = ProjectProfileService::new(store.clone());
    assert!(service.change(id, remove).await.unwrap().replayed);
    let replay = service.change(id, create).await.unwrap();
    assert!(replay.replayed);
    assert!(!replay.profile.deleted);
    assert_eq!(replay.profile.revision, 1);
    assert_eq!(service.list().await.unwrap().items.len(), 1);
    assert_eq!(service.get(replacement).await.unwrap().revision, 1);
    drop(service);
    store.close().await;
}

#[tokio::test]
async fn failed_receipt_commit_rolls_back_configuration_and_identity() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("rollback.db");
    let store = SqliteStore::open(&db).await.unwrap();
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_profile_receipt BEFORE INSERT ON project_profile_receipts BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").execute(&pool).await.unwrap();
    let id = ProjectProfileId::new();
    let request = save(settings(root.path(), "Rollback"), 0);
    assert!(matches!(
        store.change_project_profile(id, request.clone()).await,
        Err(Error::Storage(_))
    ));
    assert!(store.project_profiles().await.unwrap().items.is_empty());
    sqlx::raw_sql("DROP TRIGGER reject_profile_receipt")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !store
            .change_project_profile(id, request)
            .await
            .unwrap()
            .replayed
    );
    let original = store.project_profile(id).await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_profile_receipt BEFORE INSERT ON project_profile_receipts BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").execute(&pool).await.unwrap();
    assert!(
        store
            .change_project_profile(id, save(settings(root.path(), "Changed"), 1))
            .await
            .is_err()
    );
    assert_eq!(store.project_profile(id).await.unwrap(), original);
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn bounded_catalog_capacity_is_released_by_removal() {
    let root = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(root.path().join("capacity.db"))
        .await
        .unwrap();
    let mut first = None;
    for i in 0..MAX_PROFILES {
        let id = ProjectProfileId::new();
        if first.is_none() {
            first = Some(id);
        }
        store
            .change_project_profile(
                id,
                save(settings(root.path(), &format!("Project {i:03}")), 0),
            )
            .await
            .unwrap();
    }
    let last = ProjectProfileId::new();
    let request = save(settings(root.path(), "Last"), 0);
    conflict(store.change_project_profile(last, request.clone()).await);
    store
        .change_project_profile(
            first.unwrap(),
            ProfileRequest {
                request_id: MessageId::new(),
                expected_revision: 1,
                action: ProfileAction::Remove {},
            },
        )
        .await
        .unwrap();
    assert!(
        !store
            .change_project_profile(last, request)
            .await
            .unwrap()
            .replayed
    );
    let list = store.project_profiles().await.unwrap();
    assert_eq!(list.items.len(), 100);
    assert_eq!(list.capacity, 100);
    store.close().await;
}
