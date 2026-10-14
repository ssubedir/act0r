use super::*;
use crate::domain::project_profiles::*;

fn profile(row: &sqlx::sqlite::SqliteRow) -> Result<ProjectProfile, Error> {
    Ok(ProjectProfile {
        id: row
            .try_get::<String, _>("id")
            .map_err(storage)?
            .parse()
            .map_err(|_| Error::Storage("invalid project profile identity".into()))?,
        revision: row.try_get::<i64, _>("revision").map_err(storage)? as u64,
        deleted: row.try_get("deleted").map_err(storage)?,
        settings: serde_json::from_str(&row.try_get::<String, _>("settings").map_err(storage)?)?,
    })
}
impl ProjectProfileStorePort for SqliteStore {
    fn project_profiles(&self) -> PortFuture<'_, ProjectProfiles> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id,revision,deleted,settings FROM project_profiles WHERE deleted=0 ORDER BY name COLLATE NOCASE,id LIMIT 100")
                .fetch_all(&self.pool).await.map_err(storage)?;
            Ok(ProjectProfiles {
                items: rows.iter().map(profile).collect::<Result<_, _>>()?,
                capacity: MAX_PROFILES,
            })
        })
    }
    fn project_profile(&self, id: ProjectProfileId) -> PortFuture<'_, ProjectProfile> {
        Box::pin(async move {
            let row = sqlx::query("SELECT id,revision,deleted,settings FROM project_profiles WHERE id=? AND deleted=0")
                .bind(id.to_string()).fetch_optional(&self.pool).await.map_err(storage)?.ok_or(Error::ProjectProfileNotFound(id))?;
            profile(&row)
        })
    }
    fn change_project_profile(
        &self,
        id: ProjectProfileId,
        request: ProfileRequest,
    ) -> PortFuture<'_, ProfileReceipt> {
        Box::pin(async move {
            request.validate()?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            if let Some(row) = sqlx::query("SELECT profile_id,request,receipt FROM project_profile_receipts WHERE request_id=?")
                .bind(request.request_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)? {
                let original: ProfileRequest = serde_json::from_str(&row.try_get::<String, _>("request").map_err(storage)?)?;
                if row.try_get::<String, _>("profile_id").map_err(storage)? != id.to_string() || original != request {
                    return Err(DomainError::ProjectProfileConflict("request identity already belongs to another profile decision".into()).into());
                }
                let mut receipt: ProfileReceipt = serde_json::from_str(&row.try_get::<String, _>("receipt").map_err(storage)?)?;
                receipt.replayed = true;
                tx.commit().await.map_err(storage)?;
                return Ok(receipt);
            }
            let current =
                sqlx::query("SELECT id,revision,deleted,settings FROM project_profiles WHERE id=?")
                    .bind(id.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?
                    .as_ref()
                    .map(profile)
                    .transpose()?;
            let next = request.apply(id, current.as_ref())?;
            if current.is_none() {
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM project_profiles WHERE deleted=0")
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(storage)?;
                if count >= MAX_PROFILES as i64 {
                    return Err(DomainError::ProjectProfileConflict(
                        "at most 100 active profiles; remove an unused profile first".into(),
                    )
                    .into());
                }
            }
            if !next.deleted {
                let duplicate: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_profiles WHERE deleted=0 AND name=? COLLATE NOCASE AND id<>?)")
                    .bind(&next.settings.name).bind(id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
                if duplicate {
                    return Err(DomainError::ProjectProfileConflict(
                        "another profile already uses that name".into(),
                    )
                    .into());
                }
            }
            sqlx::query("INSERT INTO project_profiles(id,name,revision,deleted,settings) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,revision=excluded.revision,deleted=excluded.deleted,settings=excluded.settings")
                .bind(id.to_string()).bind(&next.settings.name).bind(next.revision as i64).bind(next.deleted).bind(serde_json::to_string(&next.settings)?).execute(&mut *tx).await.map_err(storage)?;
            let receipt = ProfileReceipt {
                profile: next,
                replayed: false,
            };
            sqlx::query("INSERT INTO project_profile_receipts(request_id,profile_id,request,receipt) VALUES(?,?,?,?)")
                .bind(request.request_id.to_string()).bind(id.to_string()).bind(serde_json::to_string(&request)?).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            Ok(receipt)
        })
    }
}
