use super::*;
use crate::domain::{actors::*, teams::*};

impl SqliteStore {
    pub(super) async fn load_team(
        conn: &mut SqliteConnection,
        id: TeamId,
    ) -> Result<TeamRecord, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_teams WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let record: TeamRecord = serde_json::from_str(&payload.ok_or(Error::TeamNotFound(id))?)?;
        record.validate_assembly()?;
        Ok(record)
    }
    async fn save_team(conn: &mut SqliteConnection, record: &TeamRecord) -> Result<(), Error> {
        record.validate_assembly()?;
        sqlx::query("UPDATE agent_teams SET status=?,payload=? WHERE id=?")
            .bind(record.status.as_str())
            .bind(serde_json::to_string(record)?)
            .bind(record.id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    pub(super) async fn check_team_actors(
        conn: &mut SqliteConnection,
        record: &TeamRecord,
    ) -> Result<(), Error> {
        for member in &record.members {
            let actor = Self::load_actor(conn, member.agent_id).await?;
            if !member.matches_actor(&actor)? {
                return Err(Error::Unavailable(
                    "team member no longer matches its pinned actor configuration".into(),
                ));
            }
        }
        Ok(())
    }
}
impl TeamStore for SqliteStore {
    fn register_team(&self, record: TeamRecord) -> PortFuture<'_, TeamRecord> {
        Box::pin(async move {
            record.validate_intent()?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let existing: Option<String> =
                sqlx::query_scalar("SELECT payload FROM agent_teams WHERE name=?")
                    .bind(&record.spec.name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            if let Some(payload) = existing {
                let existing: TeamRecord = serde_json::from_str(&payload)?;
                if existing.spec != record.spec {
                    return Err(DomainError::InvalidConfiguration(
                        "team name already has a different pinned configuration; choose a new name"
                            .into(),
                    )
                    .into());
                }
                tx.commit().await.map_err(storage)?;
                return Ok(existing);
            }
            sqlx::query("INSERT INTO agent_teams(id,name,status,payload) VALUES(?,?,?,?)")
                .bind(record.id.to_string())
                .bind(&record.spec.name)
                .bind(record.status.as_str())
                .bind(serde_json::to_string(&record)?)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            for member in &record.members {
                sqlx::query("INSERT INTO agent_team_members(team_id,agent_id,name) VALUES(?,?,?)")
                    .bind(record.id.to_string())
                    .bind(member.agent_id.to_string())
                    .bind(&member.name)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
            tx.commit().await.map_err(storage)?;
            Ok(record)
        })
    }
    fn team(&self, id: TeamId) -> PortFuture<'_, TeamRecord> {
        Box::pin(async move {
            let mut conn = self.pool.acquire().await.map_err(storage)?;
            Self::load_team(&mut conn, id).await
        })
    }
    fn team_named(&self, name: String) -> PortFuture<'_, Option<TeamRecord>> {
        Box::pin(async move {
            let payload: Option<String> =
                sqlx::query_scalar("SELECT payload FROM agent_teams WHERE name=?")
                    .bind(name)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(storage)?;
            payload
                .map(|payload| serde_json::from_str(&payload))
                .transpose()
                .map_err(Into::into)
        })
    }
    fn save_team_workspace(
        &self,
        id: TeamId,
        actor: AgentId,
        workspace: TeamWorkspace,
        now: i64,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            workspace.validate()?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut record = Self::load_team(&mut tx, id).await?;
            if record.status != TeamStatus::Preparing {
                return Err(DomainError::InvalidState.into());
            }
            if record
                .members
                .iter()
                .filter_map(|m| m.workspace.as_ref())
                .any(|saved| saved.snapshot_digest != workspace.snapshot_digest)
            {
                return Err(Error::Unavailable(
                    "team members require the same initial snapshot".into(),
                ));
            }
            let member = record
                .members
                .iter_mut()
                .find(|m| m.agent_id == actor)
                .ok_or(DomainError::InvalidState)?;
            if !member.needs_workspace()
                || member
                    .workspace
                    .as_ref()
                    .is_some_and(|saved| saved != &workspace)
            {
                return Err(Error::Unavailable(
                    "team workspace receipt conflicts with the setup intent".into(),
                ));
            }
            member.workspace = Some(workspace);
            record.updated_at = now;
            Self::save_team(&mut tx, &record).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn team_setup_error(&self, id: TeamId, error: Option<String>, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut record = Self::load_team(&mut tx, id).await?;
            if record.status == TeamStatus::Preparing {
                record.error = error;
                record.updated_at = now;
                Self::save_team(&mut tx, &record).await?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn publish_team(&self, id: TeamId, actors: Vec<Agent>, now: i64) -> PortFuture<'_, TeamRecord> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut record = Self::load_team(&mut tx, id).await?;
            if record.status == TeamStatus::Ready {
                Self::check_team_actors(&mut tx, &record).await?;
                tx.commit().await.map_err(storage)?;
                return Ok(record);
            }
            if actors.len() != record.members.len() {
                return Err(DomainError::InvalidState.into());
            }
            for (member, actor) in record.members.iter().zip(&actors) {
                if !member.matches_actor(actor)?
                    || actor.record().revision != 0
                    || actor.record().status != AgentStatus::Ready
                {
                    return Err(DomainError::InvalidState.into());
                }
                sqlx::query("INSERT INTO agent_actors(id,status,payload) VALUES(?,'ready',?)")
                    .bind(member.agent_id.to_string())
                    .bind(serde_json::to_string(actor.record())?)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
            record.status = TeamStatus::Ready;
            record.error = None;
            record.updated_at = now;
            Self::save_team(&mut tx, &record).await?;
            tx.commit().await.map_err(storage)?;
            Ok(record)
        })
    }
    fn submit_team(
        &self,
        id: TeamId,
        message: AgentMessage,
        now: i64,
    ) -> PortFuture<'_, MessageId> {
        Box::pin(async move {
            message.validate_public()?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let record = Self::load_team(&mut tx, id).await?;
            if record.status != TeamStatus::Ready
                || message.recipient != record.planner()?.agent_id
                || message.kind != "task"
                || message.sender.is_some()
                || message.correlation_id.is_some()
            {
                return Err(Error::Unavailable(
                    "team must be ready before submitting a planner task".into(),
                ));
            }
            Self::check_team_actors(&mut tx, &record).await?;
            let result = Self::insert_message(&mut tx, message.clone(), now).await?;
            Self::insert_actor_task(&mut tx, &message, None, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(result)
        })
    }
    fn team_view(&self, id: TeamId) -> PortFuture<'_, TeamView> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let record = Self::load_team(&mut tx, id).await?;
            if record.status == TeamStatus::Ready {
                Self::check_team_actors(&mut tx, &record).await?;
            }
            let mut members = Vec::with_capacity(record.members.len());
            for member in &record.members {
                let (status, queue) = if record.status == TeamStatus::Ready {
                    (
                        Some(
                            Self::load_actor(&mut tx, member.agent_id)
                                .await?
                                .record()
                                .status,
                        ),
                        Some(Self::load_actor_queue(&mut tx, member.agent_id).await?),
                    )
                } else {
                    (None, None)
                };
                members.push(TeamMemberView {
                    name: member.name.clone(),
                    role: member.role,
                    agent_id: member.agent_id,
                    workspace: member.workspace.clone(),
                    status,
                    queue,
                });
            }
            tx.commit().await.map_err(storage)?;
            Ok(TeamView {
                team: TeamSummary::from(&record),
                members,
            })
        })
    }
    fn list_teams(&self) -> PortFuture<'_, TeamList> {
        Box::pin(async move {
            let payloads: Vec<String> =
                sqlx::query_scalar("SELECT payload FROM agent_teams ORDER BY rowid DESC LIMIT 101")
                    .fetch_all(&self.pool)
                    .await
                    .map_err(storage)?;
            let truncated = payloads.len() > 100;
            let teams = payloads
                .iter()
                .take(100)
                .map(|p| {
                    serde_json::from_str::<TeamRecord>(p).map(|record| TeamSummary::from(&record))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(TeamList { teams, truncated })
        })
    }
}
