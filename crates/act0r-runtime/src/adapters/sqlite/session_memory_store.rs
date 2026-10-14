use super::*;
use crate::domain::{actors::*, session_memory::*, teams::*};

impl SqliteStore {
    async fn memory_team(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<TeamRecord, Error> {
        let team: Option<String> =
            sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
                .bind(session.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let team = Self::load_team(
            conn,
            team.ok_or(Error::SessionNotFound(session))?
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
        )
        .await?;
        Self::check_team_actors(conn, &team).await?;
        Ok(team)
    }
    pub(super) async fn read_session_memory_actors(
        &self,
        session: SessionId,
    ) -> Result<Vec<(TeamMember, Agent)>, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let team = Self::memory_team(&mut tx, session).await?;
        let mut items = Vec::new();
        for member in team.members {
            let actor = Self::load_actor(&mut tx, member.agent_id).await?;
            items.push((member, actor));
        }
        tx.commit().await.map_err(storage)?;
        Ok(items)
    }
    pub(super) async fn read_turn_memory(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<TurnMemory, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let team = Self::memory_team(&mut tx, session).await?;
        Self::harness_context(&mut tx, session, turn).await?;
        // Read only immutable launch memory, not current state or original pre-fit actor memory.
        let rows = sqlx::query("SELECT a.agent_id,a.request_id,r.id,r.status,r.created_at,
            json_extract(r.payload,'$.input.input.actor_memory') AS memory,
            json_extract(r.payload,'$.input.input.session_summary') AS session_summary
            FROM harness_turn_ownership o JOIN agent_runs a ON a.request_id=o.id JOIN runs r ON r.id=a.run_id
            WHERE o.root_task_id=? ORDER BY r.created_at,r.id LIMIT 129")
            .bind(turn.to_string()).fetch_all(&mut *tx).await.map_err(storage)?;
        let truncated = rows.len() > 128;
        let mut runs = Vec::new();
        for row in rows.into_iter().take(128) {
            let agent_id: AgentId = row
                .try_get::<String, _>("agent_id")
                .map_err(storage)?
                .parse()
                .map_err(|_| DomainError::InvalidState)?;
            let member = team
                .members
                .iter()
                .find(|m| m.agent_id == agent_id)
                .ok_or(DomainError::InvalidState)?;
            let raw: Option<String> = row.try_get("memory").map_err(storage)?;
            let memory: Option<MemorySnapshot> =
                raw.map(|raw| serde_json::from_str(&raw)).transpose()?;
            if let Some(memory) = &memory {
                memory.validate()?;
                if memory.actor_id != agent_id {
                    return Err(DomainError::InvalidState.into());
                }
            }
            runs.push(TurnMemoryRun {
                session_summary: row
                    .try_get::<Option<String>, _>("session_summary")
                    .map_err(storage)?
                    .map(|raw| serde_json::from_str(&raw))
                    .transpose()?,
                agent_id,
                name: member.name.clone(),
                memory,
                run_id: row
                    .try_get::<String, _>("id")
                    .map_err(storage)?
                    .parse()
                    .map_err(|_| DomainError::InvalidState)?,
                task_id: row
                    .try_get::<String, _>("request_id")
                    .map_err(storage)?
                    .parse()
                    .map_err(|_| DomainError::InvalidState)?,
                status: row.try_get("status").map_err(storage)?,
                created_at: row.try_get("created_at").map_err(storage)?,
            });
        }
        tx.commit().await.map_err(storage)?;
        Ok(TurnMemory {
            session_id: session,
            turn_id: turn,
            runs,
            truncated,
        })
    }
    pub(super) async fn change_session_memory(
        &self,
        session: SessionId,
        agent_id: AgentId,
        request: ClearMemoryRequest,
        now: i64,
        decision: ActorMemoryDecision,
    ) -> Result<ClearMemoryReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let team = Self::memory_team(&mut tx, session).await?;
        if !team.members.iter().any(|m| m.agent_id == agent_id) {
            return Err(Error::AgentNotFound(agent_id));
        }
        let saved: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_memory_clears WHERE request_id=?")
                .bind(request.request_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if let Some(saved) = saved {
            let mut receipt: ClearMemoryReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session
                || receipt.agent_id != agent_id
                || receipt.request != request
            {
                return Err(DomainError::SessionConflict(
                    "memory clear request ID was already used for a different decision".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let mut agent = Self::load_actor(&mut tx, agent_id).await?;
        let state = decision(&agent)?;
        agent.memory_cleared(state, now)?;
        Self::save_actor(&mut tx, &agent).await?;
        let receipt = ClearMemoryReceipt {
            session_id: session,
            agent_id,
            revision: request.expected_revision + 1,
            generation: request.expected_generation + 1,
            request,
            cleared_at: now,
            replayed: false,
        };
        sqlx::query("INSERT INTO harness_memory_clears(request_id,session_id,agent_id,payload) VALUES(?,?,?,?)")
            .bind(receipt.request.request_id.to_string()).bind(session.to_string()).bind(agent_id.to_string())
            .bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}
