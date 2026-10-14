use super::*;
use crate::domain::{result_recall::*, tools::ToolError};
use sqlx::{QueryBuilder, Sqlite, sqlite::SqliteRow};

fn unavailable() -> Error {
    ToolError::new(
        "result_unavailable",
        "no eligible saved result or cursor exists in this Run before the recall intent",
    )
    .into()
}
const METADATA: &str = "a.id, json_extract(a.payload,'$.spec.input.definition.id.name') AS tool_name, json_extract(a.payload,'$.spec.input.definition.id.version') AS tool_version, length(CAST(a.payload -> '$.result' AS BLOB)) AS result_bytes, CASE json_type(a.payload,'$.result.ok') WHEN 'true' THEN 1 WHEN 'false' THEN 0 END AS original_ok";

/// The history boundary excludes results completed after the recall was scheduled,
/// even if a custom workflow queued several activities before this one.
async fn boundary(
    conn: &mut SqliteConnection,
    scope: RecallScope,
    tool: &str,
) -> Result<i64, Error> {
    let sequence: Option<i64> = sqlx::query_scalar("SELECT h.sequence FROM history h JOIN activities a ON a.run_id=h.run_id AND a.id=? WHERE h.run_id=? AND json_extract(h.payload,'$.activity_id')=a.id AND json_extract(h.payload,'$.kind')='activity_scheduled' AND json_extract(a.payload,'$.spec.handler.name')='native.tool' AND json_extract(a.payload,'$.spec.handler.version')=3 AND json_extract(a.payload,'$.spec.input.definition.id.name')=? AND json_extract(a.payload,'$.spec.input.definition.id.version')=1")
        .bind(scope.activity_id.to_string()).bind(scope.run_id.to_string()).bind(tool)
        .fetch_optional(conn).await.map_err(storage)?;
    sequence.ok_or_else(|| {
        Error::Unavailable(
            "recall requires its saved native activity in the execution-supplied Run".into(),
        )
    })
}
fn eligible(select: &str, scope: RecallScope, sequence: i64) -> QueryBuilder<Sqlite> {
    let mut query = QueryBuilder::new(format!("SELECT {select} FROM activities a WHERE a.run_id="));
    query.push_bind(scope.run_id.to_string());
    query.push(" AND a.status='succeeded' AND json_extract(a.payload,'$.spec.handler.name')='native.tool' AND json_extract(a.payload,'$.spec.handler.version') BETWEEN 1 AND 5 AND json_extract(a.payload,'$.spec.input.definition.id.name') NOT IN ('list_results','read_result') AND EXISTS (SELECT 1 FROM history d WHERE d.run_id=a.run_id AND d.sequence<");
    query.push_bind(sequence);
    query.push(" AND json_extract(d.payload,'$.activity_id')=a.id AND ((json_extract(d.payload,'$.kind')='attempt_finished' AND json_extract(d.payload,'$.payload.outcome.kind') IN ('success','success_with_usage')) OR (json_extract(d.payload,'$.kind')='activity_reconciled' AND json_extract(d.payload,'$.payload.request.decision.action')='accept')))");
    query
}
fn metadata(row: &SqliteRow) -> Result<ResultMetadata, Error> {
    let id: String = row.try_get("id").map_err(storage)?;
    let version: i64 = row.try_get("tool_version").map_err(storage)?;
    let bytes: i64 = row.try_get("result_bytes").map_err(storage)?;
    Ok(ResultMetadata {
        activity_id: id
            .parse()
            .map_err(|_| Error::Storage("invalid saved result identity".into()))?,
        tool: DefinitionId::new(
            row.try_get::<String, _>("tool_name").map_err(storage)?,
            u32::try_from(version)
                .map_err(|_| Error::Storage("invalid saved result tool version".into()))?,
        ),
        result_bytes: u64::try_from(bytes)
            .map_err(|_| Error::Storage("invalid saved result byte count".into()))?,
        ok: row
            .try_get::<Option<i64>, _>("original_ok")
            .map_err(storage)?
            .map(|v| v != 0),
    })
}
impl ResultRecallReadPort for SqliteStore {
    fn list_results(
        &self,
        scope: RecallScope,
        request: ListResultsQuery,
    ) -> PortFuture<'_, ResultPage> {
        Box::pin(async move {
            let limit = request.page_size()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let sequence = boundary(&mut tx, scope, "list_results").await?;
            let cursor = if let Some(id) = request.after {
                let mut query =
                    eligible("a.scheduling_revision,a.command_ordinal", scope, sequence);
                query.push(" AND a.id=").push_bind(id.to_string());
                let row = query
                    .build()
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?
                    .ok_or_else(unavailable)?;
                Some((
                    row.try_get::<i64, _>("scheduling_revision")
                        .map_err(storage)?,
                    row.try_get::<i64, _>("command_ordinal").map_err(storage)?,
                ))
            } else {
                None
            };
            let mut query = eligible(METADATA, scope, sequence);
            if let Some((revision, ordinal)) = cursor {
                query
                    .push(" AND (a.scheduling_revision,a.command_ordinal)>(")
                    .push_bind(revision)
                    .push(",")
                    .push_bind(ordinal)
                    .push(")");
            }
            if let Some(tool) = request.tool {
                query
                    .push(" AND json_extract(a.payload,'$.spec.input.definition.id.name')=")
                    .push_bind(tool);
            }
            query
                .push(" ORDER BY a.scheduling_revision,a.command_ordinal LIMIT ")
                .push_bind(i64::from(limit) + 1);
            let rows = query.build().fetch_all(&mut *tx).await.map_err(storage)?;
            let has_more = rows.len() > limit as usize;
            let results: Vec<_> = rows
                .iter()
                .take(limit as usize)
                .map(metadata)
                .collect::<Result<_, _>>()?;
            let next_after =
                has_more.then(|| results.last().expect("positive page limit").activity_id);
            tx.commit().await.map_err(storage)?;
            Ok(ResultPage {
                results,
                next_after,
            })
        })
    }
    fn read_result(
        &self,
        scope: RecallScope,
        request: ReadResultQuery,
    ) -> PortFuture<'_, ResultChunk> {
        Box::pin(async move {
            let (offset, bytes) = request.bounds()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let sequence = boundary(&mut tx, scope, "read_result").await?;
            // Return only one bounded BLOB slice, not the whole activity, input or outcome history.
            let start = i64::try_from(offset)
                .map_err(|_| ToolError::new("result_offset", "offset is too large"))?
                .checked_add(1)
                .ok_or_else(|| ToolError::new("result_offset", "offset is too large"))?;
            // These interpolation values are checked integers, never model text.
            let mut guarded = eligible(
                &format!(
                    "{METADATA}, substr(CAST(a.payload -> '$.result' AS BLOB),{start},{bytes}) AS chunk"
                ),
                scope,
                sequence,
            );
            guarded
                .push(" AND a.id=")
                .push_bind(request.activity_id.to_string());
            let row = guarded
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?
                .ok_or_else(unavailable)?;
            let source = metadata(&row)?;
            let chunk: Vec<u8> = row.try_get("chunk").map_err(storage)?;
            let result = ResultChunk::from_bytes(source, offset, chunk)?;
            tx.commit().await.map_err(storage)?;
            Ok(result)
        })
    }
}
