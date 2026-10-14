//! Bounded queries and responses for already committed native tool results.
use super::{ActivityId, DefinitionId, RunId, tools::ToolError};
use serde::{Deserialize, Serialize};

pub const MAX_RESULT_ENTRIES: u32 = 16;
pub const DEFAULT_RESULT_ENTRIES: u32 = 8;
pub const MAX_RESULT_BYTES: u32 = 4096;
pub const DEFAULT_RESULT_BYTES: u32 = 2048;

/// Supplied by execution, never selected by model arguments.
#[derive(Clone, Copy, Debug)]
pub struct RecallScope {
    pub run_id: RunId,
    pub activity_id: ActivityId,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListResultsQuery {
    #[serde(default)]
    pub after: Option<ActivityId>,
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}
impl ListResultsQuery {
    pub fn page_size(&self) -> Result<u32, ToolError> {
        let limit = self.limit.unwrap_or(DEFAULT_RESULT_ENTRIES);
        if !(1..=MAX_RESULT_ENTRIES).contains(&limit)
            || self.tool.as_ref().is_some_and(|name| {
                name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
        {
            return Err(ToolError::new(
                "result_input",
                "list_results requires a limit of 1..16 and an optional tool name of at most 64 characters",
            ));
        }
        Ok(limit)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadResultQuery {
    pub activity_id: ActivityId,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u32>,
}
impl ReadResultQuery {
    pub fn bounds(&self) -> Result<(u64, u32), ToolError> {
        let offset = self.offset.unwrap_or(0);
        let bytes = self.max_bytes.unwrap_or(DEFAULT_RESULT_BYTES);
        if offset > i64::MAX as u64 || !(4..=MAX_RESULT_BYTES).contains(&bytes) {
            return Err(ToolError::new(
                "result_input",
                "read_result requires a nonnegative byte offset and max_bytes of 4..4096",
            ));
        }
        Ok((offset, bytes))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultMetadata {
    pub activity_id: ActivityId,
    pub tool: DefinitionId,
    pub result_bytes: u64,
    /// The original result's success flag, if it supplied a boolean.
    pub ok: Option<bool>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultPage {
    pub results: Vec<ResultMetadata>,
    pub next_after: Option<ActivityId>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultChunk {
    pub source: ResultMetadata,
    pub offset: u64,
    pub next_offset: Option<u64>,
    /// True only when this page contains the entire serialized JSON result.
    pub complete: bool,
    pub text: String,
}
impl ResultChunk {
    /// The adapter supplies at most max_bytes raw bytes starting at offset.
    /// An incomplete final Unicode character is deferred to the next page.
    pub fn from_bytes(
        source: ResultMetadata,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, ToolError> {
        if bytes.len() > MAX_RESULT_BYTES as usize {
            return Err(ToolError::new(
                "result_input",
                "saved result pages contain at most 4096 raw bytes",
            ));
        }
        if offset > source.result_bytes {
            return Err(ToolError::new(
                "result_offset",
                "offset exceeds the saved result",
            ));
        }
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(error) if error.error_len().is_none() && error.valid_up_to() > 0 => {
                std::str::from_utf8(&bytes[..error.valid_up_to()]).expect("validated UTF-8 prefix")
            }
            Err(_) => {
                return Err(ToolError::new(
                    "result_offset",
                    "offset must be a UTF-8 boundary; use the returned next_offset",
                ));
            }
        }
        .to_owned();
        let end = offset
            .checked_add(text.len() as u64)
            .ok_or_else(|| ToolError::new("result_offset", "offset is too large"))?;
        if end > source.result_bytes || (end == offset && end < source.result_bytes) {
            return Err(ToolError::new(
                "result_offset",
                "saved result page made no valid progress",
            ));
        }
        Ok(Self {
            next_offset: (end < source.result_bytes).then_some(end),
            complete: offset == 0 && end == source.result_bytes,
            source,
            offset,
            text,
        })
    }
}
