use crate::{
    application::{Error, ports::*},
    domain::{result_recall::*, tools::*},
};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

pub fn result_recall_tools(reader: Arc<dyn ResultRecallReadPort>) -> Vec<Arc<dyn NativeTool>> {
    vec![
        Arc::new(ListResultsTool::new(reader.clone())),
        Arc::new(ReadResultTool::new(reader)),
    ]
}
pub struct ListResultsTool {
    reader: Arc<dyn ResultRecallReadPort>,
}
pub struct ReadResultTool {
    reader: Arc<dyn ResultRecallReadPort>,
}
impl ListResultsTool {
    pub fn new(reader: Arc<dyn ResultRecallReadPort>) -> Self {
        Self { reader }
    }
}
impl ReadResultTool {
    pub fn new(reader: Arc<dyn ResultRecallReadPort>) -> Self {
        Self { reader }
    }
}
fn argument(value_type: ToolValueType, description: &str, required: bool) -> ToolParameter {
    ToolParameter {
        value_type,
        description: description.into(),
        required,
    }
}
fn definition(
    name: &str,
    description: &str,
    parameters: BTreeMap<String, ToolParameter>,
) -> ToolDefinition {
    let mut definition = super::native_tools::definition(name, description, parameters);
    definition.reports_errors = true;
    // 4 KiB raw JSON pages may expand sixfold when encoded as a JSON string.
    definition.max_output_bytes = 32768;
    definition
}
impl NativeTool for ListResultsTool {
    fn definition(&self) -> ToolDefinition {
        let mut definition = definition(
            "list_results",
            "List earlier completed native tool results from this Run in scheduling order. Returns stable activity IDs, tool IDs, original ok flags and JSON byte lengths, without contents. Model responses and recall results are excluded. Use next_after to paginate; no original tool is rerun.",
            BTreeMap::from([
                (
                    "after".into(),
                    argument(
                        ToolValueType::String,
                        "Previous page's next_after activity ID; omitted/null starts at the beginning.",
                        false,
                    ),
                ),
                (
                    "tool".into(),
                    argument(
                        ToolValueType::String,
                        "Optional exact tool name filter; omitted/null lists all eligible results.",
                        false,
                    ),
                ),
                (
                    "limit".into(),
                    argument(
                        ToolValueType::Integer,
                        "1..16 results per page; omitted/null defaults to 8.",
                        false,
                    ),
                ),
            ]),
        );
        definition.max_output_bytes = 8192;
        definition
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let query: ListResultsQuery = match serde_json::from_value(call.arguments) {
                Ok(query) => query,
                Err(_) => {
                    return Ok(NativeToolResult::Rejected(ToolError::new(
                        "result_input",
                        "list_results needs a valid optional activity cursor, tool name, and integer limit",
                    )));
                }
            };
            if let Err(error) = query.page_size() {
                return Ok(NativeToolResult::Rejected(error));
            }
            let scope = RecallScope {
                run_id: call.run_id,
                activity_id: call.activity_id,
            };
            match self.reader.list_results(scope, query).await {
                Ok(page) => Ok(NativeToolResult::Success(
                    json!({"ok":true,"results":page.results,"next_after":page.next_after}),
                )),
                Err(Error::ToolRejected(error)) => Ok(NativeToolResult::Rejected(error)),
                Err(error) => Err(error),
            }
        })
    }
}
impl NativeTool for ReadResultTool {
    fn definition(&self) -> ToolDefinition {
        definition(
            "read_result",
            "Read an earlier committed native tool result in this Run without rerunning it. text is a UTF-8 page of the original serialized JSON. source identifies the result; complete is true only for the whole JSON. Follow next_offset until null and concatenate text in order. Historical evidence may be stale.",
            BTreeMap::from([
                (
                    "activity_id".into(),
                    argument(
                        ToolValueType::String,
                        "A source activity ID from list_results or saved test_status.",
                        true,
                    ),
                ),
                (
                    "offset".into(),
                    argument(
                        ToolValueType::Integer,
                        "UTF-8 byte offset from next_offset; omitted/null starts at zero.",
                        false,
                    ),
                ),
                (
                    "max_bytes".into(),
                    argument(
                        ToolValueType::Integer,
                        "4..4096 raw JSON bytes per page; omitted/null defaults to 2048. Choose a size that fits your context.",
                        false,
                    ),
                ),
            ]),
        )
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let query: ReadResultQuery = match serde_json::from_value(call.arguments) {
                Ok(query) => query,
                Err(_) => {
                    return Ok(NativeToolResult::Rejected(ToolError::new(
                        "result_input",
                        "read_result needs an activity ID, nonnegative offset, and integer max_bytes",
                    )));
                }
            };
            if let Err(error) = query.bounds() {
                return Ok(NativeToolResult::Rejected(error));
            }
            let scope = RecallScope {
                run_id: call.run_id,
                activity_id: call.activity_id,
            };
            match self.reader.read_result(scope, query).await {
                Ok(page) => Ok(NativeToolResult::Success(
                    json!({"ok":true,"format":"json","source":page.source,"offset":page.offset,"next_offset":page.next_offset,"complete":page.complete,"text":page.text}),
                )),
                Err(Error::ToolRejected(error)) => Ok(NativeToolResult::Rejected(error)),
                Err(error) => Err(error),
            }
        })
    }
}
