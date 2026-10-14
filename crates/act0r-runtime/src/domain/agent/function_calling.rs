//! Pure projection of one saved action schema; execution remains owned by Run.
use super::ModelRequest;
use crate::domain::DomainError;
use serde_json::{Value, json};

pub struct ModelFunction {
    pub name: String,
    pub parameters: Value,
}

impl ModelRequest {
    pub fn function_instructions(&self) -> String {
        let mut instructions = self.instructions.clone();
        for phrase in [
            "Return only JSON matching the supplied schema.",
            "Return only JSON with an action matching the supplied schema.",
            "Return only JSON with one action matching the supplied schema.",
            "Return one JSON object with an action field per response.",
            "Return one action matching this request's schema.",
        ] {
            instructions = instructions.replace(phrase, "");
        }
        instructions.push_str("\nRequest exactly one supplied function. Its name selects the action kind; provide only its parameter fields as a JSON object. Saved observations contain committed outcomes from earlier actions. The runtime executes the selected action and obtains approvals. Choose finish only for completed work, or finish_incomplete to explain an actual blocker. Use the function-call response channel for every decision.");
        instructions
    }

    pub fn function_actions(&self) -> Result<Vec<ModelFunction>, DomainError> {
        let saved = &self.schema["properties"]["action"];
        let variants = saved["anyOf"]
            .as_array()
            .map_or_else(|| vec![saved], |v| v.iter().collect());
        let invalid = || {
            DomainError::InvalidConfiguration(
            "function calling requires named action objects; native tools require flat contract 4".into(),
        )
        };
        let mut functions: Vec<ModelFunction> = vec![];
        for variant in variants {
            let kinds = variant["properties"]["kind"]["enum"]
                .as_array()
                .ok_or_else(invalid)?;
            let name = kinds.first().and_then(Value::as_str).ok_or_else(invalid)?;
            if kinds.len() != 1
                || name == "call_tool"
                || name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || variant["type"] != "object"
                || variant["additionalProperties"] != false
            {
                return Err(invalid());
            }
            let mut parameters = variant.clone();
            parameters["properties"]
                .as_object_mut()
                .ok_or_else(invalid)?
                .remove("kind");
            parameters["required"]
                .as_array_mut()
                .ok_or_else(invalid)?
                .retain(|v| v != "kind");
            if let Some(existing) = functions.iter_mut().find(|f| f.name == name) {
                // Scoped delegation adds an optional writable_paths field. Keep the
                // final saved alternatives authoritative when parsing the reply.
                let fields = parameters["properties"].as_object().ok_or_else(invalid)?;
                for (key, value) in fields {
                    let properties = existing.parameters["properties"].as_object_mut().unwrap();
                    if properties.get(key).is_some_and(|old| old != value) {
                        return Err(invalid());
                    }
                    properties.insert(key.clone(), value.clone());
                }
                let required = parameters["required"].as_array().unwrap();
                existing.parameters["required"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|key| required.contains(key));
            } else {
                functions.push(ModelFunction {
                    name: name.into(),
                    parameters,
                });
            }
        }
        if functions.is_empty() {
            return Err(invalid());
        }
        Ok(functions)
    }

    pub fn accepts_action_fields(
        &self,
        kind: &str,
        fields: &serde_json::Map<String, Value>,
    ) -> bool {
        let saved = &self.schema["properties"]["action"];
        let variants = saved["anyOf"]
            .as_array()
            .map_or_else(|| vec![saved], |v| v.iter().collect());
        variants.iter().any(|variant| {
            variant["properties"]["kind"]["enum"][0] == kind
                && variant["properties"]
                    .as_object()
                    .is_some_and(|properties| fields.keys().all(|k| properties.contains_key(k)))
                && variant["required"].as_array().is_some_and(|required| {
                    required.iter().all(|key| {
                        key.as_str()
                            .is_some_and(|k| k == "kind" || fields.contains_key(k))
                    })
                })
        })
    }
}

/// Reconstruct the original model-facing envelope without granting new actions.
pub fn function_action_text(
    request: &ModelRequest,
    name: &str,
    arguments: Value,
) -> Result<String, String> {
    let mut fields = arguments
        .as_object()
        .cloned()
        .ok_or("function arguments must be one JSON object")?;
    if fields.contains_key("kind") || !request.accepts_action_fields(name, &fields) {
        return Err("function kind or fields differ from the saved schema".into());
    }
    fields.insert("kind".into(), json!(name));
    Ok(json!({"action":fields}).to_string())
}
