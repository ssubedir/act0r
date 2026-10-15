//! Pure validation and model projection of self-contained external tool contracts.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalToolContract {
    /// Opaque identity selected by the host; changing a binding invalidates saved pins.
    pub binding: String,
    pub read_only: bool,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}
impl ExternalToolContract {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.binding.is_empty()
            || self.binding.len() > 256
            || self.input_schema["type"] != "object"
            || self.input_schema.get("properties").is_some_and(|p| {
                p.as_object()
                    .is_none_or(|p| p.len() > 32 || p.contains_key("kind"))
            })
        {
            return Err(DomainError::InvalidConfiguration("external tools require a bounded binding and an object input schema with declared parameters; kind is reserved".into()));
        }
        for schema in std::iter::once(&self.input_schema).chain(self.output_schema.iter()) {
            validate_schema(schema)?;
        }
        Ok(())
    }
    /// Strict model projections represent absent optional fields with null. Remove
    /// only those nulls which the original schema does not accept before validation/call.
    pub fn arguments(&self, arguments: &Value) -> Value {
        let mut arguments = arguments.clone();
        normalize(&self.input_schema, &mut arguments);
        arguments
    }
    pub fn validate_arguments(&self, arguments: &Value) -> Result<(), DomainError> {
        let arguments = self.arguments(arguments);
        let object = arguments.as_object().ok_or_else(|| {
            DomainError::InvalidWorkflow("tool arguments must be an object".into())
        })?;
        if object
            .keys()
            .any(|name| self.input_schema["properties"].get(name).is_none())
            || !validator(&self.input_schema)?.is_valid(&arguments)
        {
            return Err(DomainError::InvalidWorkflow(
                "tool arguments differ from the pinned external schema".into(),
            ));
        }
        Ok(())
    }
    pub fn validate_output(&self, output: Option<&Value>) -> Result<(), DomainError> {
        if let Some(schema) = &self.output_schema {
            let validator = validator(schema)?;
            if output.is_none_or(|output| !validator.is_valid(output)) {
                return Err(DomainError::InvalidWorkflow(
                    "external tool result differs from its pinned output schema".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn model_schema(&self) -> Value {
        let mut schema = self.input_schema.clone();
        project(&mut schema);
        schema
    }
}
fn validator(schema: &Value) -> Result<jsonschema::Validator, DomainError> {
    // Cargo disables all reference retrieval. Reject references before compilation too.
    jsonschema::validator_for(schema).map_err(|_| {
        DomainError::InvalidConfiguration("invalid self-contained external tool JSON schema".into())
    })
}
fn validate_schema(schema: &Value) -> Result<(), DomainError> {
    fn bounded(value: &Value, depth: usize) -> bool {
        if depth > 32 {
            return false;
        }
        match value {
            Value::Object(object) => {
                !object.contains_key("$ref")
                    && !object.contains_key("$dynamicRef")
                    && !object.contains_key("patternProperties")
                    && object.values().all(|v| bounded(v, depth + 1))
            }
            Value::Array(array) => array.iter().all(|v| bounded(v, depth + 1)),
            _ => true,
        }
    }
    if schema.to_string().len() > 32_768 || !bounded(schema, 0) {
        return Err(DomainError::InvalidConfiguration("external tool schemas must fit 32 KiB and depth 32; references and pattern properties are not supported".into()));
    }
    validator(schema).map(|_| ())
}
fn normalize(schema: &Value, value: &mut Value) {
    if let (Some(properties), Some(object)) =
        (schema["properties"].as_object(), value.as_object_mut())
    {
        let required = schema["required"].as_array();
        object.retain(|name, value| {
            let Some(property) = properties.get(name) else {
                return true;
            };
            if value.is_null()
                && !required.is_some_and(|r| r.contains(&json!(name)))
                && validator(property).is_ok_and(|v| !v.is_valid(&Value::Null))
            {
                return false;
            }
            normalize(property, value);
            true
        });
    } else if let Some(array) = value.as_array_mut()
        && let Some(items) = schema.get("items")
    {
        for item in array {
            normalize(items, item);
        }
    }
}
fn project(schema: &mut Value) {
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    if object.get("type").is_some_and(|t| t == "object") && !object.contains_key("properties") {
        object.insert("properties".into(), json!({}));
    }
    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        // Clone required before borrowing properties below.
        let names: Vec<_> = properties.keys().cloned().collect();
        let required = object
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let properties = object
            .get_mut("properties")
            .unwrap()
            .as_object_mut()
            .unwrap();
        for (name, property) in properties {
            project(property);
            if !required.contains(&json!(name)) {
                *property = json!({"anyOf":[property.clone(),{"type":"null"}]});
            }
        }
        object.insert("required".into(), json!(names));
        object.insert("additionalProperties".into(), json!(false));
    }
    if let Some(items) = object.get_mut("items") {
        project(items);
    }
}
