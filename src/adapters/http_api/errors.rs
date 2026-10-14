use super::*;
use crate::adapters::operator::Problem;
use axum::extract::{FromRequestParts, Path, Query, rejection::JsonRejection};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

pub(super) struct ApiError {
    pub status: StatusCode,
    pub problem: Problem,
}
impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            problem: Problem {
                code,
                message: message.into(),
                status: status.as_u16(),
                details: None,
            },
        }
    }
    pub fn details(mut self, details: Value) -> Self {
        self.problem.details = Some(details);
        self
    }
    pub fn body(&self) -> Value {
        json!({"error":self.problem})
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}
impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        let problem = Problem::from(error);
        Self {
            status: StatusCode::from_u16(problem.status).expect("operator status"),
            problem,
        }
    }
}
pub(super) type ApiResult<T> = Result<T, ApiError>;
pub(super) fn body<T>(body: Result<Json<T>, JsonRejection>) -> ApiResult<T> {
    body.map(|Json(value)| value).map_err(|e| {
        let code = match e.status() {
            StatusCode::PAYLOAD_TOO_LARGE => "body_too_large",
            StatusCode::UNSUPPORTED_MEDIA_TYPE => "json_required",
            _ => "invalid_json",
        };
        ApiError::new(e.status(), code, e.body_text())
    })
}
pub(super) struct ApiPath<T>(pub T);
impl<S: Send + Sync, T: DeserializeOwned + Send> FromRequestParts<S> for ApiPath<T> {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|e| ApiError::new(e.status(), "invalid_path", e.body_text()))
    }
}
pub(super) struct ApiQuery<T>(pub T);
impl<S: Send + Sync, T: DeserializeOwned + Send> FromRequestParts<S> for ApiQuery<T> {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(|e| ApiError::new(e.status(), "invalid_query", e.body_text()))
    }
}
