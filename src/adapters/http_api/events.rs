use super::{errors::*, *};
use crate::{
    application::TeamEventPage,
    domain::{MessageId, TeamId, timeline::*},
};
use axum::{
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::stream;
use serde::Deserialize;
use serde_json::{Value, json};
use std::convert::Infallible;
use tokio::sync::OwnedSemaphorePermit;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct EventParams {
    after: Option<EventCursor>,
    limit: u32,
    max_bytes: u32,
}
impl Default for EventParams {
    fn default() -> Self {
        let d = EventQuery::default();
        Self {
            after: d.after,
            limit: d.limit,
            max_bytes: d.max_bytes,
        }
    }
}
impl EventParams {
    fn query(self) -> EventQuery {
        EventQuery {
            after: self.after,
            limit: self.limit,
            max_bytes: self.max_bytes,
        }
    }
}
pub(super) async fn read(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
    ApiQuery(params): ApiQuery<EventParams>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .events()?
            .read(team, task, params.query())
            .await?
    )))
}
pub(super) async fn stream(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
    ApiQuery(params): ApiQuery<EventParams>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut query = params.query();
    let values = headers.get_all("last-event-id");
    if values.iter().count() > 1 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "Last-Event-ID must appear at most once",
        ));
    }
    if let Some(value) = values.iter().next() {
        query.after = Some(
            value
                .to_str()
                .map_err(|_| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_cursor",
                        "Last-Event-ID must be a versioned cursor",
                    )
                })?
                .parse()
                .map_err(|e: crate::domain::DomainError| {
                    ApiError::new(StatusCode::BAD_REQUEST, "invalid_cursor", e.to_string())
                })?,
        );
    }
    let permit = state.streams.clone().try_acquire_owned().map_err(|_| {
        ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "stream_limit",
            "API event stream capacity is exhausted",
        )
    })?;
    // Validate ownership, cursor and page limits before committing HTTP 200 headers.
    let first = state
        .agents
        .teams()?
        .events()?
        .read(team, task, query)
        .await?;
    let feed = Feed {
        stopping: state.stopping.clone(),
        state,
        scope: (team, task),
        query,
        first: Some(first),
        error: None,
        finished: false,
        wait: false,
        _permit: permit,
    };
    let feed = stream::unfold(feed, |mut feed| async move {
        let item = feed.next().await?;
        Some((Ok::<_, Infallible>(item), feed))
    });
    Ok(Sse::new(feed)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response())
}
struct Feed {
    state: Arc<ApiState>,
    scope: (TeamId, MessageId),
    query: EventQuery,
    first: Option<TeamEventPage>,
    error: Option<ApiError>,
    finished: bool,
    wait: bool,
    stopping: watch::Receiver<bool>,
    _permit: OwnedSemaphorePermit,
}
impl Feed {
    fn error_event(&mut self, error: ApiError) -> Option<Event> {
        self.finished = true;
        // Error frames intentionally have no ID: only a complete page advances a client cursor.
        Some(
            Event::default()
                .event("api_error")
                .json_data(error.body())
                .expect("serializable API problem"),
        )
    }
    async fn next(&mut self) -> Option<Event> {
        if self.finished || *self.stopping.borrow() {
            return None;
        }
        if let Some(error) = self.error.take() {
            return self.error_event(error);
        }
        loop {
            if self.wait {
                tokio::select! {
                    biased;
                    _=self.stopping.changed() => return None,
                    _=tokio::time::sleep(self.state.options.event_poll_interval) => {}
                }
            }
            let initial = self.first.is_some();
            let result = if let Some(first) = self.first.take() {
                Ok(first)
            } else {
                let read = async {
                    self.state
                        .agents
                        .teams()?
                        .events()?
                        .read(self.scope.0, self.scope.1, self.query)
                        .await
                };
                tokio::select! {
                    biased;
                    _=self.stopping.changed() => return None,
                    result=read => result,
                }
            };
            let page = match result {
                Ok(page) => page,
                Err(error) => return self.error_event(error.into()),
            };
            self.query.after = Some(page.next_cursor);
            self.wait = !page.has_more;
            if let Some(oversized) = &page.oversized_event {
                self.error=Some(ApiError::new(StatusCode::PAYLOAD_TOO_LARGE,"event_too_large","increase max_bytes and reconnect from the last complete page cursor")
                    .details(json!({"event_id":oversized.event_id,"position":oversized.position,"required_bytes":oversized.required_bytes,"max_bytes":MAX_EVENT_PAGE_BYTES,"next_cursor":page.next_cursor})));
            }
            if initial || !page.events.is_empty() || page.oversized_event.is_some() {
                return match Event::default()
                    .event("page")
                    .id(page.next_cursor.to_string())
                    .json_data(&page)
                {
                    Ok(event) => Some(event),
                    Err(error) => {
                        tracing::error!(%error,"HTTP event serialization failed");
                        self.error_event(ApiError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "event serialization failed",
                        ))
                    }
                };
            }
        }
    }
}
