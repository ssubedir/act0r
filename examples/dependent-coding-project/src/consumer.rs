use crate::api::User;

pub fn render_greeting(user: &User<'_>) -> String {
    let parts: Vec<_> = [user.first.trim(), user.last.trim()]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect();
    let name = if parts.is_empty() {
        "Anonymous".to_owned()
    } else {
        parts.join(" ")
    };
    format!("Hello, {name}!")
}
