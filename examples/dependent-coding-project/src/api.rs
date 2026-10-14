pub struct User<'a> {
    pub first: &'a str,
    pub last: &'a str,
}

pub fn display_name(user: &User<'_>) -> String {
    user.first.trim().to_owned()
}
