use dependent_coding_fixture::{
    api::{User, display_name},
    consumer::render_greeting,
};

#[test]
fn api_full_name() {
    assert_eq!(
        display_name(&User {
            first: "Ada",
            last: "Lovelace"
        }),
        "Ada Lovelace"
    );
}

#[test]
fn api_trims_and_skips_empty_parts() {
    assert_eq!(
        display_name(&User {
            first: "  Grace  ",
            last: " "
        }),
        "Grace"
    );
    assert_eq!(
        display_name(&User {
            first: " ",
            last: " Hopper "
        }),
        "Hopper"
    );
}

#[test]
fn api_unicode() {
    assert_eq!(
        display_name(&User {
            first: "李",
            last: "雷"
        }),
        "李 雷"
    );
}

#[test]
fn api_empty_name() {
    assert_eq!(
        display_name(&User {
            first: " ",
            last: ""
        }),
        "Anonymous"
    );
}

#[test]
fn consumer_full_name() {
    assert_eq!(
        render_greeting(&User {
            first: "Ada",
            last: "Lovelace"
        }),
        "Hello, Ada Lovelace!"
    );
}

#[test]
fn consumer_trims_and_skips_empty_parts() {
    assert_eq!(
        render_greeting(&User {
            first: " Grace ",
            last: " "
        }),
        "Hello, Grace!"
    );
    assert_eq!(
        render_greeting(&User {
            first: "",
            last: " Hopper "
        }),
        "Hello, Hopper!"
    );
}

#[test]
fn consumer_unicode() {
    assert_eq!(
        render_greeting(&User {
            first: "李",
            last: "雷"
        }),
        "Hello, 李 雷!"
    );
}

#[test]
fn consumer_empty_name() {
    assert_eq!(
        render_greeting(&User {
            first: " ",
            last: " "
        }),
        "Hello, Anonymous!"
    );
}
