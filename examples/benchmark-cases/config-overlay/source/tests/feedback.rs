use bench_config_overlay::{overlay, parse};

#[test]
fn parsing_preserves_equals_in_values() {
    let config = parse("name=actor\nexpression=x=y").unwrap();
    assert_eq!(config["name"], "actor");
    assert_eq!(config["expression"], "x=y");
}

#[test]
fn overrides_win_and_missing_defaults_survive() {
    let base = parse("mode=local\nworkers=1").unwrap();
    let override_config = parse("mode=remote\nqueue=durable").unwrap();
    let merged = overlay(&base, override_config);
    assert_eq!(merged["mode"], "remote");
    assert_eq!(merged["workers"], "1");
    assert_eq!(merged["queue"], "durable");
    assert_eq!(base["mode"], "local");
}

#[test]
fn comments_whitespace_duplicates_and_invalid_lines() {
    assert_eq!(
        parse("# ignored\n a = first \n a=last \n empty= \n").unwrap()["a"],
        "last"
    );
    assert!(parse("missing delimiter").is_err());
    assert!(parse(" = value").is_err());
}
