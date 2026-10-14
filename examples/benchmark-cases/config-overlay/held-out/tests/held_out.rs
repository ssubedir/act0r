use bench_config_overlay::{overlay, parse};

#[test]
fn held_out_empty_and_equals_rich_values() {
    let config = parse("expression==a==b=\n blank = \n#comment\n").unwrap();
    assert_eq!(config["expression"], "=a==b=");
    assert_eq!(config["blank"], "");
    assert!(parse("").unwrap().is_empty());
}

#[test]
fn held_out_unicode_keys_and_literal_comments_inside_values() {
    let config = parse("名 = 猫=犬\ncolor=#blue\n").unwrap();
    assert_eq!(config["名"], "猫=犬");
    assert_eq!(config["color"], "#blue");
}

#[test]
fn held_out_overlay_empty_values_and_empty_maps() {
    let base = parse("a=default\nb=retained").unwrap();
    let merged = overlay(&base, parse("a=\nc=new").unwrap());
    assert_eq!(merged["a"], "");
    assert_eq!(merged["b"], "retained");
    assert_eq!(merged["c"], "new");
    assert_eq!(overlay(&base, parse("").unwrap()), base);
    assert_eq!(overlay(&parse("").unwrap(), merged.clone()), merged);
}
