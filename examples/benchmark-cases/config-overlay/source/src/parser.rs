use crate::Config;

pub fn parse(input: &str) -> Result<Config, String> {
    let mut config = Config::new();
    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<_> = line.split('=').collect();
        if parts.len() != 2 {
            return Err("expected key=value".into());
        }
        let key = parts[0].trim();
        let value = parts[1].trim();
        if key.is_empty() {
            return Err("empty key".into());
        }
        config.insert(key.into(), value.into());
    }
    Ok(config)
}
