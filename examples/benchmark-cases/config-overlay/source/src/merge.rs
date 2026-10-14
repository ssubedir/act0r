use crate::Config;

pub fn overlay(base: &Config, mut overrides: Config) -> Config {
    for (key, value) in base {
        overrides.insert(key.clone(), value.clone());
    }
    overrides
}
