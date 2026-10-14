use std::collections::BTreeMap;

pub type Config = BTreeMap<String, String>;

pub mod merge;
pub mod parser;

pub use merge::overlay;
pub use parser::parse;
