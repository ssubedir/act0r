use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use uuid::Uuid;

macro_rules! identity {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);
        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
        impl FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(s)?))
            }
        }
    };
}
identity!(RunId);
identity!(ActivityId);
identity!(AttemptId);
identity!(EventId);
identity!(EventStreamId);
identity!(TimerId);
identity!(WaitId);
identity!(TaskId);
identity!(BenchmarkId);
identity!(CheckpointId);
identity!(ForkId);
identity!(SearchId);
identity!(AgentId);
identity!(MessageId);
identity!(TaskGroupId);
identity!(TeamId);
identity!(SessionId);
identity!(ProjectProfileId);
