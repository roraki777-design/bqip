//! Public-market capture only. Raw acknowledgement precedes every parser handoff.
pub mod capture;
pub mod config;
pub mod error;
pub mod http;
pub mod raw_writer;
pub mod realtime;
pub mod recovery;
pub mod runtime;
pub mod telemetry;
pub mod ws;
pub use error::{Error, Kind, Result};
