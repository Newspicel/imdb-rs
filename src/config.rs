use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Application configuration driven by environment variables.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub index_dir: PathBuf,
    pub bind_addr: SocketAddr,
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let data_dir = env::var("IMDB_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data"));

        let index_dir = env::var("IMDB_INDEX_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("tantivy_index"));

        let bind_addr: SocketAddr = env::var("IMDB_BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse()?;

        Ok(Self {
            data_dir,
            index_dir,
            bind_addr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_applied_when_env_missing() {
        let prev_data = env::var("IMDB_DATA_DIR").ok();
        let prev_index = env::var("IMDB_INDEX_DIR").ok();
        let prev_bind = env::var("IMDB_BIND_ADDR").ok();

        // Mutating process environment is unsafe in Rust 2024 because it affects global state.
        unsafe {
            env::remove_var("IMDB_DATA_DIR");
            env::remove_var("IMDB_INDEX_DIR");
            env::remove_var("IMDB_BIND_ADDR");
        }

        let config = AppConfig::from_env().expect("config should load");
        assert_eq!(config.data_dir, PathBuf::from("data"));
        assert_eq!(config.index_dir, PathBuf::from("data/tantivy_index"));
        assert_eq!(config.bind_addr, "127.0.0.1:3000".parse().unwrap());

        // Restore any previous environment to avoid leaking state across tests.
        unsafe {
            if let Some(value) = prev_data {
                env::set_var("IMDB_DATA_DIR", value);
            } else {
                env::remove_var("IMDB_DATA_DIR");
            }
            if let Some(value) = prev_index {
                env::set_var("IMDB_INDEX_DIR", value);
            } else {
                env::remove_var("IMDB_INDEX_DIR");
            }
            if let Some(value) = prev_bind {
                env::set_var("IMDB_BIND_ADDR", value);
            } else {
                env::remove_var("IMDB_BIND_ADDR");
            }
        }
    }
}
