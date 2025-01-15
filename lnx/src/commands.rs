use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Subcommand;
use tracing::info;
use url::Url;

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run the lnx SQL server
    Run {
        #[arg(
            short,
            long,
            env = "LNX_LISTEN_ADDRESS",
            default_value = "127.0.0.1:4202"
        )]
        /// The server bind address. {n}
        /// {n}
        /// * For local development this should be `127.0.0.1`. {n}
        /// * For external use this should be `0.0.0.0`.
        listen_address: SocketAddr,
        #[arg(long, env = "LNX_DATA_PATH", default_value = "/var/lib/lnx/data/")]
        /// The path where lnx should write all data to.
        ///
        /// NOTE: This does not include temporary files.
        data_path: PathBuf,
    },
    /// Run the lnx SQL shell
    Shell {
        #[arg(long, env = "LNX_HOST")]
        /// The host lnx server to connect to.
        ///
        /// This can be either `http` or `https` URL containing any
        /// necessary connection information.
        host: Url,
    },
}

impl Commands {
    /// Triggers any additional startup messages which are aware
    /// of the provided subcommand.
    pub fn display_startup_message(&self) {
        match self {
            Commands::Run {
                listen_address,
                data_path,
            } => {
                info!(listen_address = %listen_address, data_path = %data_path.display(), "Starting the lnx API server");

                if listen_address.ip().is_loopback() {
                    let extra = if listen_address.port() == 4202 {
                        "".to_string()
                    } else {
                        format!("?p={}", listen_address.port())
                    };

                    info!("You can access the local UI @ https://my.lnx.rs/dashboard{extra}")
                }
            },
            Commands::Shell { host } => {
                info!(host = %host, "Connecting to the lnx API server");
            },
        }
    }

    /// Executes the command
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Commands::Run {
                listen_address,
                data_path,
            } => lnx_server::run(listen_address, data_path).await?,
            Commands::Shell { .. } => {},
        }

        Ok(())
    }
}
