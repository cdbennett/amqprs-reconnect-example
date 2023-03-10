//! # amqprs test message sender
//!
//! Pass the RabbitMQ connection parameters in environment variables like this:
//!
//!     RUST_LOG=trace RABBIT_USER=guest RABBIT_PASSWORD=guest RABBIT_HOST=localhost RABBIT_VHOST="/" cargo run
//!
//! The variables have sensible defaults, so you can omit some of them, depending on your server's setup.

use std::{env, sync::Arc};
use thiserror::Error;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{BasicAckArguments, BasicPublishArguments, Channel},
    connection::{Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use anyhow::Context;
use async_trait::async_trait;
use tokio::time::{self, sleep, Duration};
use tracing::{debug, error, info, warn};

/// Main program entry point. Use a structure that reflects real-world programs,
/// e.g. multiple concurrent tasks, sharing some global state. Support for graceful
/// shutdown, and for reconnecting when connection to a server is lost.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Arc::new(load_config().await);

    tokio::select!(
        result = rabbit_manager(config.clone()) => {
            tracing::error!("rabbit exited: {result:?}");
        },
        result = shutdown_monitor(config.clone()) => {
            tracing::warn!("shutdown command received: {result:?}");
        }
        // We would start other tasks here too, e.g. HTTP server
    );

    Ok(())
}

/// Load the application configuration.
/// Uses environment variable, but in reality it might use some other external configuration source.
async fn load_config() -> Config {
    sleep(Duration::from_millis(10)).await; // delay to simulate loading configuration
    Config {
        virtual_host: env::var("RABBIT_VHOST").unwrap_or("/".to_owned()),
        host: env::var("RABBIT_HOST").unwrap_or("localhost".to_owned()),
        password: env::var("RABBIT_PASSWORD").unwrap_or("guest".to_owned()),
        port: env::var("RABBIT_PORT")
            .map(|s| s.parse::<u16>().expect("can't parse RABBIT_PORT"))
            .unwrap_or(5672),
        username: env::var("RABBIT_USER").unwrap_or("guest".to_owned()),
    }
}

async fn shutdown_monitor(cfg: Arc<Config>) -> anyhow::Result<String> {
    // Show how tasks can share access to application config, though obviously we don't need config here right now.
    info!(
        "waiting for Ctrl+C.  I have access to the configuration. Rabbit host: {}",
        cfg.host
    );
    tokio::signal::ctrl_c()
        .await
        .context("problem waiting for Ctrl+C")?;
    info!("Received Ctrl+C signal");

    // Spawn a task that will immediately abort if a second Ctrl+C is received while shutting down.
    tokio::task::spawn(async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                warn!("Aren't you in a hurry?!");
            }
            Err(err) => {
                error!("problem waiting for Ctrl+C 2nd time: {err:?}");
            }
        };
        warn!("aborting process due to 2nd Ctrl+C");
        std::process::abort();
    });

    Ok("Ctrl+C".to_owned())
}

/// Application configuration data.
pub struct Config {
    pub virtual_host: String,
    pub host: String,
    pub password: String,
    pub port: u16,
    pub username: String,
}

#[derive(Error, Debug)]
pub enum RabbitError {
    #[error("RabbitMQ server connection lost: {0}")]
    ConnectionLost(String),
}

/// This function is the long-running RabbitMQ task.
/// It starts the RabbitMQ client, and if it fails, it will attempt to reconnect.
pub async fn rabbit_manager(cfg: Arc<Config>) -> anyhow::Result<()> {
    loop {
        let result = rabbit_connection_process(cfg.clone()).await;
        match result {
            Ok(value) => {
                // Not actually implemented right now.
                warn!("exiting in response to a shutdown command");
                return Ok(value);
            }
            Err(err) => {
                error!("RabbitMQ connection returned error: {err:?}");
                sleep(Duration::from_millis(1000)).await;
                info!("ready to restart RabbitMQ task");
            }
        }
    }
}

/// RabbitMQ client task. Returns an error result if the connection is lost.
async fn rabbit_connection_process(cfg: Arc<Config>) -> anyhow::Result<()> {
    debug!("starting RabbitMQ task");

    let connection = Connection::open(
        &OpenConnectionArguments::new(&cfg.host, cfg.port, &cfg.username, &cfg.password)
            .virtual_host(&cfg.virtual_host),
    )
    .await
    .with_context(|| {
        format!(
            "can't connect to RabbitMQ server at {}:{}",
            cfg.host, cfg.port
        )
    })?;

    // Add simple connection callback, it just logs diagnostics.
    connection
        .register_callback(DefaultConnectionCallback)
        .await
        .context("registering connection callback failed")?;

    let channel = connection
        .open_channel(None)
        .await
        .context("opening channel failed")?;
    channel
        .register_callback(DefaultChannelCallback)
        .await
        .context("registering channel callback failed")?;

    let exchange_name = "amq.topic";

    // create arguments for basic_publish
    let args = BasicPublishArguments::new(exchange_name, "");

    channel
        .basic_publish(
            BasicProperties::default(),
            "Hello, World!".as_bytes().to_vec(),
            args,
        )
        .await
        .unwrap();

    // keep the `channel` and `connection` object from dropping before pub/sub is done.
    // channel/connection will be closed when drop.
    time::sleep(time::Duration::from_secs(1)).await;
    // explicitly close
    channel.close().await.unwrap();
    connection.close().await.unwrap();

    Ok(())

    // if connection.listen_network_io_failure().await {
    //     Err(RabbitError::ConnectionLost("connection failure".to_owned()).into())
    // } else {
    //     Err(RabbitError::ConnectionLost("connection shut down normally. Since we don't close it ourselves, this shouldn't happen in this program".to_owned()).into())
    // }
}

pub struct MyConsumer {
    no_ack: bool,
    panic_countdown: u32,
}

impl MyConsumer {
    /// Return a new consumer.
    ///
    /// See [Acknowledgement Modes](https://www.rabbitmq.com/consumers.html#acknowledgement-modes)
    ///
    /// no_ack = [`true`] means automatic ack and should NOT send ACK to server.
    ///
    /// no_ack = [`false`] means manual ack, and should send ACK message to server.
    pub fn new(no_ack: bool) -> Self {
        Self {
            no_ack,
            panic_countdown: 3,
        }
    }
}

#[async_trait]
impl AsyncConsumer for MyConsumer {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        info!(
            "consume delivery {} on channel {}, content size: {}",
            deliver,
            channel,
            content.len()
        );

        match self.panic_countdown {
            0 => {
                self.panic_countdown = 3;
                info!("panic time!");
                panic!("testing consumer handling of panics");
            }
            i => {
                info!("panic countdown: {i}");
                self.panic_countdown -= 1;
            }
        };

        // Ack explicitly if using manual ack mode. Otherwise, the library auto-acks it.
        if !self.no_ack {
            info!("ack to delivery {} on channel {}", deliver, channel);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
            channel.basic_ack(args).await.unwrap();
        }
    }
}
