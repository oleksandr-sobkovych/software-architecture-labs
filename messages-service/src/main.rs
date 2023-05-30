use futures::stream::StreamExt;
use get_message::{
    message_getter_server::{MessageGetter, MessageGetterServer},
    MsgGetRequest, MsgReply,
};
use log::{error, info};
use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::BorrowedMessage,
    util::get_rdkafka_version,
    ClientConfig, ClientContext, Message, TopicPartitionList,
};
use std::thread::sleep;
use std::{env, net::ToSocketAddrs, process::exit};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

pub mod get_message {
    tonic::include_proto!("get_message");
}

#[derive(Debug)]
pub struct VecStoreGetter {
    vec_store: Arc<RwLock<Vec<String>>>,
}

impl VecStoreGetter {
    fn from(vec_store: Arc<RwLock<Vec<String>>>) -> VecStoreGetter {
        VecStoreGetter { vec_store }
    }
}

#[tonic::async_trait]
impl MessageGetter for VecStoreGetter {
    async fn get_message(
        &self,
        request: Request<MsgGetRequest>,
    ) -> Result<Response<MsgReply>, Status> {
        info!("Got a request: {request:#?}");
        Ok(Response::new(MsgReply {
            message: self.vec_store.read().await.join(" : ").to_string(),
        }))
    }
}

struct LoggingContext;

impl ClientContext for LoggingContext {}

impl ConsumerContext for LoggingContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

async fn record_message(
    consumer: &StreamConsumer<LoggingContext>,
    vec_store: Arc<RwLock<Vec<String>>>,
    msg: KafkaResult<BorrowedMessage<'_>>,
) {
    match msg {
        Err(e) => error!("Kafka error: {e}"),
        Ok(m) => {
            let payload = match m.payload_view::<str>() {
                None => "",
                Some(Ok(s)) => s,
                Some(Err(e)) => {
                    error!("Failed to deserialize {e:?}");
                    ""
                }
            }
            .to_string();

            info!("Received message: {payload}");
            vec_store.write().await.push(payload);

            consumer
                .commit_message(&m, CommitMode::Async)
                .expect("Kafka err");
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let vec_store = Arc::new(RwLock::new(Vec::<String>::new()));
    let writable_vec_store = Arc::clone(&vec_store);

    tokio::spawn(async move {
        loop {
            match ClientConfig::new()
                .set("bootstrap.servers", "kafka-service:9092")
                .set("group.id", Uuid::new_v4().to_string())
                .set("enable.auto.commit", "false")
                .set("isolation.level", "read_committed")
                .create_with_context::<LoggingContext, StreamConsumer<LoggingContext>>(
                    LoggingContext {},
                ) {
                Ok(consumer) => match consumer.subscribe(&["messages"]) {
                    Ok(_) => {
                        info!("Subscribed to 'messages' topic");
                        consumer
                            .stream()
                            .for_each(|msg| {
                                record_message(&consumer, Arc::clone(&writable_vec_store), msg)
                            })
                            .await;
                    }
                    Err(err) => info!(
                        "Can't subscribe to specified topics due to: {}",
                        err.to_string()
                    ),
                },
                Err(err) => info!("Consumer creation failed with: {}", err.to_string()),
            };

            // retry connection to Kafka instance
            sleep(Duration::from_secs(10));
        }
    });

    info!("Attempting connection...");
    let addr_source = "messages-service:".to_owned()
        + &env::var("MESSAGES_SERVICE_PORT").unwrap_or_else(|err| {
            error!("{err}");
            exit(exitcode::DATAERR);
        });
    let mut addrs = match addr_source.to_socket_addrs() {
        Ok(addr) => addr,
        Err(err) => {
            error!("{err}");
            exit(exitcode::DATAERR);
        }
    };
    let addr = match addrs.next() {
        Some(addr) => addr,
        None => {
            error!("Invalid address supplied to service");
            exit(exitcode::DATAERR);
        }
    };

    info!("Starting the service at {addr}");
    if let Err(err) = Server::builder()
        .add_service(MessageGetterServer::new(VecStoreGetter::from(vec_store)))
        .serve(addr)
        .await
    {
        error!("{err}");
        exit(exitcode::SOFTWARE);
    };
}
