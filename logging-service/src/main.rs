use log::{error, info};
use log_message::{
    message_logger_server::{MessageLogger, MessageLoggerServer},
    GetAllReply, LogReply, MsgAllGetRequest, MsgPostRequest,
};
use std::{collections::HashMap, env, net::ToSocketAddrs, process::exit, str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

pub mod log_message {
    tonic::include_proto!("log_message");
}

#[derive(Debug, Default)]
pub struct HashMapLogger(Arc<RwLock<HashMap<Uuid, String>>>);

#[tonic::async_trait]
impl MessageLogger for HashMapLogger {
    async fn log_message(
        &self,
        request: Request<MsgPostRequest>,
    ) -> Result<Response<LogReply>, Status> {
        info!("Got a request: {request:#?}");

        let MsgPostRequest { uuid, message } = request.into_inner();
        self.0.write().await.insert(
            Uuid::from_str(&uuid).map_err(|_| Status::invalid_argument("invalid uuid"))?,
            message,
        );
        Ok(Response::new(LogReply {}))
    }

    async fn get_all_messages(
        &self,
        request: Request<MsgAllGetRequest>,
    ) -> Result<Response<GetAllReply>, Status> {
        info!("Got a request: {request:#?}");

        let messages: String = self.0.read().await.values().cloned().collect();
        Ok(Response::new(GetAllReply { messages }))
    }
}

// TODO: refactor with macros in the future
#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Attempting connection...");
    let addr_source = "logging-service:".to_owned()
        + &env::var("LOGGING_SERVICE_PORT").unwrap_or_else(|err| {
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
    let logger = HashMapLogger::default();

    info!("Starting the service at {addr}");
    if let Err(err) = Server::builder()
        .add_service(MessageLoggerServer::new(logger))
        .serve(addr)
        .await
    {
        error!("{err}");
        exit(exitcode::SOFTWARE);
    };
}
