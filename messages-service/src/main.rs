use get_message::{
    message_getter_server::{MessageGetter, MessageGetterServer},
    MsgGetRequest, MsgReply,
};
use log::{error, info};
use std::{env, net::ToSocketAddrs, process::exit};
use tonic::{transport::Server, Request, Response, Status};

pub mod get_message {
    tonic::include_proto!("get_message");
}

#[derive(Debug, Default)]
pub struct PlainGetter {}

#[tonic::async_trait]
impl MessageGetter for PlainGetter {
    async fn get_message(
        &self,
        request: Request<MsgGetRequest>,
    ) -> Result<Response<MsgReply>, Status> {
        info!("Got a request: {request:#?}");
        Ok(Response::new(MsgReply {
            message: "not implemented yet".into(),
        }))
    }
}

// TODO: refactor with macros in the future
#[tokio::main]
async fn main() {
    env_logger::init();
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

    let greeter = PlainGetter::default();

    info!("Starting the service at {addr}");
    if let Err(err) = Server::builder()
        .add_service(MessageGetterServer::new(greeter))
        .serve(addr)
        .await
    {
        error!("{err}");
        exit(exitcode::SOFTWARE);
    };
}
