use actix_web::{
    error::ResponseError,
    http::{header::ContentType, StatusCode},
    web::Data,
    HttpResponse,
};
use get_message::{message_getter_client::MessageGetterClient, MsgGetRequest, MsgReply};
use log::{error, info};
use log_message::{
    message_logger_client::MessageLoggerClient, GetAllReply, MsgAllGetRequest, MsgPostRequest,
};
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
    ClientConfig,
};
use std::{env, sync::Mutex, time::Duration};
use thiserror::Error;
use tonic::{transport::Channel, Status as GrpcStatus};
use uuid::Uuid;

pub mod get_message {
    tonic::include_proto!("get_message");
}

pub mod log_message {
    tonic::include_proto!("log_message");
}

#[derive(Debug, Error)]
pub enum UserError {
    #[error("At least one of the required services is down\nPlease standby\n")]
    ServiceConnectionError,

    #[error("An error inside the facade service occured\nPlease standby\n")]
    ServiceInternalError,

    #[error("A grpc error occured: {message}\nPlease standby\n", message=.0.message())]
    GrpcError(GrpcStatus),

    #[error("A kafka error occured\nPlease standby\n")]
    KafkaError(KafkaError),
}

impl ResponseError for UserError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::html())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            UserError::ServiceConnectionError => StatusCode::SERVICE_UNAVAILABLE,
            UserError::ServiceInternalError => StatusCode::INTERNAL_SERVER_ERROR,
            UserError::GrpcError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            UserError::KafkaError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub struct ServiceClients {
    messaging_client: Option<MessageGetterClient<Channel>>,
    logging_client: Option<MessageLoggerClient<Channel>>,
    kafka_client: FutureProducer,
}

impl ServiceClients {
    pub fn new() -> ServiceClients {
        let kafka_client: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "kafka-service:9092")
            .set("enable.idempotence", "true")
            .set("transactional.id", "facade")
            .create()
            .expect("Producer creation error");
        loop {
            match kafka_client.init_transactions(Timeout::After(Duration::from_secs(10))) {
                Ok(_) => break,
                Err(err) => error!("Could not init transactions due to: {}", err.to_string()),
            }
        }
        info!("Connected kafka producer...");

        ServiceClients {
            messaging_client: None,
            logging_client: None,
            kafka_client,
        }
    }

    pub async fn connect_logging(
        &mut self,
    ) -> Result<&mut MessageLoggerClient<Channel>, UserError> {
        if let None = self.logging_client {
            self.logging_client = Some(
                MessageLoggerClient::connect(
                    "http://nginx-logging-proxy-service:".to_owned()
                        + &env::var("NGINX_LOGGING_SERVICE_PORT")
                            .map_err(|_| UserError::ServiceConnectionError)?,
                )
                .await
                .map_err(|err| {
                    error!("{err}");
                    UserError::ServiceConnectionError
                })?,
            );
        }
        self.logging_client
            .as_mut()
            .ok_or(UserError::ServiceInternalError {})
    }

    pub async fn connect_messaging(
        &mut self,
    ) -> Result<&mut MessageGetterClient<Channel>, UserError> {
        if let None = self.messaging_client {
            self.messaging_client = Some(
                MessageGetterClient::connect(
                    "http://nginx-messages-proxy-service:".to_owned()
                        + &env::var("NGINX_MESSAGES_SERVICE_PORT")
                            .map_err(|_| UserError::ServiceConnectionError)?,
                )
                .await
                .map_err(|err| {
                    error!("{err}");
                    UserError::ServiceConnectionError
                })?,
            );
        }
        self.messaging_client
            .as_mut()
            .ok_or(UserError::ServiceInternalError {})
    }
}

pub async fn receive_random_messages(
    data: Data<Mutex<ServiceClients>>,
) -> Result<MsgReply, UserError> {
    let request = tonic::Request::new(MsgGetRequest {});
    let response = data
        .lock()
        .unwrap()
        .connect_messaging()
        .await?
        .get_message(request)
        .await
        .map_err(|err| UserError::GrpcError(err))?;
    Ok(response.into_inner())
}

pub async fn receive_logged_messages(
    data: Data<Mutex<ServiceClients>>,
) -> Result<GetAllReply, UserError> {
    let request = tonic::Request::new(MsgAllGetRequest {});
    let response = data
        .lock()
        .unwrap()
        .connect_logging()
        .await?
        .get_all_messages(request)
        .await
        .map_err(|err| UserError::GrpcError(err))?;
    Ok(response.into_inner())
}

pub async fn log_message(
    data: Data<Mutex<ServiceClients>>,
    post_request: MsgPostRequest,
) -> Result<(), UserError> {
    let request = tonic::Request::new(post_request);
    data.lock()
        .unwrap()
        .connect_logging()
        .await?
        .log_message(request)
        .await
        .map_err(|err| UserError::GrpcError(err))?;
    Ok(())
}

pub async fn send_message(
    data: Data<Mutex<ServiceClients>>,
    msg: &String,
    id: &Uuid,
) -> Result<(), UserError> {
    let clients = data.lock().unwrap();

    clients
        .kafka_client
        .begin_transaction()
        .map_err(|err| UserError::KafkaError(err))?;

    clients
        .kafka_client
        .send(
            FutureRecord::to("messages")
                .payload(msg.as_str())
                .key(id.to_string().as_str()),
            Duration::from_secs(0),
        )
        .await
        .map_err(|(err, _)| {
            error!("{}", err.to_string());
            match clients
                .kafka_client
                .abort_transaction(Timeout::After(Duration::from_secs(10)))
            {
                Ok(_) => UserError::KafkaError(err),
                Err(err) => {
                    error!("{}", err.to_string());
                    UserError::KafkaError(err)
                }
            }
        })?;

    clients
        .kafka_client
        .commit_transaction(Timeout::After(Duration::from_secs(10)))
        .map_err(|err| UserError::KafkaError(err))?;

    Ok(())
}
