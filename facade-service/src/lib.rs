use actix_web::{
    error::ResponseError,
    http::{header::ContentType, StatusCode},
    web::Data,
    HttpResponse,
};
use get_message::{message_getter_client::MessageGetterClient, MsgGetRequest, MsgReply};
use log::error;
use log_message::{
    message_logger_client::MessageLoggerClient, GetAllReply, MsgAllGetRequest, MsgPostRequest,
};
use std::{env, sync::Mutex};
use thiserror::Error;
use tonic::{transport::Channel, Status as GrpcStatus};

pub mod get_message {
    tonic::include_proto!("get_message");
}

pub mod log_message {
    tonic::include_proto!("log_message");
}

#[derive(Debug, Error)]
pub enum UserError {
    #[error("At least one of the required services is down\nPlease standby")]
    ServiceConnectionError,

    #[error("An error inside the facade service occured\nPlease standby")]
    ServiceInternalError,

    #[error("A grpc error occured: {message}\nPlease standby", message=.0.message())]
    GrpcError(GrpcStatus),
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
        }
    }
}

pub struct ServiceClients {
    messaging_client: Option<MessageGetterClient<Channel>>,
    logging_client: Option<MessageLoggerClient<Channel>>,
}

impl ServiceClients {
    pub fn new() -> ServiceClients {
        ServiceClients {
            messaging_client: None,
            logging_client: None,
        }
    }

    // TODO: refactor with macros in the future
    pub async fn connect_logging(
        &mut self,
    ) -> Result<&mut MessageLoggerClient<Channel>, UserError> {
        if let None = self.logging_client {
            self.logging_client = Some(
                MessageLoggerClient::connect(
                    "http://logging-service:".to_owned()
                        + &env::var("LOGGING_SERVICE_PORT")
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
                    "http://messages-service:".to_owned()
                        + &env::var("MESSAGES_SERVICE_PORT")
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

pub async fn receive_static_message(
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
