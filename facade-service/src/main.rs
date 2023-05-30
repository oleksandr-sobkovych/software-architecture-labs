use actix_web::{
    get, middleware::Logger, post, web::Data, App, HttpResponse, HttpServer, Responder,
};
use facade_service::{
    log_message, receive_logged_messages, receive_random_messages, send_message, ServiceClients,
    UserError,
};
use log::{error, info};
use log_message::MsgPostRequest;
use rdkafka::util::get_rdkafka_version;
use std::{env, process::exit, sync::Mutex};
use uuid::Uuid;

#[get("/")]
async fn get_messages(data: Data<Mutex<ServiceClients>>) -> Result<impl Responder, UserError> {
    let responce = receive_random_messages(data.clone()).await?;
    let logged_msg = receive_logged_messages(data).await?;
    Ok(HttpResponse::Ok().body(
        "Message sequence:\n".to_string()
            + responce.message.as_ref()
            + "\nLogged messages:\n"
            + logged_msg.messages.as_ref()
            + "\n",
    ))
}

#[post("/")]
async fn post_message(
    data: Data<Mutex<ServiceClients>>,
    msg: String,
) -> Result<impl Responder, UserError> {
    let msg_id = Uuid::new_v4();
    send_message(data.clone(), &msg, &msg_id).await?;
    log_message(
        data,
        MsgPostRequest {
            uuid: msg_id.to_string(),
            message: msg,
        },
    )
    .await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::main]
async fn main() {
    env_logger::init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let clients = Data::new(Mutex::new(ServiceClients::new()));

    let server = HttpServer::new(move || {
        let logger = Logger::default();

        App::new()
            .wrap(logger)
            .app_data(clients.clone())
            .service(get_messages)
            .service(post_message)
    })
    .bind(
        "0.0.0.0:".to_owned()
            + &env::var("FACADE_SERVICE_PORT").unwrap_or_else(|err| {
                error!("{err}");
                exit(exitcode::DATAERR);
            }),
    );
    match server {
        Ok(server) => {
            if let Err(err) = server.run().await {
                error!("{err}");
                exit(exitcode::SOFTWARE);
            }
        }
        Err(err) => {
            error!("{err}");
            exit(exitcode::DATAERR);
        }
    };
}
