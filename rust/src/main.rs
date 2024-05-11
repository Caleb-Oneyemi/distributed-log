use std::sync::Mutex;

use actix_web::{web, App, HttpServer};

use crate::{models::Log, state::AppState};

mod models;
mod state;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let addr = "127.0.0.1:7777";

    println!("Starting Server on {}...", addr);

    let log = Log {
        records: Vec::new(),
    };

    let data = web::Data::new(AppState {
        log: Mutex::new(log),
    });

    let app = move || App::new().app_data(data.clone());

    HttpServer::new(app).bind(addr)?.run().await
}
