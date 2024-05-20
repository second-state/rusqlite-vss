use std::sync::Arc;

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use tokio::sync::Mutex;

pub mod service;
pub mod store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    store::init();

    let addr = std::env::var("LISTEN_ADDR").unwrap_or("0.0.0.0:3000".to_string());

    let db = store::open("store.sqlite")?;

    let app = Router::new()
        .route("/collections/:name", put(service::create_collections))
        .route("/collections/:name", get(service::get_collections_info))
        .route("/collections/:name", delete(service::delete_collection))
        .route("/collections/:name/points", put(service::add_points))
        .route(
            "/collections/:name/points/delete",
            post(service::delete_points),
        )
        .route(
            "/collections/:name/points/:point_id",
            get(service::get_point),
        )
        .route("/collections/:name/points", post(service::get_points))
        .with_state(Arc::new(Mutex::new(db)));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
