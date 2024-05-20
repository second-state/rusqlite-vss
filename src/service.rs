use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};

use rusqlite::OptionalExtension;
use tokio::sync::Mutex;

use crate::store;

#[derive(Debug, serde::Serialize)]
pub struct APIResult<T> {
    pub result: T,
    pub status: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct CreateConllections {
    pub vectors: CreateConllectionsVectors,
}

#[derive(Debug, serde::Deserialize)]
pub struct CreateConllectionsVectors {
    pub size: usize,
}

pub type CreateConllectionsResult = APIResult<bool>;

pub async fn create_collections(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
    Json(create_conllections): Json<CreateConllections>,
) -> impl IntoResponse {
    let conn = db.lock().await;
    if let Err(e) = store::create_collections(&conn, &name, create_conllections.vectors.size) {
        log::error!("Failed to create collection: {}", e);
        return (
            axum::http::StatusCode::CONFLICT,
            Json(CreateConllectionsResult {
                result: false,
                status: None,
                error: Some(e.to_string()),
            }),
        );
    } else {
        return (
            axum::http::StatusCode::OK,
            Json(CreateConllectionsResult {
                result: true,
                status: Some("ok".to_string()),
                error: None,
            }),
        );
    }
}

#[derive(Debug, serde::Serialize)]
pub struct CollectionsInfo {
    pub points_count: u64,
}

pub type GetCollectionsResult = APIResult<CollectionsInfo>;

pub async fn get_collections_info(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
) -> impl IntoResponse {
    let conn = db.lock().await;
    match store::get_collections_info(&conn, &name) {
        Ok(info) => (
            axum::http::StatusCode::OK,
            Json(GetCollectionsResult {
                result: CollectionsInfo {
                    points_count: info.points_count,
                },
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Err(e) => {
            log::error!("Failed to get collection info: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetCollectionsResult {
                    result: CollectionsInfo { points_count: 0 },
                    status: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct AddPoints {
    pub points: Vec<Point>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Point {
    pub id: u64,
    pub vector: Vec<f32>,
    pub payload: Option<serde_json::Map<String, serde_json::Value>>,
}

pub type AddPointsResult = APIResult<Option<Vec<u64>>>;

pub async fn add_points(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
    Json(points): Json<AddPoints>,
) -> impl IntoResponse {
    {
        let conn = db.lock().await;
        match store::add_point(&conn, &name, &points.points) {
            Ok(success_id) => (
                axum::http::StatusCode::OK,
                Json(AddPointsResult {
                    result: Some(success_id),
                    status: Some("ok".to_string()),
                    error: None,
                }),
            ),
            Err(e) => {
                log::error!("Failed to add points: {}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(AddPointsResult {
                        result: None,
                        status: None,
                        error: Some(e.to_string()),
                    }),
                )
            }
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct GetPoints {
    ids: Vec<u64>,
}

pub type GetPointsResult = APIResult<Option<Vec<Point>>>;

pub async fn get_points(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
    Json(ids): Json<GetPoints>,
) -> impl IntoResponse {
    let r = {
        let conn = db.lock().await;
        store::get_points(&conn, &name, ids.ids).optional()
    };

    match r {
        Ok(Some(points)) => (
            axum::http::StatusCode::OK,
            Json(GetPointsResult {
                result: Some(points),
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Ok(None) => (
            axum::http::StatusCode::OK,
            Json(GetPointsResult {
                result: Some(Vec::new()),
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(GetPointsResult {
                result: None,
                status: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

pub type GetPointResult = APIResult<Option<Point>>;

pub async fn get_point(
    Path((name, point_id)): Path<(String, u64)>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
) -> impl IntoResponse {
    let conn: tokio::sync::MutexGuard<rusqlite::Connection> = db.lock().await;
    let r = store::get_point(&conn, &name, point_id).optional();
    match r {
        Ok(Some(point)) => (
            axum::http::StatusCode::OK,
            Json(GetPointResult {
                result: Some(point),
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Ok(None) => (
            axum::http::StatusCode::NOT_FOUND,
            Json(GetPointResult {
                result: None,
                status: None,
                error: Some(format!(
                    "Not found: Point with id {} does not exists",
                    point_id
                )),
            }),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(GetPointResult {
                result: None,
                status: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Search {
    pub vector: Vec<f32>,
    pub limit: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct ScoredPoint {
    pub id: u64,
    pub vector: Vec<f32>,
    pub payload: Option<serde_json::Map<String, serde_json::Value>>,
    pub score: f32,
}

pub type SearchResult = APIResult<Option<Vec<ScoredPoint>>>;

pub async fn search_points(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
    Json(search): Json<Search>,
) -> impl IntoResponse {
    let conn = db.lock().await;
    let r = store::search_points(&conn, &name, search.vector.as_slice(), search.limit).optional();
    match r {
        Ok(Some(points)) => (
            axum::http::StatusCode::OK,
            Json(SearchResult {
                result: Some(points),
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Ok(None) => (
            axum::http::StatusCode::OK,
            Json(SearchResult {
                result: Some(Vec::new()),
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(SearchResult {
                result: None,
                status: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct DeletePoints {
    pub points: Vec<u64>,
}

pub type DeletePointsResult = APIResult<bool>;

pub async fn delete_points(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
    Json(points): Json<DeletePoints>,
) -> impl IntoResponse {
    let conn = db.lock().await;
    match store::delete_points(&conn, &name, points.points) {
        Ok(_) => (
            axum::http::StatusCode::OK,
            Json(DeletePointsResult {
                result: true,
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(DeletePointsResult {
                result: false,
                status: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

pub async fn delete_collection(
    Path(name): Path<String>,
    State(db): State<Arc<Mutex<rusqlite::Connection>>>,
) -> impl IntoResponse {
    let conn = db.lock().await;
    match store::delete_collection(&conn, &name) {
        Ok(_) => (
            axum::http::StatusCode::OK,
            Json(DeletePointsResult {
                result: true,
                status: Some("ok".to_string()),
                error: None,
            }),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(DeletePointsResult {
                result: false,
                status: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}
