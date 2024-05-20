use std::{collections::HashMap, mem::size_of};

use rusqlite::{ffi::sqlite3_auto_extension, params, Connection};
use sqlite_vss::{sqlite3_vector_init, sqlite3_vss_init};

use crate::service::{CollectionsInfo, Point, ScoredPoint};

pub fn init() {
    unsafe {
        sqlite3_auto_extension(Some(sqlite3_vector_init));
        sqlite3_auto_extension(Some(sqlite3_vss_init));
    }
}

pub fn open(path: &str) -> rusqlite::Result<Connection> {
    rusqlite::Connection::open(path)
}

pub fn create_collections(conn: &Connection, name: &str, size: usize) -> rusqlite::Result<()> {
    let sql = format!(
        r#"
        BEGIN;
        CREATE VIRTUAL TABLE IF NOT EXISTS {} USING vss0(point({}));
        CREATE TABLE IF NOT EXISTS {}_payload (rowid INTEGER PRIMARY KEY, payload TEXT);
        COMMIT;
        "#,
        name, size, name
    );
    conn.execute_batch(sql.as_str())
}

pub fn get_collections_info(conn: &Connection, name: &str) -> rusqlite::Result<CollectionsInfo> {
    let sql = format!(
        r#"
        SELECT COUNT(*) FROM {};
        "#,
        name
    );
    let mut stmt = conn.prepare(sql.as_str())?;
    let count: u64 = stmt.query_row([], |row| row.get(0)).unwrap();
    Ok(CollectionsInfo {
        points_count: count,
    })
}

#[test]
fn test_collections() {
    init();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    create_collections(&conn, "test_vss", 4).unwrap();
    let r = get_collections_info(&conn, "test_vss").unwrap();
    assert_eq!(r.points_count, 0);
}

fn blob_to_vector(blob: &[u8]) -> Vec<f32> {
    unsafe {
        std::slice::from_raw_parts(blob.as_ptr() as *const f32, blob.len() / size_of::<f32>())
            .to_vec()
    }
}

fn vector_to_blob(vector: &[f32]) -> Vec<u8> {
    unsafe {
        std::slice::from_raw_parts(
            vector.as_ptr() as *const u8,
            vector.len() * size_of::<f32>(),
        )
        .to_vec()
    }
}

pub fn add_point(conn: &Connection, name: &str, points: &[Point]) -> rusqlite::Result<Vec<u64>> {
    let mut vector_stmt = conn.prepare(&format!(
        "INSERT INTO {}(rowid,point) VALUES (?1, vector_from_raw(?2))",
        name
    ))?;

    let mut payload_stmt = conn.prepare(&format!(
        "INSERT OR REPLACE INTO {}_payload(rowid,payload) VALUES (?1, ?2)",
        name
    ))?;

    let mut success_id = vec![];

    for point in points {
        let raw = vector_to_blob(&point.vector);
        vector_stmt.execute(params![point.id as i64, raw])?;

        let payload = serde_json::to_string(&point.payload).unwrap();
        payload_stmt.execute(params![point.id as i64, payload])?;

        success_id.push(point.id);
    }

    Ok(success_id)
}

pub fn get_points(
    conn: &rusqlite::Connection,
    name: &str,
    ids: Vec<u64>,
) -> rusqlite::Result<Vec<Point>> {
    let ids = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<String>>()
        .join(",");

    let point_sql = format!(
        r#"
        SELECT rowid,vector_to_raw(point) FROM {} WHERE rowid in ({});
        "#,
        name, ids
    );

    let payload_sql = format!(
        r#"
        SELECT * FROM {}_payload WHERE rowid in ({});
        "#,
        name, ids
    );

    let mut point_stmt = conn.prepare(point_sql.as_str()).unwrap();
    let mut payload_stmt = conn.prepare(payload_sql.as_str()).unwrap();

    let mut map = HashMap::new();

    let vector_r = point_stmt.query_map(params![], |row| {
        let id: u64 = row.get(0)?;
        let vector_raw: Vec<u8> = row.get(1)?;
        let vector: Vec<f32> = blob_to_vector(&vector_raw);
        Ok((id, vector))
    })?;

    let payload_r = payload_stmt.query_map(params![], |row| {
        let id: u64 = row.get(0)?;
        let payload_str: String = row.get(1)?;
        let payload: Option<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_str(&payload_str).unwrap_or_default();
        Ok((id, payload))
    })?;

    for v in vector_r {
        if let Ok((id, vector)) = v {
            map.insert(
                id,
                Point {
                    id,
                    vector,
                    payload: None,
                },
            );
        }
    }

    for v in payload_r {
        if let Ok((id, payload)) = v {
            if let Some(point) = map.get_mut(&id) {
                point.payload = payload;
            }
        }
    }

    Ok(map.into_iter().map(|(_, v)| v).collect())
}

pub fn get_point(conn: &Connection, name: &str, id: u64) -> rusqlite::Result<Point> {
    let point_sql = format!(
        r#"
        SELECT rowid,vector_to_raw(point) FROM {} WHERE rowid = ?1;
        "#,
        name
    );

    let payload_sql = format!(
        r#"
        SELECT * FROM {}_payload WHERE rowid = ?1;
        "#,
        name
    );

    let mut point_stmt = conn.prepare(point_sql.as_str())?;
    let mut payload_stmt = conn.prepare(payload_sql.as_str())?;

    let vector = point_stmt.query_row(params![id], |row| {
        let vector_raw: Vec<u8> = row.get(1)?;
        let vector: Vec<f32> = blob_to_vector(&vector_raw);
        Ok(vector)
    })?;

    let payload = payload_stmt.query_row(params![id], |row| {
        let payload_str: String = row.get(1)?;
        let payload: Option<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_str(&payload_str).unwrap_or_default();
        Ok(payload)
    })?;

    Ok(Point {
        id,
        vector,
        payload,
    })
}

#[test]
fn test_points_base() {
    use serde_json::json;
    init();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    create_collections(&conn, "test_vss", 4).unwrap();
    let mut points = Vec::<Point>::new();
    {
        points.push(Point {
            id: 1,
            vector: vec![0.05, 0.61, 0.76, 0.74],
            payload: json!({"city": "Berlin"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 2,
            vector: vec![0.19, 0.81, 0.75, 0.11],
            payload: json!({"city": "London"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 3,
            vector: vec![0.36, 0.55, 0.47, 0.94],
            payload: json!({"city": "Moscow"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 4,
            vector: vec![0.18, 0.01, 0.85, 0.80],
            payload: json!({"city": "New York"})
                .as_object()
                .map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 5,
            vector: vec![0.24, 0.18, 0.22, 0.44],
            payload: json!({"city": "Beijing"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 6,
            vector: vec![0.35, 0.08, 0.11, 0.44],
            payload: json!({"city": "Mumbai"}).as_object().map(|m| m.to_owned()),
        });
    }
    let r = add_point(&conn, "test_vss", &points).unwrap();
    assert_eq!(r, vec![1, 2, 3, 4, 5, 6]);

    let mut r = get_points(&conn, "test_vss", vec![1, 2, 3]).unwrap();
    assert_eq!(r.len(), 3);
    r.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(r[0].payload, points[0].payload);
    assert_eq!(r[1].payload, points[1].payload);
    assert_eq!(r[2].payload, points[2].payload);

    let r = get_point(&conn, "test_vss", 4).unwrap();
    assert_eq!(r.payload, points[3].payload);
}

pub fn search_points(
    conn: &Connection,
    name: &str,
    vector: &[f32],
    limit: usize,
) -> rusqlite::Result<Vec<ScoredPoint>> {
    let sql = format!(
        r#"
        SELECT rowid,vector_to_raw(point),distance FROM {} WHERE vss_search(point,vector_from_raw(?1)) ORDER BY distance LIMIT ?2;
        "#,
        name
    );

    let mut stmt = conn.prepare(sql.as_str())?;
    let vector_raw = vector_to_blob(&vector);
    let points = stmt.query_map(params![vector_raw, limit], |row| {
        let id: u64 = row.get(0)?;
        let vector_raw: Vec<u8> = row.get(1)?;
        let score: f32 = row.get(2)?;
        let vector: Vec<f32> = blob_to_vector(&vector_raw);
        Ok(ScoredPoint {
            id,
            vector,
            payload: None,
            score,
        })
    })?;

    let mut map = HashMap::new();
    for point in points {
        if let Ok(point) = point {
            map.insert(point.id, point);
        }
    }

    let ids = map
        .keys()
        .map(|id| id.to_string())
        .collect::<Vec<String>>()
        .join(",");

    let payload_sql = format!(
        r#"
            SELECT * FROM {}_payload WHERE rowid in ({});
            "#,
        name, ids
    );
    let mut payload_stmt = conn.prepare(payload_sql.as_str())?;
    let payload_r = payload_stmt.query_map(params![], |row| {
        let id: u64 = row.get(0)?;
        let payload_str: String = row.get(1)?;
        let payload: Option<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_str(&payload_str).unwrap_or_default();
        Ok((id, payload))
    })?;

    for v in payload_r {
        if let Ok((id, payload)) = v {
            if let Some(point) = map.get_mut(&id) {
                point.payload = payload;
            }
        }
    }

    Ok(map.into_iter().map(|(_, v)| v).collect())
}

#[test]
fn test_points_search() {
    use serde_json::json;
    init();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    create_collections(&conn, "test_vss", 4).unwrap();
    let mut points = Vec::<Point>::new();
    {
        points.push(Point {
            id: 1,
            vector: vec![0.05, 0.61, 0.76, 0.74],
            payload: json!({"city": "Berlin"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 2,
            vector: vec![0.19, 0.81, 0.75, 0.11],
            payload: json!({"city": "London"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 3,
            vector: vec![0.36, 0.55, 0.47, 0.94],
            payload: json!({"city": "Moscow"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 4,
            vector: vec![0.18, 0.01, 0.85, 0.80],
            payload: json!({"city": "New York"})
                .as_object()
                .map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 5,
            vector: vec![0.24, 0.18, 0.22, 0.44],
            payload: json!({"city": "Beijing"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 6,
            vector: vec![0.35, 0.08, 0.11, 0.44],
            payload: json!({"city": "Mumbai"}).as_object().map(|m| m.to_owned()),
        });
    }
    let r = add_point(&conn, "test_vss", &points).unwrap();
    assert_eq!(r, vec![1, 2, 3, 4, 5, 6]);

    let q = vec![0.2, 0.1, 0.9, 0.7];
    let r = search_points(&conn, "test_vss", &q, 2).unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].id, 4);
    assert_eq!(r[1].id, 1);
}

pub fn delete_points(conn: &Connection, name: &str, ids: Vec<u64>) -> rusqlite::Result<()> {
    let ids = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<String>>()
        .join(",");

    let sql = format!(
        r#"
        BEGIN;
        DELETE FROM {} WHERE rowid in ({});
        DELETE FROM {}_payload WHERE rowid in ({});
        COMMIT;
        "#,
        name, ids, name, ids
    );
    conn.execute_batch(sql.as_str())
}

#[test]
fn test_points_delete() {
    use serde_json::json;
    init();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    create_collections(&conn, "test_vss", 4).unwrap();
    let mut points = Vec::<Point>::new();
    {
        points.push(Point {
            id: 1,
            vector: vec![0.05, 0.61, 0.76, 0.74],
            payload: json!({"city": "Berlin"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 2,
            vector: vec![0.19, 0.81, 0.75, 0.11],
            payload: json!({"city": "London"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 3,
            vector: vec![0.36, 0.55, 0.47, 0.94],
            payload: json!({"city": "Moscow"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 4,
            vector: vec![0.18, 0.01, 0.85, 0.80],
            payload: json!({"city": "New York"})
                .as_object()
                .map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 5,
            vector: vec![0.24, 0.18, 0.22, 0.44],
            payload: json!({"city": "Beijing"}).as_object().map(|m| m.to_owned()),
        });
        points.push(Point {
            id: 6,
            vector: vec![0.35, 0.08, 0.11, 0.44],
            payload: json!({"city": "Mumbai"}).as_object().map(|m| m.to_owned()),
        });
    }
    let r = add_point(&conn, "test_vss", &points).unwrap();
    assert_eq!(r, vec![1, 2, 3, 4, 5, 6]);

    delete_points(&conn, "test_vss", vec![1, 2, 3, 4]).unwrap();

    let r = get_points(&conn, "test_vss", vec![1, 2, 3, 4]).unwrap();
    assert_eq!(r.len(), 0);
}

pub fn delete_collection(conn: &Connection, name: &str) -> rusqlite::Result<()> {
    let sql = format!(
        r#"
        BEGIN;
        DROP TABLE IF EXISTS {};
        DROP TABLE IF EXISTS {}_payload;
        COMMIT;
        "#,
        name, name
    );
    conn.execute_batch(sql.as_str())
}
