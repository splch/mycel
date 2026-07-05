//! Wire protocol: the ALPN string IS the version; frames are u32-LE length +
//! JSON. JSON over a binary codec: zero new dependencies, additive evolution
//! via serde defaults, debuggable by eye; bulk bytes (shard payloads)
//! bypass the codec entirely as raw stream bytes.

use crate::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub const ALPN_QUERY: &[u8] = b"mycel/query/1";
pub const ALPN_SYNC: &[u8] = b"mycel/sync/1";
pub const MAX_FRAME_BYTES: u32 = 4 * 1024 * 1024;
pub const MAX_QUERY_BYTES: usize = 1024;
pub const MAX_RESULTS_PER_PEER: usize = 50;

/// QUIC application close codes (0 = normal close, implicit on drop).
pub const CLOSE_UNAUTHORIZED: u32 = 1;
pub const CLOSE_PROTOCOL: u32 = 2;

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryRequest {
    pub query: String,
    pub limit: u16,
    #[serde(default)]
    pub lang: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Reply<T> {
    Ok(T),
    Err(ErrorFrame),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorFrame {
    pub code: ErrCode,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrCode {
    BadRequest,
    NotFound,
    Busy,
    Internal,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryOk {
    pub hits: Vec<RemoteHit>,
}

/// No `source` on the wire: the requester stamps attribution from the
/// EndpointId it dialed, so provenance is unspoofable.
#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteHit {
    pub url: String,
    pub title: String,
    pub snippet: String,
    pub score: f32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SyncRequest {
    Catalog,
    Fetch { shard_id: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CatalogOk {
    pub shards: Vec<ShardMeta>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardMeta {
    pub shard_id: String,
    pub blake3: String,
    pub bytes: u64,
    pub doc_count: u32,
    pub created_at: i64,
    pub origin_node: String,
}

/// Fetch header; the shard bytes follow raw on the stream, then FIN.
#[derive(Serialize, Deserialize, Debug)]
pub struct FetchOk {
    pub bytes: u64,
    pub blake3: String,
}

pub async fn write_frame<T: Serialize>(s: &mut iroh::endpoint::SendStream, msg: &T) -> Result<()> {
    let payload = serde_json::to_vec(msg)?;
    if payload.len() as u32 > MAX_FRAME_BYTES {
        return Err("frame too large".into());
    }
    s.write_all(&(payload.len() as u32).to_le_bytes()).await?;
    s.write_all(&payload).await?;
    Ok(())
}

pub async fn read_frame<T: DeserializeOwned>(r: &mut iroh::endpoint::RecvStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf);
    if len == 0 || len > MAX_FRAME_BYTES {
        return Err(format!("bad frame length {len}").into());
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload).await?;
    Ok(serde_json::from_slice(&payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Codec invariants via the raw byte layout (streams need a live endpoint;
    /// framing logic is byte-level and testable directly).
    #[test]
    fn frame_layout_roundtrip() {
        let msg = QueryRequest {
            query: "hello".into(),
            limit: 10,
            lang: None,
        };
        let payload = serde_json::to_vec(&msg).unwrap();
        let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&payload);

        let len = u32::from_le_bytes(frame[..4].try_into().unwrap());
        assert!(len > 0 && len <= MAX_FRAME_BYTES);
        let back: QueryRequest = serde_json::from_slice(&frame[4..4 + len as usize]).unwrap();
        assert_eq!(back.query, "hello");
        assert_eq!(back.limit, 10);
    }

    #[test]
    fn additive_evolution_tolerated() {
        // Unknown fields are ignored; missing optional fields default.
        let with_extra = r#"{"query":"q","limit":5,"lang":null,"future_field":123}"#;
        let q: QueryRequest = serde_json::from_str(with_extra).unwrap();
        assert_eq!(q.limit, 5);
        let minimal = r#"{"query":"q","limit":5}"#;
        let q: QueryRequest = serde_json::from_str(minimal).unwrap();
        assert!(q.lang.is_none());
    }

    #[test]
    fn reply_encoding() {
        let ok: Reply<QueryOk> = Reply::Ok(QueryOk { hits: vec![] });
        let s = serde_json::to_string(&ok).unwrap();
        assert!(s.contains("\"ok\""));
        let err: Reply<QueryOk> = Reply::Err(ErrorFrame {
            code: ErrCode::BadRequest,
            message: "empty query".into(),
        });
        let s = serde_json::to_string(&err).unwrap();
        assert!(s.contains("bad_request"));
        let back: Reply<QueryOk> = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, Reply::Err(e) if e.code == ErrCode::BadRequest));
    }
}
