//! Arrow Flight 数据面（feature `flight`，默认关闭守二进制红线）。
//!
//! 独立 h2c 监听口（与 HTTP API 分离；`--flight-bind`）。协议面（v1）：
//! - handshake：接受（鉴权走 Bearer metadata，同 HTTP 约定，每次调用校验）
//! - do_get：ticket = SQL 文本 → FlightData 流（schema + IPC 批）
//! - get_flight_info：descriptor cmd = SQL → schema + 自 endpoint（复用连接）
//! - do_put：首帧 flight_descriptor.cmd = `table:<表名>` → 流解码为批 → append
//!   （复用核心库写入端：schema 校验 / 约束 / 宽松写）
//!
//! 查询/写入经 spawn_blocking 进同步核心库（同 HTTP 路径）。

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PollInfo, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use qsdb::Qsdb;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

pub struct QsdbFlightService {
    db: Arc<Qsdb>,
    token: Option<String>,
}

impl QsdbFlightService {
    pub fn new(db: Arc<Qsdb>, token: Option<String>) -> Self {
        Self { db, token }
    }

    /// Bearer 鉴权（token 配置时强制；同 HTTP API 约定）。
    fn check_auth(&self, meta: &MetadataMap) -> Result<(), Status> {
        let Some(want) = &self.token else {
            return Ok(());
        };
        let got = meta
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if got == want {
            Ok(())
        } else {
            Err(Status::unauthenticated("missing or invalid bearer token"))
        }
    }
}

type BoxStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for QsdbFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // 鉴权不在此握手（Bearer metadata 每调用校验）；空应答完成协议
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.check_auth(request.metadata())?;
        Err(Status::unimplemented("list_flights lands later"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_auth(request.metadata())?;
        let desc = request.into_inner();
        let sql = String::from_utf8(desc.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8 SQL"))?;
        let db = Arc::clone(&self.db);
        let sql_for_task = sql.clone();
        let (schema, rows) = tokio::task::spawn_blocking(move || {
            let ctx = db.new_query_context(None);
            match db.sql_with_context(&sql_for_task, &ctx)? {
                qsdb::sql::SqlResult::Query { schema, batches } => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    Ok((schema, rows))
                }
                qsdb::sql::SqlResult::Ok { message } => {
                    Err(qsdb::QsdbError::invalid(format!("not a query: {message}")))
                }
            }
        })
        .await
        .map_err(|e| Status::internal(format!("join: {e}")))?
        .map_err(|e| Status::invalid_argument(format!("query: {e}")))?;
        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: sql.clone().into_bytes().into(),
            }),
            location: vec![arrow_flight::Location {
                uri: "arrow-flight-reuse-connection://".to_string(),
            }],
            ..Default::default()
        };
        let opts = arrow::ipc::writer::IpcWriteOptions::default();
        let IpcMessage(schema_bytes) = SchemaAsIpc::new(&schema, &opts).try_into().map_err(
            |e: arrow::error::ArrowError| Status::internal(format!("schema encode: {e}")),
        )?;
        let info = FlightInfo {
            schema: schema_bytes,
            endpoint: vec![endpoint],
            total_records: rows as i64,
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.check_auth(request.metadata())?;
        let desc = request.into_inner();
        let sql = String::from_utf8(desc.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8 SQL"))?;
        let db = Arc::clone(&self.db);
        let schema = tokio::task::spawn_blocking(move || {
            let ctx = db.new_query_context(None);
            match db.sql_with_context(&sql, &ctx)? {
                qsdb::sql::SqlResult::Query { schema, .. } => Ok(schema),
                qsdb::sql::SqlResult::Ok { message } => {
                    Err(qsdb::QsdbError::invalid(format!("not a query: {message}")))
                }
            }
        })
        .await
        .map_err(|e| Status::internal(format!("join: {e}")))?
        .map_err(|e| Status::invalid_argument(format!("query: {e}")))?;
        let opts = arrow::ipc::writer::IpcWriteOptions::default();
        let result = SchemaAsIpc::new(&schema, &opts).try_into().map_err(
            |e: arrow::error::ArrowError| Status::internal(format!("schema encode: {e}")),
        )?;
        Ok(Response::new(result))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info lands later"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.check_auth(request.metadata())?;
        let sql = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket must be UTF-8 SQL"))?;
        let db = Arc::clone(&self.db);
        let batches = tokio::task::spawn_blocking(move || db.sql_collect(&sql))
            .await
            .map_err(|e| Status::internal(format!("join: {e}")))?
            .map_err(|e| Status::invalid_argument(format!("query: {e}")))?;
        if batches.is_empty() {
            return Err(Status::invalid_argument(
                "query returned no batches; project at least one column",
            ));
        }
        let schema = batches[0].schema();
        let s = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(
                batches
                    .into_iter()
                    .map(Ok::<_, arrow_flight::error::FlightError>),
            ))
            .map_err(|e| Status::internal(format!("encode: {e:?}")));
        Ok(Response::new(Box::pin(s)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.check_auth(request.metadata())?;
        let mut inbound = request.into_inner();
        // 首帧（schema）携带 flight_descriptor.cmd = "table:<表名>"
        let first = inbound
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty do_put stream"))?
            .map_err(|e| Status::internal(format!("stream: {e}")))?;
        let desc = first.flight_descriptor.clone().ok_or_else(|| {
            Status::invalid_argument("first frame needs flight_descriptor cmd = 'table:<name>'")
        })?;
        let table = String::from_utf8(desc.cmd.to_vec())
            .ok()
            .and_then(|s| s.strip_prefix("table:").map(str::to_string))
            .ok_or_else(|| Status::invalid_argument("cmd must be 'table:<name>'"))?;
        // 首帧 + 其余 → 解码为批
        let rest = inbound.try_collect::<Vec<_>>().await?;
        let frames =
            stream::iter(std::iter::once(Ok::<_, Status>(first)).chain(rest.into_iter().map(Ok)))
                .map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)));
        let mut decoder = FlightDataDecoder::new(frames);
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(decoded) = decoder
            .try_next()
            .await
            .map_err(|e| Status::internal(format!("decode: {e}")))?
        {
            if let DecodedPayload::RecordBatch(b) = decoded.payload {
                batches.push(b);
            }
        }
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || db.append(&table, &batches))
            .await
            .map_err(|e| Status::internal(format!("join: {e}")))?
            .map_err(|e| Status::invalid_argument(format!("append: {e}")))?;
        let ack = PutResult {
            app_metadata: format!(
                "{{\"rows\":{},\"bytes\":{},\"segments\":{}}}",
                result.rows, result.bytes, result.segments
            )
            .into_bytes()
            .into(),
        };
        Ok(Response::new(Box::pin(stream::iter(vec![Ok(ack)]))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange lands later"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action lands later"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions lands later"))
    }
}

/// 在指定地址启动 Flight 服务（h2c）。
pub async fn serve_flight(
    db: Arc<Qsdb>,
    token: Option<String>,
    bind: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use arrow_flight::flight_service_server::FlightServiceServer;
    let addr: std::net::SocketAddr = bind.parse()?;
    let svc = QsdbFlightService::new(db, token);
    println!("flight listening on {bind} (h2c)");
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::flight_service_client::FlightServiceClient;

    #[tokio::test]
    async fn flight_roundtrip() {
        let root = std::env::temp_dir().join(format!(
            "qsdb_flight_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let db = Arc::new(Qsdb::open(&root, qsdb::QsdbOptions::default()).unwrap());
        db.sql("CREATE TABLE t (k BIGINT, v BIGINT) PARTITIONED BY (k)")
            .unwrap();
        db.sql("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

        // 临时口起服务
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let db2 = Arc::clone(&db);
        let handle = tokio::spawn(async move { serve_flight(db2, None, &addr.to_string()).await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightServiceClient::new(channel);

        // do_get 查询
        let resp = client
            .do_get(Request::new(Ticket {
                ticket: b"SELECT k, v FROM t ORDER BY k".to_vec().into(),
            }))
            .await
            .unwrap();
        let frames: Vec<FlightData> = resp.into_inner().try_collect().await.unwrap();
        assert!(!frames.is_empty());

        // get_flight_info
        let info = client
            .get_flight_info(Request::new(FlightDescriptor {
                r#type: 2, // DescriptorType::Cmd
                cmd: b"SELECT count(*) FROM t".to_vec().into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(info.get_ref().total_records, 1);

        // do_put：首帧 descriptor + schema/批（encoder 造流，物化后注入 descriptor）
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(Int64Array::from(vec![30])),
            ],
        )
        .unwrap();
        let mut frames: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_schema(batch.schema())
            .build(stream::iter(vec![Ok(batch)]))
            .try_collect()
            .await
            .unwrap();
        if let Some(first) = frames.first_mut() {
            first.flight_descriptor = Some(FlightDescriptor {
                r#type: 2, // DescriptorType::Cmd
                cmd: b"table:t".to_vec().into(),
                ..Default::default()
            });
        }
        let framed = stream::iter(frames);
        let put_resp = client.do_put(framed).await.unwrap();
        let acks: Vec<PutResult> = put_resp.into_inner().try_collect().await.unwrap();
        assert_eq!(acks.len(), 1);

        // 写入可读
        let n = db.sql_collect("SELECT count(*) FROM t").unwrap();
        assert_eq!(n[0].num_rows(), 1);

        handle.abort();
        let _ = std::fs::remove_dir_all(&root);
    }
}
