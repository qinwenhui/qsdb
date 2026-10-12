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
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

pub struct QsdbFlightService {
    /// 与 HTTP 面**共用同一份** AppState：查询注册表（取消/并发闸）、token、指标。
    state: Arc<crate::server::AppState>,
}

impl QsdbFlightService {
    pub fn new(state: Arc<crate::server::AppState>) -> Self {
        Self { state }
    }

    /// 从 gRPC metadata 取客户端透传的 query id（契约 J11，与 HTTP 头同语义）。
    fn client_query_id(meta: &MetadataMap) -> Result<Option<String>, Status> {
        let Some(v) = meta.get("x-qsdb-query-id") else {
            return Ok(None);
        };
        match v
            .to_str()
            .ok()
            .filter(|s| crate::server::valid_client_query_id(s))
        {
            Some(id) => Ok(Some(id.to_string())),
            None => Err(Status::invalid_argument(
                "invalid x-qsdb-query-id: expect 1..=64 chars of [A-Za-z0-9._:-]",
            )),
        }
    }

    /// 经**注册表**执行 SQL——与 HTTP 面同语义：并发闸、可取消
    /// （`/queries/{id}/cancel`）、J11 客户端 id。此前各 Flight 方法各建一个游离
    /// ctx（`sql_collect` 更是走 benign ctx）：既不受闸门约束，也无法被取消。
    ///
    /// 返回（**实际使用**的 query_id, 执行结果）。
    async fn run_registered(
        &self,
        meta: &MetadataMap,
        sql: &str,
    ) -> Result<(String, qsdb::sql::SqlResult), Status> {
        let ctx = self
            .state
            .register_query(Self::client_query_id(meta)?, None, sql.to_string())
            .map_err(|msg| {
                // 并发闸拒绝：与 HTTP 面同一个码（RESOURCE_EXHAUSTED = 可退避重试）
                crate::error_code::grpc_status_msg(
                    crate::error_code::ErrorCode::TooManyQueries,
                    msg,
                )
            })?;
        let query_id = ctx.query_id.clone();
        let db = Arc::clone(&self.state.db);
        let sql_exec = sql.to_string();
        let ctx_exec = Arc::clone(&ctx);
        let res =
            tokio::task::spawn_blocking(move || db.sql_with_context(&sql_exec, &ctx_exec)).await;
        self.state.unregister_query(&query_id);
        // 查询错误走**同一张码表**（`ErrorCode::from_qsdb`）→ gRPC status：
        // 此前一律 invalid_argument，超时/内存超限/只读库全被压成"参数错"，
        // Flight 客户端既看不出该不该重试，也映射不出 SQLState。
        let res = res
            .map_err(|e| Status::internal(format!("join: {e}")))?
            .map_err(|e| crate::error_code::grpc_status(&e))?;
        Ok((query_id, res))
    }

    /// Bearer 鉴权（token 配置时强制；同 HTTP API 约定）。
    fn check_auth(&self, meta: &MetadataMap) -> Result<(), Status> {
        let Some(want) = &self.state.token else {
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
        let meta = request.metadata().clone();
        self.check_auth(&meta)?;
        let desc = request.into_inner();
        let sql = String::from_utf8(desc.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8 SQL"))?;
        let (_, res) = self.run_registered(&meta, &sql).await?;
        let (schema, rows) = match res {
            qsdb::sql::SqlResult::Query { schema, batches } => {
                (schema, batches.iter().map(|b| b.num_rows()).sum::<usize>())
            }
            qsdb::sql::SqlResult::Ok { message } => {
                return Err(Status::invalid_argument(format!("not a query: {message}")));
            }
        };
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
        let meta = request.metadata().clone();
        self.check_auth(&meta)?;
        let desc = request.into_inner();
        let sql = String::from_utf8(desc.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8 SQL"))?;
        let (_, res) = self.run_registered(&meta, &sql).await?;
        let schema = match res {
            qsdb::sql::SqlResult::Query { schema, .. } => schema,
            qsdb::sql::SqlResult::Ok { message } => {
                return Err(Status::invalid_argument(format!("not a query: {message}")));
            }
        };
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
        let meta = request.metadata().clone();
        self.check_auth(&meta)?;
        let sql = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket must be UTF-8 SQL"))?;
        let (query_id, res) = self.run_registered(&meta, &sql).await?;
        let batches = match res {
            qsdb::sql::SqlResult::Query { batches, .. } => batches,
            qsdb::sql::SqlResult::Ok { message } => {
                return Err(Status::invalid_argument(format!("not a query: {message}")));
            }
        };
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
        let mut resp: Response<Self::DoGetStream> = Response::new(Box::pin(s));
        // J11 在 Flight 面的回显：响应 metadata 透出**实际使用**的 id
        if let Ok(v) = tonic::metadata::MetadataValue::try_from(query_id.as_str()) {
            resp.metadata_mut().insert("x-qsdb-query-id", v);
        }
        Ok(resp)
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
        while let Some(decoded) = decoder.try_next().await.map_err(|e| {
            // 流解码失败 = INVALID_IPC（同 HTTP 面 /write 的取码）
            Status::invalid_argument(format!("decode: {e}"))
        })? {
            if let DecodedPayload::RecordBatch(b) = decoded.payload {
                batches.push(b);
            }
        }
        let db = Arc::clone(&self.state.db);
        let result = tokio::task::spawn_blocking(move || db.append(&table, &batches))
            .await
            .map_err(|e| Status::internal(format!("join: {e}")))?
            // 写错误同表映射（只读库 → FAILED_PRECONDITION 等，此前一律
            // invalid_argument，只读/内部错误全被压扁）
            .map_err(|e| crate::error_code::grpc_status(&e))?;
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
    state: Arc<crate::server::AppState>,
    bind: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use arrow_flight::flight_service_server::FlightServiceServer;
    let addr: std::net::SocketAddr = bind.parse()?;
    let svc = QsdbFlightService::new(state);
    // 自持 listener：回显**实际绑定**端口（`--flight-bind 127.0.0.1:0` 才可用，
    // 与 HTTP 侧 `--addr :0` 同口径）
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    let bound = std_listener.local_addr()?;
    let listener = tokio::net::TcpListener::from_std(std_listener)?;
    let incoming: tonic::transport::server::TcpIncoming = listener.into();
    println!("flight listening on {bound} (h2c)");
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::flight_service_client::FlightServiceClient;
    use qsdb::Qsdb;

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
        let state = Arc::new(crate::server::AppState::new(Arc::clone(&db), None, 16));
        let handle = tokio::spawn(async move { serve_flight(state, &addr.to_string()).await });
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
