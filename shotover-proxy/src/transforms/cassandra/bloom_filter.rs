use std::io::Cursor;
use murmur3::murmur3_x64_128;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::message;
use crate::message::{Message, Messages, QueryType};
use crate::protocols::cassandra_codec::CassandraCodec;
use crate::protocols::RawFrame;
use crate::transforms::util::unordered_cluster_connection_pool::OwnedUnorderedConnectionPool;
use crate::transforms::util::Request;
use crate::transforms::{Transform, Transforms, Wrapper};
use bloomfilter::bloomfilter::{Shape, Simple, BloomFilterType, BloomFilter};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::{Frame, StreamId};
use metrics::{counter, register_counter, Unit};
use serde::Deserialize;
use std::collections::HashMap;
use std::string::FromUtf8Error;
use std::time::Duration;
use bloomfilter::bloomfilter::hasher::{Hasher, HasherType, SimpleHasher};
use bytes::Buf;
use cassandra_protocol::frame::frame_result::ColType::Udt;
use cassandra_protocol::query::QueryParams;
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, trace};
use crate::codec::cassandra::CassandraCodec;
use crate::frame::{CassandraFrame, CassandraOperation, CQL};
use tree_sitter::{Language, Tree, TreeCursor, Node, Query, QueryCursor, QueryCapture, QueryMatch, LogType};
use tree_sitter_cql::Parser;
use crate::transforms::cassandra::bloom_filter::CassandraASTStatementType::{AlterKeyspace, AlterMaterializedView, AlterRole, AlterTable, AlterType, AlterUser, ApplyBatch, CreateAggregate, CreateFunction, CreateIndex, CreateKeyspace, CreateMaterializedView, CreateRole, CreateTable, CreateTrigger, CreateType, CreateUser, DeleteStatement, DropAggregate, DropFunction, DropIndex, DropKeyspace, DropMaterializedView, DropRole, DropTable, DropTrigger, DropType, DropUser, Grant, InsertStatement, ListPermissions, ListRoles, Revoke, SelectStatement, Truncate, Update, UseStatement};

/// The configuration for a single bloom filter in a single table.
#[derive(Deserialize, Debug, Clone)]
pub struct CassandraBloomFilterTableConfig {
    #[serde(rename = "remote_address")]
    /// keyspace for the table name
    pub keyspace: String,
    /// the table name holding the bloom filter
    pub table: String,
    /// the column name for the bloom filter
    pub bloomColumn: String,
    /// the number of bits in the bloom filter
    pub bits: usize,
    /// the number of functions in the bloom filter
    pub funcs: usize,
    /// the names of the columns that are added to the bloom filter
    pub columns: Vec<String>,
}


#[derive(Debug)]
pub struct CassandraBloomFilter {
    /// the outbound connection for this filter
    outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec>>,
    /// the name of the chain
    chain_name: String,
    /// a mapping of fully qualified table names to configurations.
    /// Fully qualified => keyspace . table
    tables: HashMap<String, CassandraBloomFilterTableConfig>,
    /// a mapping of messages by stream id.
    messages : HashMap<StreamId, QueryMessage>,
}

impl Clone for CassandraBloomFilter {
    fn clone(&self) -> Self {
        CassandraBloomFilter::new(&self.tables, self.chain_name.clone())
    }
}

impl CassandraBloomFilter {
    pub fn new(tables: &HashMap<String, CassandraBloomFilterTableConfig>, chain_name: String) -> CassandraBloomFilter {
        let sink_single = CassandraBloomFilter {
            outbound: None,
            chain_name: chain_name.clone(),
            tables: tables.clone(),
            messages : HashMap.new(),
        };

        register_counter!("failed_requests", Unit::Count, "chain" => chain_name, "transform" => sink_single.get_name());

        sink_single
    }

    fn get_name(&self) -> &'static str {
        "CassandraBloomFilter"
    }

    /// Create the hasher for a string value
    fn make_hasher(value : String) -> HasherType {
        let hash_result = murmur3_x64_128(&mut Cursor::new(value), 0)?;
        let mask : u64 = (-1 : i64) as u64;
        let initial = ((hash_result >> 64) & mask) as u64;
        let incr = (hash_result & mask) as u64;
        SimpleHasher::new( initial, incr )
    }

    /// encode the bloom filter for a blob colum in Cassandra
    fn make_filter_column( bloomFilter : Box<dyn BloomFilter> ) -> String {
        let bitMaps = bloomFilter.get_bitmaps() ;

        let mut parts: Vec<String> = Vec::with_capacity( bitMaps.len()+1);
        parts.push( "0x".to_string());
        for word in bitMaps {
            parts.push( format!("{:X}", word))
        }
        parts.join("")
    }

    /// process the query message.  This method has to handle all possible Queries that could
    /// impact the Bloom filter.
    fn process_query(&self, frame : &CassandraFrame, query: String, params: QueryParams ) -> msg {
        let language = tree_sitter_cql::language();
        let mut parser = tree_sitter_cql::Parser::new();
        if parser.set_language(language).is_err() {
            panic!("language version mismatch");
        }

        //parser.set_logger( Some( Box::new( log)) );
        let source_code = query.as_bytes();
        let tree = parser.parse(source_code, None).unwrap();
        println!("{}", tree.root_node().to_sexp());

        match CassandraAST::get_statement_type( tree ) {
            SelectStatement => process_select(tree),
            InsertStatement => process_insert(tree),
            DeleteStatement => process_delete(tree),
            UseStatement => process_use(tree),
            _ => msg,
        }
    }

    fn  process_insert( tree : Tree ) {
        let nodes: Box<Vec<Node>> = CassandraAST::search( tree.root_node(), "insert_column_spec / column_list" );

    }

    fn process_delete( tree : Tree ) {

    }

    fn process_use( tree : Tree ) {

    }

    fn process_select( tree : Tree ) {
        query.to_query_string()
        let x = frame.operation;
        msg.namespace()
        let fq_table_name = msg.namespace.join( ".");
        let config = self.tables.get( fq_table_name )?;
        let shape = Shape {
            m : config.bits,
            k : config.funcs,
        };
        let query_values = msg.query_values?;
        let mut bloomfilter = Simple::empty_instance(self.get_shape( fq_table_name ))
        let mut columns: Vec<String> = vec![];
        // modify these to remove ones from bloom filter
        let mut values =  msg.query_values?.clone();
        let mut rqd_projection = msg.projection?.clone();
        for column_name in config.columns {
            match query_values.get( column_name.as_str() ) {
                Some(value) => {
                    if ! rqd_projection.contains( &column_name ) {
                        rqd_projectionj.insert(column_name.clone());
                    }
                    values.remove( column_name.as_str() );
                    bloomfilter.merge_hasher_in_place( self.make_hasher(value));
                },
                _ => {},
            }
        }

        // bloom filter now contains all the columns from the query.
        // oldValues contains the column-value pairs that went into the bloom filter
        // values contains only the values that were moved to the query.
        values.insert( config.bloomColumn.clone(), self.make_filter_column( bloomfilter ));

        QueryMessage{
            query_string: "".to_string(),
            namespace: msg.namespace.clone(),
            primary_key: msg.primary_key.clone(),
            query_values: Some(values),
            projection: Some(rqd_projection),
            query_type: msg.query_type.clone(),
            ast: None
        }
    }

    fn remove_unwanted_data( row_data : &mut Vec<HashMap<String,Value>>, &old_msg : &QueryMessage) {
        row_data.retain( |mut row| {
            for (name, expected) in old_msg.query_values?.keys() {
                if row.get(name) != expected {
                    return false;
                }
                if ! old_msg.projection?.contains( name ) {
                    row.remove( name );
                }
            }
            true
        };
    }

    fn process_result( &mut self, query_response : & QueryResponse, stream : &StreamId) {
        let query_msg = self.messages.remove(stream);
        match query_msg  {
            Some( old_msg ) =>match query_response.result.unwrap() {
                Value::NamedRows(mut rows) => {
                    CassandraBloomFilter::remove_unwanted_data(&mut rows, &old_msg);
                },
                _ => {},
            },
            _ => {},
        }
    }

    fn process_bloom_data(&mut self, messages : Messages ) -> Messages {
        let mut new_msgs: Messages = vec![];

        for mut msg in messages {
            let stream = match msg.stream_id {
                Some(id) => id,
                None => {
                    info!("no cassandra frame found");
                    new_msgs.push(msg);
                    break;
                }
            };
            let frame = msg.frame().unwrap().into_cassandra().unwrap();

            match frame.operation {
                CassandraOperation::QueryType{ query, params } => {
                    self.messages.insert(streamId, msg.clone());

                    new_msgs.push(self.process_query(&frame, query.to_query_string(), params);
                },
                CassandraOperation::Result(result) => {
                    self.process_result(& result, &stream);
                    new_msgs.push(msg.clone());
                },
                _ => new_msgs.push(msg.clone()),
            };
        }
        new_msgs
    }


    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        loop {
            match self.outbound {
                None => {
                    trace!("creating outbound connection {:?}", self.address);
                    let mut conn_pool = OwnedUnorderedConnectionPool::new(
                        self.address.clone(),
                        CassandraCodec::new(),
                    );
                    // we should either connect and set the value of outbound, or return an error... so we shouldn't loop more than 2 times
                    conn_pool.connect(1).await?;
                    self.outbound = Some(conn_pool);
                }
                Some(ref mut outbound_framed_codec) => {
                    trace!("sending frame upstream");
                    let sender = outbound_framed_codec
                        .connections
                        .get_mut(0)
                        .expect("No connections found");
                    let expected_size = messages.len();
                    let results: Result<FuturesOrdered<Receiver<(Message, ChainResponse)>>> =
                        messages
                            .into_iter()
                            .map(|m| {
                                let (return_chan_tx, return_chan_rx) =
                                    tokio::sync::oneshot::channel();
                                let stream = if let RawFrame::Cassandra(frame) = &m.original {
                                    frame.stream_id
                                } else {
                                    info!("no cassandra frame found");
                                    return Err(anyhow!("no cassandra frame found"));
                                };

                                sender.send(Request {
                                    messages: m,
                                    return_chan: Some(return_chan_tx),
                                    message_id: Some(stream),
                                })?;

                                Ok(return_chan_rx)
                            })
                            .collect();

                    let mut responses = Vec::with_capacity(expected_size);
                    let mut results = results?;

                    loop {
                        match timeout(Duration::from_secs(5), results.next()).await {
                            Ok(Some(prelim)) => {
                                match prelim? {
                                    (_, Ok(mut resp)) => {
                                        for message in &resp {
                                            if let RawFrame::Cassandra(Frame {
                                                opcode: cassandra_protocol::frame::Opcode::Error,
                                                ..
                                            }) = &message.original
                                            {
                                                counter!("failed_requests", 1, "chain" => self.chain_name.clone(), "transform" => self.get_name());
                                            }
                                        }
                                        responses.append(&mut resp);
                                    }
                                    (m, Err(err)) => {
                                        responses.push(Message::new_response(
                                            QueryResponse::empty_with_error(Some(
                                                message::Value::Strings(format!("{}", err)),
                                            )),
                                            true,
                                            m.original,
                                        ));
                                    }
                                };
                            }
                            Ok(None) => break,
                            Err(_) => {
                                info!(
                                    "timed out waiting for results got - {:?} expected - {:?}",
                                    responses.len(),
                                    expected_size
                                );
                                info!(
                                    "timed out waiting for results - {:?} - {:?}",
                                    responses, results
                                );
                            }
                        }
                    }

                    return Ok(responses);
                }
            }
        }
    }

}

impl CassandraAST {

    // TODO move this to tree-sitter-cql or other external location.
    pub fn search<'tree>(node : Node<'tree>, path : &'static str) -> Box<Vec<Node<'tree>>> {
        let mut nodes = Box::new(vec!(node));
        for segment in path.split( '/').map(|tok| tok.trim() ) {
            let mut newNodes  = Box::new(vec!());
            for node in nodes.iter() {
                _find(&mut newNodes, *node, segment);
            }
            nodes = newNodes;
        }
        nodes
    }

    // TODO move this to tree-sitter-cql or other external location.
    fn _find<'tree>(nodes : &mut Vec<Node<'tree>>, node : Node<'tree>, name : &str) {
        if node.kind().eq(name) {
            nodes.push( node );
        } else {
            if node.child_count() > 0 {
                for childNo in 0..node.child_count() {
                    _find( nodes, node.child( childNo ).unwrap(), name );
                }
            }
        }
    }

    // TODO move this to tree-sitter-cql or other external location.
    fn has(node : Node, path : &'static str) -> bool {
        return ! search( node, path ).is_empty()
    }

    pub fn get_statement_type( tree : Tree ) -> CassandraASTStatementType {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child( 0 ).unwrap();
        }
        CassandraASTStatementType::from_node( node );
    }
}

pub enum CassandraASTStatementType {
    AlterKeyspace,
    AlterMaterializedView,
    AlterRole,
    AlterTable,
    AlterType,
    AlterUser,
    ApplyBatch,
    CreateAggregate,
    CreateFunction,
    CreateIndex,
    CreateKeyspace,
    CreateMaterializedView,
    CreateRole,
    CreateTable,
    CreateTrigger,
    CreateType,
    CreateUser,
    DeleteStatement,
    DropAggregate,
    DropFunction,
    DropIndex,
    DropKeyspace,
    DropMaterializedView,
    DropRole,
    DropTable,
    DropTrigger,
    DropType,
    DropUser,
    Grant,
    InsertStatement,
    ListPermissions,
    ListRoles,
    Revoke,
    SelectStatement,
    Truncate,
    Update,
    UseStatement,
    UNKNOWN( String ),
}

impl CassandraASTStatementType {
    pub fn from_node( node : Node ) -> CassandraASTStatementType {
        match node.kind() {
            "alter_keyspace" => AlterKeyspace,
            "alter_materialized_view" => AlterMaterializedView,
            "alter_role" => AlterRole,
            "alter_table" => AlterTable,
            "alter_type" => AlterType,
            "alter_user" => AlterUser,
            "apply_batch" => ApplyBatch,
            "create_aggregate" => CreateAggregate,
            "create_function" => CreateFunction,
            "create_index" => CreateIndex,
            "create_keyspace" => CreateKeyspace,
            "create_materialized_view" => CreateMaterializedView,
            "create_role" => CreateRole,
            "create_table" => CreateTable,
            "create_trigger" => CreateTrigger,
            "create_type" => CreateType,
            "create_user" => CreateUser,
            "delete_statement" => DeleteStatement,
            "drop_aggregate" => DropAggregate,
            "drop_function" => DropFunction,
            "drop_index" => DropIndex,
            "drop_keyspace" => DropKeyspace,
            "drop_materialized_view" => DropMaterializedView,
            "drop_role" => DropRole,
            "drop_table" => DropTable,
            "drop_trigger" => DropTrigger,
            "drop_type" => DropType,
            "drop_user" => DropUser,
            "grant" => Grant,
            "insert_statement" => InsertStatement,
            "list_permissions" => ListPermissions,
            "list_roles" => ListRoles,
            "revoke" => Revoke,
            "select_statement" => SelectStatement,
            "truncate" => Truncate,
            "update" => Update,
            "use" => UseStatement,
            _ => CassandraASTStatementType::UNKNOWN( node.kind().to_string() )
        }
    }
}
}



#[async_trait]
impl Transform for CassandraBloomFilter {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let messages = self.send_message(self.process_bloom_data(message_wrapper.messages)).await?
        Ok(self.process_bloom_data( messages ) )
    }

    fn is_terminating(&self) -> bool {
        false
    }

}
