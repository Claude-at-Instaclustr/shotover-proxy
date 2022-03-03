use reduce::Reduce;
use std::io::Cursor;
use murmur3::murmur3_x64_128;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::message;
use crate::message::{Message, Messages, QueryType};
//use crate::protocols::cassandra_codec::CassandraCodec;
//use crate::protocols::RawFrame;
use crate::transforms::util::unordered_cluster_connection_pool::OwnedUnorderedConnectionPool;
use crate::transforms::util::Request;
use crate::transforms::{Transform, Transforms, Wrapper};
use bloomfilter::bloomfilter::{Shape, Simple, BloomFilter};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::{Frame, StreamId, Version};
use metrics::{counter, register_counter, Unit};
use serde::Deserialize;
use std::collections::HashMap;
use std::iter::Map;
use std::string::FromUtf8Error;
use std::time::Duration;
use bloomfilter::bloomfilter::bitmap_producer::BitMapProducer;
use bloomfilter::bloomfilter::hasher::{Hasher, HasherCollection, HasherType, SimpleHasher};
use bytes::Buf;
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, ErrorBody};
use cassandra_protocol::frame::frame_result::ColType::Udt;
use cassandra_protocol::query::QueryParams;
use futures::StreamExt;
use tls_parser::nom::error::make_error;

use tokio::io::AsyncReadExt;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tracing::{info, trace};
use crate::codec::cassandra::CassandraCodec;
use crate::frame::{CassandraFrame, CassandraOperation, CQL};
use tree_sitter::{Language, Parser, Tree, TreeCursor, Node, Query, QueryCursor, QueryCapture, QueryMatch, LogType};
use proc_macro::TokenTree;
use crate::frame::CassandraOperation::Error;
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
    messages : HashMap<StreamId, BloomFilterState>,
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
    fn process_query(&self, state : &mut BloomFilterState) -> Message {
        match ast.statement_type {
            SelectStatement => self.process_select( state ),
            InsertStatement => self.process_insert(state),
            DeleteStatement => self.process_delete(state),
            UseStatement => { self.process_use( &mut state.original_ast); state.original_msg},
            _ => {
                state.ignore = true;
                state.original_msg
            },
        }
    }

    fn process_insert(&self, state : &mut BloomFilterState ) -> Message {
        let cfg = self.check_table_name( state );
        match cfg {
            None => msg,
            Some(config) => {
                // we need to determine if the bloom filter is being updated if any of the
                // affected nodes are also updated.
                // this is not quite accurate as we do not deal with simultaneous updates
                let nodes: Box<Vec<Node>> = state.original_ast.search( "insert_column_spec / column_list / column" );
                if nodes.is_empty() {
                    self.make_error( state, "columns must be specified in inserts for Bloom filter based tables")
                }
                // the names of the column
                let columns : Vec<String> = nodes.iter().map( |n| state.original_ast.node_text(n) ).collect();
                let some_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a || b)?;
                let all_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a && b)?;
                let has_filter = columns.contains( &config.bloomColumn );
                // cases to check
                // none of the columns -> ok
                // has filter -> ok
                // some but not all columns  -> error
                // all columns and not bloom filter -> add bloom filter
                // all columns and bloom filter -> ok
                if !some_columns || has_filter {
                    state.ignore = true;
                    return msg;
                }

                // some but not all and no filter
                if !all_columns {
                    self.make_error( state, "modifying some but not all bloom filter columns requires updating filter" )
                }
                // all the columns in the filter are being inserted and there is no bloom filter provided
                // so build the filter

                if insert_values_spec.child(0).unwrap().kind().equals( "JSON") {
                    self.make_error( state, "column values may not be set with JSON in inserts for Bloom filter based tables")
                }

                let mut hashers = HasherCollection::new();

                // assignment_map|assignment_list|assignment_set|assignment_tuple

                let expressions: Box<Vec<Node>> = state.original_ast.search( "insert_values_spec / expression_list / expression" );

                // there should be one expression for each column
                if expressions.len() != columns.len() {
                    self.make_error( state, "not enough values for the columns");
                }
                for i in 0..expressions.len() {
                    if config.columns.contains( columns.get(i) ) {
                        expr :Node = expressions.get(i);
                        if CassandraAST::hasNode(expr, "assignment_map|assignment_list|assignment_set|assignment_tuple") {
                            self.make_error( state, "assignment maps, lists, sets or tuples may not be used to specify values for bloom filter columns.");
                        }
                        hashers.add( self.make_hasher( state.original_ast.node_text( expr )));
                    }
                }
                let shape = Shape {
                    m: config.bits,
                    k: config.funcs,
                };

                filter = Simple::from_hasher( &shape, &hashers );
                state.added_values.push( ( config.bloomColumn, CassandraBloomFilter::as_hex( filter )));
            }
        }
        msg
    }

    fn check_table_name( &self, state : &mut BloomFilterState ) -> Some(CassandraBloomFilterTableConfig) {
        let table_name = state.original_ast.get_table_name();
        let cfg = self.tables.get( table_name.as_str() );
        state.ignore = cfg.is_none();
        cfg
    }

    fn process_delete( &self, state : &mut BloomFilterState ) -> Message {
        let cfg = self.check_table_name( state );
        match cfg {
            None => msg,
            Some(config) => {
                // we need to determine if bloom filter columns are being deleted and the
                // bloom filter not updated
                let nodes: Box<Vec<Node>> = state.original_ast.search( "delete_column_list / column" );
                let columns : Vec<String> = nodes.iter().map( |n| state.original_ast.node_text(n) ).collect();
                let some_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a || b)?;
                let all_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a && b)?;
                let has_filter = columns.contains( &config.bloomColumn );
                // cases to check
                // none of the columns -> ok
                // some of the columns and bloom filter -> ok
                // otherwise error
                if !some_columns || has_filter {
                    return msg;
                }
                self.make_error( state, "deleting bloom filter columns requires deleting filter")
            }
        }
    }

    fn process_use(&self, ast: &mut CassandraAST) {
        // structure should be (source_file (use))
        ast.default_keyspace = Some(ast.node_text( ast.tree.root_node().child(0).child(1)));
    }

    fn make_error( &self, state : &mut BloomFilterState, message : &str ) -> Message {
        let mut frame  = state.original_msg.frame().unwrap().clone().into_cassandra()?;
        frame.operation = Error(ErrorBody {
            error_code: 0x2200,
            message: message.to_string(),
            additional_info: AdditionalErrorInfo::Invalid,
        });
        let mut new_msg = Message::from_frame( Frame::Cassandra(frame) );
        new_msg.meta_timestamp = msg.meta_timestamp;
        new_msg.return_to_sender = true;
        new_msg
    }

    fn process_select(&self, state : &mut BloomFilterState) -> Message {
        let cfg = self.check_table_name( state);
        match cfg {
            None =>  state.original_msg,
            Some(config) => {
                // we need to
                // remove any where clauses that have the bloom filter columns
                // build a bloom filter with the values
                // insert the bloom filter where clause into the query
                // add the where clause bloom filter columns to the select so we can filter later

                let mut hashers = HasherCollection::new();

                // the colums we are interested in are in the where clause
                let column_nodes = state.original_ast.search( "where_spec / relation_elements / column" );
                // get a list of all the column names.
                let columns_names : Vec<String> = nodes.iter().map( |n| state.original_ast.node_text(n) ).collect();
                // see if some of the columns are in the bloom filter cnfiguration.
                let some_columns : bool = column_names.map( |c| config.columns.contains( c )).reduce( |a,b|  a || b);

                // there is a weird case where the bloom filter is provided along with the search values
                // we will assume that the user knows what they want and not touch the filter but will
                // remove the columns from the query so the query will execute.  We will add the columns
                if some_columns {
                    // see if the bloom filter is in the where clause
                    let has_filter = columns_names.contains( &config.bloomColumn );
                    // create a list of column node ids in the where clause that match the columns we are interested in
                    let interesting_columns : Vec<usize> = column_nodes.iter()
                        .filter( |n :Node| config.columns.contains( &ast.node_text(n) ))
                        .map( |n| n.id() ).collect();

                    /*
                    Removing interesting columns from the where clause.
                    Retaining a list of the comparisons to check on the return
                    adding the bloom filter if necessary.
                    Now we process the relations by moving interesting columns into the select clause,

                    relations come in several flavors:
                        column comparator constant
                        function comparator constant
                        function comparator function
                        column IN ( function_args )
                        (column ...) IN ( tuple...)
                        (column ...) comparator tuple...
                        column CONTAINS constant
                        column CONTAINS KEY constant

                    a tuple is defined as
                        tuple => ( constant, (constant | tuple )...)
                            or  (constant (tuple ...))
                    we are only interested in the ones that have columns
                     */

                    let mut interesting_relations : Vec<Node> = state.original_ast.search( "where_spec / relation_elements[column]" )
                        .iter().filter( |node| CassandraAST.filterNode( node, |n| interesting_columns.contains( n.id()) ))
                        .collect();

                    state.ignore = false;
                    for relation in interesting_relations {
                        if !has_filter && CassandraAST::hasNode( relation, "<|<=|<>|>|>=|IN|CONTAINS") {
                            return self.make_error( state, "only equality checks are allowed if bloom filter is not provided" );
                        }

                        if relation.child(0).unwrap().kind().equals( "(") {
                            // is a list of columns
                        } else {
                            let column = relation.child(0).unwrap();
                            state.added_selects.push( column );
                            state.verify_funcs.push( relation );
                            if !has_filter {
                                // only get here if we have equality statement and no filter defined
                                hashers.add( self.make_hasher( state.original_ast.node_text( column )));
                            }
                        }
                    }
                    let selected_columns : Vec<usize> = state.original_ast.search( "select_element / column ")
                        .iter().map( |n : Node| n.id() ).collect();
                    // remove duplicate select columns
                    state.added_selects = state.added_selects.iter()
                        .filter( |n| selected_columns.contains(n.id()))
                        .collect();
                    if !has_filter {
                        let shape = Shape {
                            m: config.bits,
                            k: config.funcs,
                        };
                        filter = Simple::from_hasher( &shape, &hashers );

                        state.added_where.push( format!( "{} = {}", config.bloomColumn, CassandraBloomFilter::as_hex( filter ) ) );
                   }
                    let new_query = state.rewrite_query();
// TODO put the new query into the message.
// END OF EDIT
                }

            }
        }
        msg
    }

    fn as_hex( producer : & dyn BitMapProducer ) -> String {
        let mut hex = String::from( "0x");
        producer.get_bitmaps().for_each( |u| hex.push_str( format!("{:#010x}", u ).as_str()));
        hex
    }

/*
    fn node_filter_print( &self,ast : CassandraAST, node : &node, moved_columns : Vec<String> ) {
        moved_columns
        if (node.kind() == )
    }
    */

/*
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
*/
    // modify the message if Bloom filter data is involved.
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
                    let mut state = BloomFilterState {
                        ignore : false,
                        original_msg: msg.clone(),
                        original_ast: CassandraAST::new(query.to_query_string()),
                        verify_funcs: vec![],
                        added_selects: vec![],
                        added_where: vec![],
                        added_values: vec![]
                    };
                    new_msgs.push(self.process_query( &mut state ));
                    self.messages.insert(streamId, state);
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
                                    message: m,
                                    return_chan: Some(return_chan_tx),
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

pub struct BloomFilterState {
    ignore: bool,
    original_msg: QueryMessage,
    original_ast: CassandraAST,
    verify_funcs: Vec<Node>,
    added_selects: Vec<Node>,
    added_where: Vec<String>,
    added_values: Vec<(String,String)>,
}

impl BloomFilterState {
    pub fn rewrite_query( &self ) -> String {
        let mut result = String::new();
        let has_where = self.original_ast.has( "where_spec");
        _rewrite( self.original_ast.walk(), &result );
        if !has_where && ! self.added_where.is_empty() {
            result.push_str( " WHERE ");
            self.added_where.for_each( |s| result.push_str(  format!( " AND {}", s  ).as_str()));
        }
        result
    }

    fn _rewrite(&self, cursor : &mut TreeCursor , result : &mut String) {
        let skip = self.verify_funcs.contains( cursor.node() );
        if (!skip) {
            if cursor.node().is_named() {
                result.push_str(self.original_ast.node_text(cursor.node()).as_str())
            }
            if (cursor.goto_first_child()) {
                self._rewrite(cursor, result);
            }
            if (cursor.goto_next_sibling()) {
                self._rewrite(cursor, result);
            }
        }
        // leaving check
        if cursor.node().kind().equals( "select_elements") {
            self.added_selects.for_each( |column| result.push_str( format!( ", {}", self.original_ast.node_text( column ) ).as_str() ))
        }
        if cursor.node().kind().equals( "where_spec") {
            self.added_where.for_each( |s| result.push_str(  format!( " AND {}", s  ).as_str()));
        }
        if cursor.node().kind().equals( "insert_column_spec") && ! self.added_values.is_empty() {
            // we have to insert before last char which is the ')'
            result.remove( result.len()-1 );
            self.added_values.for_each( |(x,y)| result.push_str( format!( ", {}", x  ).as_str()));
            result.push(')');
        }
        if cursor.node().kind().equals( "expression_list") && ! self.added_values.is_empty() {
            // we have to insert before last char which is the ')'
            self.added_values.for_each( |(x,y)| result.push_str( format!( ", {}", y  ).as_str()));
        }
        // now leaving
        cursor.goto_parent();
    }

}

pub struct CassandraAST {
    text: [u8],
    tree: Tree,
    statement_type: CassandraASTStatementType,
    default_keyspace: Option<String>,

}

impl CassandraAST {

    /// create an AST from the query string
    pub fn new(cassandra_statement : String) ->CassandraAST {
        let language = tree_sitter_cql::language();
        let mut parser = tree_sitter_cql::Parser::new();
        if parser.set_language(language).is_err() {
            panic!("language version mismatch");
        }

        // this code enables debug logging
        /*
        fn log( _x : LogType, message : &str) {
            println!("{}", message );
        }
        parser.set_logger( Some( Box::new( log)) );
        */
        let tree = parser.parse(source_code, None).unwrap();

        CassandraAST {
            text : *cassandra_statement.as_bytes(),
            tree,
            statement_type: if tree.root_node().has_error() {
                    CassandraASTStatementType::UNKNOWN(cassandra_statement)
                } else {
                    CassandraASTStatementType::from_node(tree.root_node())
                },
            default_keyspace: None,
        }
    }

    /// returns true if the parsing exposed an error in the query
    pub fn has_error(&self) -> bool {
        self.tree.root_node().has_error()
    }

    /// retrieves the query value for the node (word or phrase enclosed by the node)
    pub fn node_text(&self, node : Node) -> String {
        node.utf8_text(&self.text)
    }

    pub fn get_table_name(&self) -> String {
        let nodes = self.search( "tree_name");
        match nodes.first() {
            None => "".to_string(),
            Some(node) => {
                let candidate_name = self.node_text( node );
                if candidate_name.contains(".") {
                    candidate_name.as_string()
                } else {
                    match &self.default_keyspace {
                        None => candidate_name,
                        Some(keyspace) => format!("{}.{}", keyspace, candidate_name)
                    }
                }
            }
        }
    }

    pub fn search(&self, path : &'static str) -> Box<Vec<Node>> {
        CassandraAST::searchNode(self.tree.root_node(), str)
    }

    // TODO move this to tree-sitter-cql or other external location.
    pub fn searchNode<'tree>(node : Node<'tree>, path : &'static str) -> Box<Vec<Node<'tree>>> {
        let mut nodes = Box::new(vec![node]);
        for segment in path.split('/').map(|tok| tok.trim()) {
            let mut newNodes = Box::new(vec![]);
            let pattern = Pattern::from_str( segment );
            for node in nodes.iter() {
                CassandraAST::_find(&mut newNodes, *node, &pattern );
            }
            nodes = newNodes;
        }
        nodes
    }

    // performs a recursive search in the tree
    fn _find<'tree>(nodes: &mut Vec<Node<'tree>>, node: Node<'tree>, pattern: &SearchPattern) {
        if pattern.name.is_match(node.kind()) {
            match &pattern.child {
                None => nodes.push(node),
                Some( child ) => if CassandraAST::_has( &node, child ) {
                    nodes.push(node);
                }
            }
        } else {
            if node.child_count() > 0 {
                for childNo in 0..node.child_count() {
                    CassandraAST::_find(nodes, node.child(childNo).unwrap(), pattern);
                }
            }
        }
    }

    /// checks if a node has a specific child node
    fn _has(node : &Node, name : &Regex) -> bool {
        if node.child_count() > 0 {
            for child_no in 0..node.child_count() {
                let child: Node = node.child(child_no).unwrap();
                let x = child.kind().eq(name);
                if name.is_match(child.kind()) || CassandraAST::_has(&child, name) {
                    return true;
                }
            }
        }
        false
    }

    pub fn has(&self, path : &'static str) -> bool {
        return ! self.search(  path ).is_empty()
    }

    pub fn hasNode(node : Node, path : &'static str) -> bool {
        return ! CassandraAST::searchNode( node, path ).is_empty()
    }

    pub fn extractNode(node : Node, f : fn(Node) ->bool ) -> Vec<Node> {
        let mut result : Vec<Node> = vec!();
        CassandraAST::applyNode( node, f, |n| result.push(n) );
        result
    }

    pub fn filter(&self,  f : fn(Node)->bool ) -> Vec<Node> {
        CassandraAST::extractNode(self.tree.root_node(), ids )
    }

    pub fn apply(&self, selector : fn(Node)->bool, action : fn(Node) ) {
        CassandraAST::applyNode( self.tree.root_node, selector, action );
    }

    pub fn applyNode(node : Node, selector : fn(Node)->bool, action : fn(Node) ) {
        if f( node ) {
            action(node);
        }
        if node.child_count() > 0 {
            for child_no in 0..node.child_count() {
                CassandraAST::applyNode( node.child(child_no).unwrap(), selector, action );
            }
        }
    }

    pub fn filterNode(node : Node, selector : fn(Node) ->bool ) -> bool {
        if f( node ) {
            true
        } else {
            if node.child_count() > 0 {
                for child_no in 0..node.child_count() {
                    if CassandraAST::filterNode(node.child(child_no).unwrap(), selector) {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn get_statement_type( tree : Tree ) -> CassandraASTStatementType {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child( 0 ).unwrap();
        }
        CassandraASTStatementType::from_node( node );
    }
}
pub struct SearchPattern {
    pub name : Regex,
    pub child : Option<Regex>,
}

impl SearchPattern {
    pub fn from_str( pattern : &str ) -> SearchPattern {
        let parts : Vec<&str> = pattern.split("[").collect();
        let namePat = format!("^{}$", parts[0].trim() );
        Pattern {
            name : Regex::new(  namePat.as_str() ).unwrap(),
            child : if parts.len()==2 {
                let name : Vec<&str> = parts[1].split("]").collect();
                let namePat = format!("^{}$", name[0].trim() );
                Some(Regex::new(namePat.as_str()).unwrap())
            } else {
                None
            },
        }
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



#[async_trait]
impl Transform for CassandraBloomFilter {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let messages = self.send_message(self.process_bloom_data(message_wrapper.messages)).await?;
        Ok(self.process_bloom_data( messages ) )
    }

    fn is_terminating(&self) -> bool {
        false
    }

}

#[cfg(test)]
mod test {

    use crate::transforms::cassandra::bloom_filter::CassandraAST;

    fn testAST() {
        let ast = CassandraAST::new( "SELECT foo from bar.baz where fu='something'".to_string() );
        assert_eq!( "bar.baz", ast.get_table_name() );
    }
}