//use reduce::Reduce;
use crate::error::ChainResponse;
use crate::message;
use crate::message::{Message, Messages};
use crate::transforms::{Transform, Transforms, Wrapper};
use async_trait::async_trait;
use bloomfilter::bloomfilter::bitmap_producer::BitMapProducer;
use bloomfilter::bloomfilter::hasher::{Hasher, HasherCollection, HasherType, SimpleHasher};
use bloomfilter::bloomfilter::{BloomFilter, BloomFilterType, Shape, Simple};
use bytes::Buf;
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, ErrorBody};
use cassandra_protocol::frame::frame_result::ColType::Udt;
use cassandra_protocol::frame::{Frame, StreamId, Version};
use cassandra_protocol::query::QueryParams;
use metrics::{counter, register_counter, Unit};
use murmur3::murmur3_x64_128;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::LowerHex;
use std::io::Cursor;
use std::iter::Map;
use std::string::FromUtf8Error;
use std::time::Duration;

use crate::codec::cassandra::CassandraCodec;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, CQL};
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tracing::{info, trace};
use tree_sitter::{
    Language, LogType, Node, Parser, Query, QueryCapture, QueryCursor, QueryMatch, Tree, TreeCursor,
};

//use proc_macro::TokenTree;
use crate::frame::CassandraOperation::Error;
use crate::transforms::cassandra::cassandra_ast::{
    CassandraAST, CassandraStatement, Named, Operand, OrderClause, RelationElement,
    RelationOperator, SelectElement, SelectStatementData,
};

/// The configuration for a single bloom filter in a single table.
#[derive(Deserialize, Debug, Clone)]
pub struct CassandraBloomFilterTableConfig {
    #[serde(rename = "remote_address")]
    /// keyspace for the table name
    pub keyspace: String,
    /// the table name holding the bloom filter
    pub table: String,
    /// the column name for the bloom filter
    pub bloom_column: String,
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
    //outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec>>,
    /// the name of the chain
    chain_name: String,
    /// a mapping of fully qualified table names to configurations.
    /// Fully qualified => keyspace . table
    tables: HashMap<String, CassandraBloomFilterTableConfig>,
    /// a mapping of messages by stream id.
    messages: HashMap<StreamId, BloomFilterState>,
}

impl Clone for CassandraBloomFilter {
    fn clone(&self) -> Self {
        CassandraBloomFilter::new(&self.tables, self.chain_name.clone())
    }
}

impl CassandraBloomFilter {
    pub fn new(
        tables: &HashMap<String, CassandraBloomFilterTableConfig>,
        chain_name: String,
    ) -> CassandraBloomFilter {
        let sink_single = CassandraBloomFilter {
            chain_name: chain_name.clone(),
            tables: tables.clone(),
            messages: Default::default(),
        };
        //register_counter!("failed_requests", Unit::Count, "chain" => chain_name, "transform" => sink_single.get_name());

        sink_single
    }

    fn get_name(&self) -> &'static str {
        "CassandraBloomFilter"
    }

    /// Create the hasher for a string value
    fn make_hasher(value: String) -> HasherType {
        let hash_result = murmur3_x64_128(&mut Cursor::new(value), 0).unwrap();
        let imask: i64 = -1;
        let mask: u64 = imask as u64;
        let upper: u64 = (hash_result >> 64) as u64;
        let initial = (upper & mask) as u64;
        let incr = (hash_result & (mask as u128)) as u64;
        SimpleHasher::new(initial, incr)
    }

    /// encode the bloom filter for a blob colum in Cassandra
    fn make_filter_column(bloomFilter: Box<dyn BloomFilter>) -> String {
        let bitmaps = bloomFilter.get_bitmaps();

        let mut parts: Vec<String> = Vec::with_capacity(bitmaps.len() + 1);
        parts.push("0x".to_string());
        for word in bitmaps {
            parts.push(format!("{:X}", word))
        }
        parts.join("")
    }

    fn make_response(&self, state: &mut BloomFilterState, new_query: String) -> Option<Message> {
        let mut message = state.original_msg.clone().unwrap();
        let original_frame = message.frame().unwrap().clone().into_cassandra().unwrap();
        let cassandra_frame = CassandraFrame {
            version: Version::V3,
            stream_id: original_frame.stream_id,
            tracing_id: original_frame.tracing_id,
            warnings: original_frame.warnings.clone(),
            operation: CassandraOperation::Query {
                query: CQL::parse_from_string(new_query),
                params: Default::default(),
            },
        };
        let frame = crate::frame::Frame::Cassandra(cassandra_frame);
        Some(Message::from_frame(frame))
    }

    /// process the query message.  This method has to handle all possible Queries that could
    /// impact the Bloom filter.
    fn process_query(self, state: &mut BloomFilterState) -> Option<Message> {
        match &state.statement {
            CassandraStatement::SelectStatement(statement_data) => {
                let cfg = self.tables.get(&statement_data.table_name);
                match cfg {
                    None => {
                        state.ignore = true;
                        None
                    }
                    Some(_) => {
                        let select_data = statement_data.clone();
                        let result =
                            CassandraBloomFilter::process_select(state, cfg.unwrap(), &select_data);
                        if result.is_err() {
                            self.make_error(state, result.err().unwrap())
                        } else {
                            match result.unwrap() {
                                Some(new_select) => {
                                    self.make_response(state, new_select.to_string())
                                }
                                None => None,
                            }
                        }
                    }
                }
            }
            CassandraStatement::UseStatement(keyspace) => {
                state.default_keyspace = Some(keyspace.clone());
                None
            }
            /*  InsertStatement => self.process_insert(ast, state),
            DeleteStatement => self.process_delete(ast, state),
            UseStatement => { self.process_use( ast, state); None},*/
            _ => {
                state.ignore = true;
                None
            }
        }
    }
    /*
        fn process_insert(&self, ast : & CassandraAST, state : &mut BloomFilterState ) -> Option<Message> {
            let cfg = self.check_table_name( ast, state );
            match cfg {
                None => None,
                Some(config) => {
                    // we need to determine if the bloom filter is being updated if any of the
                    // affected nodes are also updated.
                    // this is not quite accurate as we do not deal with simultaneous updates
                    let nodes: Box<Vec<Node>> = ast.search( "insert_column_spec / column_list / column" );
                    if nodes.is_empty() {
                        return self.make_error( state, "columns must be specified in inserts for Bloom filter based tables");
                    }
                    // the names of the column
                    let columns : Vec<String> = nodes.iter().map( |n| ast.node_text(n) ).collect();
                    let some_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a || b)?;
                    let all_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a && b)?;
                    let has_filter = columns.contains( &config.bloom_column);
                    // cases to check
                    // none of the columns -> ok
                    // has filter -> ok
                    // some but not all columns  -> error
                    // all columns and not bloom filter -> add bloom filter
                    // all columns and bloom filter -> ok
                    if !some_columns || has_filter {
                        state.ignore = true;
                        return None
                    }

                    // some but not all and no filter
                    if !all_columns {
                        return self.make_error( state, "modifying some but not all bloom filter columns requires updating filter" );
                    }
                    // all the columns in the filter are being inserted and there is no bloom filter provided
                    // so build the filter
                    let insert_values_spec = ast.search( "insert_values_spec" );

                        let insert_values_spec_node = insert_values_spec.get(0).unwrap();
                        if insert_values_spec_node.child(0).unwrap().kind().eq("JSON") {
                            return self.make_error(state, "column values may not be set with JSON in inserts for Bloom filter based tables");
                        }

                    let mut hashers = vec!();
                    //let mut hashers = Box::new(HasherCollection::new());

                    // assignment_map|assignment_list|assignment_set|assignment_tuple

                    let expressions: Box<Vec<Node>> = CassandraAST::search_node(insert_values_spec_node, "expression_list / expression" );

                    // there should be one expression for each column
                    if expressions.len() != columns.len() {
                        return self.make_error( state, "not enough values for the columns");
                    }
                    for i in 0..expressions.len() {
                        if config.columns.contains( columns.get(i).unwrap() ) {
                            let expr = expressions.get(i).unwrap();
                            if CassandraAST::has_node(expr, "assignment_map|assignment_list|assignment_set|assignment_tuple") {
                                return self.make_error( state, "assignment maps, lists, sets or tuples may not be used to specify values for bloom filter columns.");
                            }
                            hashers.push( CassandraBloomFilter::make_hasher( ast.node_text( expr )));
                        }
                    }
                    let shape = Shape {
                        m: config.bits,
                        k: config.funcs,
                    };
                    //let hasher:HasherType = Box::new(hashers);
                    let mut filter = Simple::empty_instance( &shape );
                    for hasher in hashers {
                        filter.merge_hasher_in_place( &hasher );
                    }
                    state.added_values.push( (config.bloom_column.clone(), CassandraBloomFilter::as_hex( &filter )));
                    // TODO continue implementation
                    None
                }
            }
        }
    */
    /*fn check_table_name( &self, ast : & CassandraAST, state : &mut BloomFilterState ) -> Option<&CassandraBloomFilterTableConfig> {
            let table_name = ast.get_table_name( &state.default_keyspace);
            let cfg = self.tables.get( table_name.as_str() );
            state.ignore = cfg.is_none();
            cfg
        }
    */
    /*
        fn process_delete( &self, ast : & CassandraAST, state : &mut BloomFilterState ) -> Option<Message> {
            let cfg = self.check_table_name( ast, state );
            match cfg {
                None => None,
                Some(config) => {
                    // we need to determine if bloom filter columns are being deleted and the
                    // bloom filter not updated
                    let nodes: Box<Vec<Node>> = ast.search( "delete_column_list / column" );
                    let columns : Vec<String> = nodes.iter().map( |n| ast.node_text(n) ).collect();
                    let some_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a || b)?;
                    let all_columns : bool = columns.iter().map( |c| config.columns.contains( c )).reduce( |a,b|  a && b)?;
                    let has_filter = columns.contains( &config.bloom_column);
                    // cases to check
                    // none of the columns -> ok
                    // some of the columns and bloom filter -> ok
                    // otherwise error
                    if !some_columns || has_filter {
                        return None
                    }
                    return self.make_error( state, "deleting bloom filter columns requires deleting filter");
                }
            }
        }

        fn process_use(&self, ast: & CassandraAST, state : &mut BloomFilterState) {
            // structure should be (source_file (use))
            let keyspace = ast.search( "keyspace");
            state.default_keyspace = Some(ast.node_text( keyspace.get(0).unwrap()) );
        }
    */
    fn make_error(&self, state: &mut BloomFilterState, message: String) -> Option<Message> {
        let mut msg = state.original_msg.as_ref().unwrap().clone();
        let mut cassandra_frame = msg.frame().unwrap().clone().into_cassandra().unwrap();
        cassandra_frame.operation = Error(ErrorBody {
            error_code: 0x2200,
            message,
            additional_info: AdditionalErrorInfo::Invalid,
        });
        let frame = crate::frame::Frame::Cassandra(cassandra_frame);
        let mut new_msg = Message::from_frame(frame);
        new_msg.meta_timestamp = msg.meta_timestamp;
        new_msg.return_to_sender = true;
        Some(new_msg)
    }

    fn process_select(
        state: &mut BloomFilterState,
        config: &CassandraBloomFilterTableConfig,
        select_data: &SelectStatementData,
    ) -> Result<Option<SelectStatementData>, String> {
        // we need to
        // remove any where clauses that have the bloom filter columns
        // build a bloom filter with the values
        // insert the bloom filter where clause into the query
        // add the where clause bloom filter columns to the select so we can filter later

        // the columns we are interested in are in the where clause
        let column_names = select_data.where_columns();
        // get a list of all the column names.
        // see if some of the columns are in the bloom filter configuration.
        let some_columns: bool = column_names
            .iter()
            .map(|c| config.columns.contains(c))
            .reduce(|a, b| a || b)
            .unwrap();

        if some_columns {
            // hashers for building bloom filter
            let mut hashers = vec![];

            // There is a weird case where the bloom filter is provided along with the search values
            // we will assume that the user knows what they want and not touch the filter but will
            // remove the columns from the query so the query will execute.  We will add the columns
            // to the selected columns
            let has_filter = column_names.contains(&config.bloom_column);

            // create a list of columns in the where clause that match the columns we are interested in
            let interesting_columns: Vec<&String> = column_names
                .iter()
                .filter(|n| config.columns.contains(n))
                .collect();

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

            if select_data.where_clause.is_some()
                && select_data
                    .where_clause
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|re| match &re.obj {
                        Operand::LIST(name) => true,
                        _ => false,
                    })
                    .reduce(|a, b| (a | b))
                    .unwrap()
            {
                return Err(
                    "bloom filter processing does not yet support (column...) = (values...)"
                        .to_string(),
                );
            }

            let interesting_relations: Vec<&RelationElement> = if select_data.where_clause.is_some()
            {
                select_data
                    .where_clause
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter(|re| match &re.obj {
                        Operand::COLUMN(name) => !(has_filter && name.eq(&config.bloom_column)),
                        _ => false,
                    })
                    .collect()
            } else {
                vec![]
            };

            state.ignore = false;
            let selected_columns = select_data.select_names();
            for relation in interesting_relations {
                if !has_filter
                    && match relation.oper {
                        RelationOperator::EQ => false,
                        _ => true,
                    }
                {
                    return Err(
                        "only equality checks are allowed if bloom filter is not provided"
                            .to_string(),
                    );
                }

                // add the relation to the added selects
                match &relation.obj {
                    Operand::COLUMN(name) => {
                        state.added_selects.push(SelectElement::COLUMN(Named {
                            name: name.clone(),
                            alias: None,
                        }));
                        if !has_filter {
                            // only get here if we have equality statement and no filter defined
                            for x in &relation.value {
                                hashers.push(CassandraBloomFilter::make_hasher(x.to_string()));
                            }
                        }
                    }
                    _ => (),
                }
                state.verify_funcs.push(relation.clone())
            }
            // remove duplicate select columns
            state
                .added_selects
                .retain(|s| !selected_columns.contains(&s.to_string()));

            // build the bloom filter
            if !has_filter {
                let shape = Shape {
                    m: config.bits,
                    k: config.funcs,
                };
                //let filter = Simple::from_hasher( &shape, &hashers );
                let mut filter = Simple::empty_instance(&shape);
                for hasher in hashers {
                    filter.merge_hasher_in_place(&hasher);
                }

                let where_stmt = RelationElement {
                    obj: Operand::COLUMN(config.bloom_column.clone()),
                    oper: RelationOperator::EQ,
                    value: {
                        let mut result = vec![];
                        result.push(Operand::CONST(CassandraBloomFilter::as_hex(&filter)));
                        result
                    },
                };
                state.added_where.push(where_stmt);
            }
            return Ok(Some(state.rewrite_select(select_data)));
        }
        state.ignore = true;
        return Ok(None);
    }

    fn as_hex(producer: &BloomFilterType) -> String {
        let mut hex = String::from("0X");
        producer
            .get_bitmaps()
            .iter()
            .for_each(|u| hex.push_str(format!("{:016x}", u).as_str()));
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
    */
    fn process_result(&mut self, query_response: &CassandraResult, stream: &StreamId) {
        /*      let query_msg = self.messages.remove(stream);
             match query_msg  {
                 Some( old_msg ) =>match query_response.result.unwrap() {
                     Value::NamedRows(mut rows) => {
                         CassandraBloomFilter::remove_unwanted_data(&mut rows, &old_msg);
                     },
                     _ => {},
                 },
                 _ => {},
             }

        */
    }

    // modify the message if Bloom filter data is involved.
    fn process_bloom_data(&mut self, messages: Messages) -> Messages {
        /*
        let mut new_msgs: Messages = vec![];

        for mut msg in messages {
            let stream = match msg.stream_id() {
                Some(id) => id,
                None => {
                    info!("no cassandra frame found");
                    new_msgs.push(msg);
                    break;
                }
            };
            let frame = msg.frame().unwrap().clone().into_cassandra().unwrap();

            match frame.operation {

                CassandraOperation::Query{ query, params } => {
                    let ast = CassandraAST::new(query.to_query_string());
                    let mut state = BloomFilterState {
                        statement : ast.statement,
                        ignore : false,
                        original_msg: msg.clone(),
                        verify_funcs: vec![],
                        added_selects: vec![],
                        added_where: vec![],
                        added_values: vec![],
                        default_keyspace: None
                    };
                    match self.process_query( &ast, &mut state ) {
                        None => new_msgs.push( state.original_msg.clone() ),
                        Some(msg) => new_msgs.push( msg ),
                    }
                    self.messages.insert(stream, state);
                },
                CassandraOperation::Result(result) => {
                    self.process_result(& result, &stream);
                    new_msgs.push(msg.clone());
                },
                _ => new_msgs.push(msg.clone()),
            };
        }
        new_msgs

         */
        messages
    }

    /*
        async fn send_message(&mut self, messages: Messages) -> ChainResponse {
            loop {
                match self.outbound {
                    None => {
                        trace!("creating outbound connection {:?}", self.address);
                        let mut conn_pool = ConnectionPool::new(
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
    */
}

#[derive(Debug, Clone)]
pub struct BloomFilterState {
    statement: CassandraStatement,
    ignore: bool,
    original_msg: Option<Message>,
    verify_funcs: Vec<RelationElement>,
    added_selects: Vec<SelectElement>,
    added_where: Vec<RelationElement>,
    added_values: Vec<(String, String)>,
    default_keyspace: Option<String>,
}

impl BloomFilterState {
    /// rewrite the select statement data as a _NEW_ select statement data object.
    pub fn rewrite_select(&self, select_data: &SelectStatementData) -> SelectStatementData {
        let mut new_elements: Vec<SelectElement> = vec![];
        select_data
            .elements
            .iter()
            .for_each(|s| new_elements.push(s.clone()));
        self.added_selects
            .iter()
            .for_each(|s| new_elements.push(s.clone()));

        let mut new_where: Vec<RelationElement> = vec![];

        // add the where clauses that were not moved to verify_funcs.
        if select_data.where_clause.as_ref().is_some() {
            select_data
                .where_clause
                .as_ref()
                .unwrap()
                .iter()
                .filter(|f| !self.verify_funcs.contains(f))
                .for_each(|w| new_where.push(w.clone()));
        }
        self.added_where
            .iter()
            .for_each(|w| new_where.push(w.clone()));

        SelectStatementData {
            modifiers: select_data.modifiers.clone(),
            table_name: select_data.table_name.clone(),
            elements: new_elements,
            where_clause: if new_where.is_empty() {
                None
            } else {
                Some(new_where)
            },
            order: match &select_data.order {
                None => None,
                Some(x) => Some(x.clone()),
            },
        }
    }
}

/*
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
*/
#[cfg(test)]
mod test {
    use crate::frame::CassandraOperation::Query;
    use crate::frame::Frame::Cassandra;
    use crate::frame::{CassandraFrame, CassandraOperation, CQL};
    use crate::message::Message;
    use crate::transforms::cassandra::bloom_filter::{
        BloomFilterState, CassandraBloomFilter, CassandraBloomFilterTableConfig,
    };
    use crate::transforms::cassandra::cassandra_ast::CassandraStatement::SelectStatement;
    use crate::transforms::cassandra::cassandra_ast::{
        CassandraAST, CassandraStatement, Named, Operand, RelationElement, RelationOperator,
        SelectElement, SelectStatementData,
    };
    use cassandra_protocol::frame::Version;
    use itertools::Itertools;
    use reduce::Reduce;
    use std::collections::HashMap;

    pub fn build_message(select_stmt: &String) -> Message {
        let frame = CassandraFrame {
            version: Version::V3,
            stream_id: 10,
            tracing_id: None,
            warnings: vec![],
            operation: Query {
                query: CQL::parse_from_string(select_stmt.clone()),
                params: Default::default(),
            },
        };
        Message::from_frame(Cassandra(frame))
    }

    fn build_state(ast: &CassandraAST, msg: Message) -> BloomFilterState {
        BloomFilterState {
            statement: ast.statement.clone(),
            ignore: false,
            original_msg: Some(msg),
            verify_funcs: vec![],
            added_selects: vec![],
            added_where: vec![],
            added_values: vec![],
            default_keyspace: None,
        }
    }

    fn get_select_result(
        ast: &CassandraAST,
        state: &mut BloomFilterState,
        config: &CassandraBloomFilterTableConfig,
    ) -> Option<SelectStatementData> {
        let result = match &ast.statement {
            SelectStatement(select_data) => {
                CassandraBloomFilter::process_select(state, &config, &select_data)
            }
            _ => panic!("Not a select statement"),
        };
        if result.is_err() {
            assert!(false, "{}", result.err().unwrap().to_string());
            None
        } else {
            result.unwrap()
        }
    }
    #[test]
    pub fn test_process_select() {
        let config = CassandraBloomFilterTableConfig {
            keyspace: "".to_string(),
            table: "myTable".to_string(),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };

        let select_stmt = "SELECT foo FROM myTable WHERE bfCol1 = 'bar'".to_string();
        let ast = CassandraAST::new(select_stmt.clone());

        let msg = build_message(&select_stmt);
        let mut state = build_state(&ast, msg);

        let result = get_select_result(&ast, &mut state, &config);

        let expected_select = SelectElement::COLUMN(Named {
            name: "bfCol1".to_string(),
            alias: None,
        });

        let expected_verify = RelationElement {
            obj: Operand::COLUMN("bfCol1".to_string()),
            oper: RelationOperator::EQ,
            value: {
                let mut v = vec![];
                v.push(Operand::CONST("'bar'".to_string()));
                v
            },
        };

        match result {
            None => assert!(false),
            Some(statement_data) => {
                assert!(!state.ignore);
                assert_eq!(1, state.added_selects.len());
                assert!(state.added_selects.contains(&expected_select));
                assert_eq!(1, state.verify_funcs.len());
                assert!(state.verify_funcs.iter().contains(&expected_verify));
                assert_eq!(1, state.added_where.len());
                assert_eq!(
                    config.bloom_column.as_str(),
                    state.added_where.get(0).unwrap().obj.to_string().as_str()
                );
                let stmt = statement_data.to_string();
                assert_eq!( "SELECT foo, bfCol1 FROM myTable WHERE filterColumn = 0X02000000000020000000000000000080", stmt.as_str());
            }
        }

        // verify original was not changed
        let mut msg = state.original_msg.as_ref().unwrap().clone();
        let mut cassandra_frame = msg.frame().unwrap().clone().into_cassandra().unwrap();

        match cassandra_frame.operation {
            CassandraOperation::Query { query, params } => {
                assert_eq!(select_stmt, query.to_query_string());
            }
            _ => assert!(false),
        }
    }

    #[test]
    pub fn test_process_select_with_bloom_filter() {
        let config = CassandraBloomFilterTableConfig {
            keyspace: "".to_string(),
            table: "myTable".to_string(),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };

        let select_stmt = "SELECT foo FROM myTable WHERE bfCol1 <> 'bar' and filterColumn = 0X02000000000020000000000000000080".to_string();
        let ast = CassandraAST::new(select_stmt.clone());

        let msg = build_message(&select_stmt);
        let mut state = build_state(&ast, msg);

        let result = get_select_result(&ast, &mut state, &config);

        let expected_select = SelectElement::COLUMN(Named {
            name: "bfCol1".to_string(),
            alias: None,
        });

        let expected_verify = RelationElement {
            obj: Operand::COLUMN("bfCol1".to_string()),
            oper: RelationOperator::NE,
            value: {
                let mut v = vec![];
                v.push(Operand::CONST("'bar'".to_string()));
                v
            },
        };

        match result {
            None => assert!(false),
            Some(statement_data) => {
                assert!(!state.ignore);
                assert_eq!(1, state.added_selects.len());
                assert!(state.added_selects.contains(&expected_select));
                assert_eq!(1, state.verify_funcs.len());
                assert!(state.verify_funcs.iter().contains(&expected_verify));
                assert_eq!(0, state.added_where.len());
                let stmt = statement_data.to_string();
                assert_eq!( "SELECT foo, bfCol1 FROM myTable WHERE filterColumn = 0X02000000000020000000000000000080", stmt.as_str());
            }
        }
    }

    #[test]
    pub fn test_process_select_without_filter_columns() {
        let config = CassandraBloomFilterTableConfig {
            keyspace: "".to_string(),
            table: "myTable".to_string(),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };

        let select_stmt = "SELECT foo FROM myTable WHERE junk <> 'bar'".to_string();
        let ast = CassandraAST::new(select_stmt.clone());

        let msg = build_message(&select_stmt);
        let mut state = build_state(&ast, msg);

        let result = get_select_result(&ast, &mut state, &config);

        match result {
            None => assert_eq!(true, state.ignore),
            Some(statement_data) => assert!(false),
        }
    }
}
