//use reduce::Reduce;
use crate::error::ChainResponse;
use crate::message;
use crate::message::{Message, MessageValue, Messages};
use crate::transforms::{Transform, Transforms, Wrapper};
use async_trait::async_trait;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use bloomfilter::bloomfilter::bitmap_producer::BitMapProducer;
use bloomfilter::bloomfilter::hasher::{Hasher, HasherCollection, HasherType, SimpleHasher};
use bloomfilter::bloomfilter::{BloomFilter, BloomFilterType, Shape, Simple};
use bytes::{Buf, Bytes};
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, ErrorBody};
use cassandra_protocol::frame::frame_result::ColType::Udt;
use cassandra_protocol::frame::frame_result::{ColSpec, RowsMetadata};
use cassandra_protocol::frame::{Frame, StreamId, Version};
use cassandra_protocol::query::QueryParams;
use itertools::Itertools;
use metrics::{counter, register_counter, Unit};
use murmur3::murmur3_x64_128;
use ordered_float::OrderedFloat;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::LowerHex;
use std::io::Cursor;
use std::iter::Map;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::string::FromUtf8Error;
use std::time::Duration;
use tls_parser::nom::AsBytes;

use crate::codec::cassandra::CassandraCodec;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, CQL};
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tracing::{info, trace};
use tree_sitter::{
    Language, LogType, Node, Parser, Query, QueryCapture, QueryCursor, QueryMatch, Tree, TreeCursor,
};
use uuid::Uuid;

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
    fn make_filter_column(bloom_filter: Box<dyn BloomFilter>) -> String {
        let bitmaps = bloom_filter.get_bitmaps();

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
    fn process_query(&self, state: &mut BloomFilterState) -> Option<Message> {
        match &state.statement {
            CassandraStatement::SelectStatement(statement_data) => {
                let table_name = state.fixup_table_name(statement_data.table_name.clone());
                let cfg = self.tables.get(&table_name);
                match cfg {
                    None => {
                        state.ignore = true;
                        None
                    }
                    Some(config) => {
                        let mut select_data = statement_data.clone();
                        select_data.table_name = table_name;
                        let result =
                            CassandraBloomFilter::process_select(state, config, &select_data);
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

            if select_data.where_clause.is_some()
                && select_data
                    .where_clause
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|re| match &re.obj {
                        Operand::LIST(_) => true,
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
            if select_data.elements.contains(&SelectElement::STAR) {
                state.added_selects.truncate(0);
            } else {
                state
                    .added_selects
                    .retain(|s| !selected_columns.contains(&s.to_string()));
            }
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

    /// process a result message
    fn process_result(
        &mut self,
        query_response: &CassandraResult,
        state: &mut BloomFilterState,
    ) -> Option<CassandraResult> {
        match query_response {
            CassandraResult::Rows { value, metadata } => {
                CassandraBloomFilter::process_rows(value, metadata, state)
            }
            CassandraResult::SetKeyspace(keyspace) => {
                state.default_keyspace = Some(keyspace.body.clone());
                None
            }
            CassandraResult::Prepared(_)
            | CassandraResult::SchemaChange(_)
            | CassandraResult::Void => None,
        }
    }

    /// process the rows.
    fn process_rows(
        value: &MessageValue,
        metadata: &RowsMetadata,
        state: &mut BloomFilterState,
    ) -> Option<CassandraResult> {
        let added_columns = state.get_added_column_names();

        let final_columns: Vec<bool> = metadata
            .col_specs
            .iter()
            .map(|cs| !added_columns.contains(&cs.name))
            .collect();

        let mut new_metadata = if added_columns.is_empty() {
            metadata.clone()
        } else {
            RowsMetadata {
                flags: metadata.flags,
                columns_count: metadata.columns_count - (added_columns.len() as i32),
                paging_state: metadata.paging_state.clone(),
                global_table_spec: metadata.global_table_spec.clone(),
                col_specs: metadata.col_specs.clone(),
            }
        };

        let mut iter = final_columns.iter();
        new_metadata.col_specs.retain(|_| *iter.next().unwrap());

        let new_value = match value {
            MessageValue::Rows(rows) => {
                let mut verify_map: Vec<(usize, &RelationElement)> = vec![];
                state
                    .verify_funcs
                    .iter()
                    .map(|re| {
                        let name = match &re.obj {
                            Operand::COLUMN(name) => name,
                            _ => unreachable!(),
                        };
                        for i in 0..metadata.columns_count {
                            let colspec: &ColSpec = metadata.col_specs.get(i as usize).unwrap();
                            if colspec.name.eq(name) {
                                return (i as usize, re);
                            }
                        }
                        unreachable!()
                    })
                    .for_each(|(x, y)| verify_map.push((x, y)));
                let mut filtered_rows = vec![];
                rows.iter()
                    .filter(|vr| {
                        for (idx, relation_element) in &verify_map {
                            match vr.get(*idx) {
                                Some(v) => {
                                    if !CassandraBloomFilter::verify(v, relation_element) {
                                        return false;
                                    }
                                }
                                None => unreachable!(),
                            }
                        }
                        return true;
                    })
                    .for_each(|v| {
                        let mut v2 = v.clone();
                        let mut iter = final_columns.iter();
                        v2.retain(|_| *iter.next().unwrap());
                        filtered_rows.push(v2);
                    });
                MessageValue::Rows(filtered_rows)
            }

            MessageValue::NamedRows(named_rows) => {
                let mut filtered_rows = vec![];
                named_rows
                    .iter()
                    .filter(|bt| {
                        // if there is a verify func for the column execute it.
                        for vf in &state.verify_funcs {
                            let name = match &vf.obj {
                                Operand::COLUMN(name) => name,
                                _ => unreachable!(),
                            };
                            if bt.contains_key(name) {
                                if !CassandraBloomFilter::verify(bt.get(name).unwrap(), &vf) {
                                    return false;
                                }
                            } else {
                                // if the value does not exist then check fails.
                                return false;
                            }
                        }
                        return true;
                    })
                    .for_each(|t| {
                        let mut bt = t.clone();
                        for col in &added_columns {
                            bt.remove(col.as_str());
                        }
                        filtered_rows.push(bt);
                    });
                MessageValue::NamedRows(filtered_rows)
            }
            _ => unreachable!(),
        };
        // put new values into result
        Some(CassandraResult::Rows {
            value: new_value,
            metadata: new_metadata,
        })
    }

    // verify that the relaiton matches
    fn verify(value: &MessageValue, relation_element: &RelationElement) -> bool {
        /* we only support "column operator value"  So any operations that expect anything other
        than a single value we can fail
         */

        let relation_value = relation_element.first_value().to_string();

        // convert relation_element message value  Can not use from as relation element does not
        // have enough data.
        match value {
            MessageValue::Ascii(str) | MessageValue::Strings(str) | MessageValue::Varchar(str) => {
                relation_element.oper.eval(str, &relation_value)
            }

            MessageValue::Bytes(b) => {
                let v = Bytes::copy_from_slice(relation_value.as_bytes());
                relation_element.oper.eval(b, &v)
            }

            MessageValue::Integer(i, _)
            | MessageValue::Timestamp(i)
            | MessageValue::Time(i)
            | MessageValue::Counter(i) => match relation_value.parse::<i64>() {
                Ok(v) => relation_element.oper.eval(i,&v),
                Err(e) => false,
            },

            MessageValue::Double(d) => match relation_value.parse::<f64>() {
                Ok(v) => relation_element.oper.eval(d, &OrderedFloat(v)),
                Err(e) => false,
            },

            MessageValue::Float(f) => match relation_value.parse::<f32>() {
                Ok(v) => relation_element.oper.eval(f, &OrderedFloat(v)),
                Err(e) => false,
            },

            MessageValue::Boolean(b) => {
                let v = relation_value.to_uppercase().eq("TRUE");
                relation_element.oper.eval(b,&v)
            }

            MessageValue::Inet(ipaddr) => match IpAddr::from_str(relation_value.as_str()) {
                Ok(v) => relation_element.oper.eval(ipaddr,&v),
                Err(e) => false,
            },

            MessageValue::Varint(i) => match relation_value.parse::<BigInt>() {
                Ok(v) => relation_element.oper.eval(i,&v),
                Err(e) => false,
            },

            MessageValue::Decimal(d) => match relation_value.parse::<BigDecimal>() {
                Ok(v) => relation_element.oper.eval(d,&v),
                Err(e) => false,
            },

            MessageValue::Date(i) => match relation_value.parse::<i32>() {
                Ok(v) => relation_element.oper.eval(i,&v),
                Err(e) => false,
            },

            MessageValue::Timeuuid(u) | MessageValue::Uuid(u) => {
                match Uuid::parse_str(relation_value.as_str()) {
                    Ok(v) => relation_element.oper.eval(u, &v),
                    Err(e) => false,
                }
            }

            // unsupported  types
            /*
                MessageValue::NULL |
                MessageValue::None |
                MessageValue::List(_) |
                MessageValue::Rows(_) |
                MessageValue::NamedRows(_) |
                MessageValue::Document(_) |
                MessageValue::FragmentedResponse(_) ||
                MessageValue::Set(_) ||
                MessageValue::Map(_) ||
                MessageValue::Tuple(_) ||
                MessageValue::Udt(_) => false,
            */
            _ => false,
        }
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
    /// create a new BloomFilterState
    pub fn new(ast: &CassandraAST, msg: Option<Message>) -> BloomFilterState {
        BloomFilterState {
            statement: ast.statement.clone(),
            ignore: false,
            original_msg: msg,
            verify_funcs: vec![],
            added_selects: vec![],
            added_where: vec![],
            added_values: vec![],
            default_keyspace: None,
        }
    }

    /// converts to lower case and adds default_keyspace if defined and keyspace not specified.
    pub fn fixup_table_name(&self, table_name: String) -> String {
        if self.default_keyspace.is_none() || table_name.contains(".") {
            table_name.to_lowercase()
        } else {
            format!("{}.{}", self.default_keyspace.as_ref().unwrap(), table_name).to_lowercase()
        }
    }

    /// the column names as returned by a query for the columns we added to the query.
    pub fn get_added_column_names(&self) -> Vec<String> {
        self.added_selects
            .iter()
            .map(|se| match se {
                SelectElement::COLUMN(named) => named.alias_or_name(),
                _ => unreachable!(),
            })
            .collect()
    }

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

        let mut result = SelectStatementData {
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
        };
        result.modifiers.limit = None;
        result
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
    use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, CQL};
    use crate::message::{IntSize, Message, MessageValue};
    use crate::transforms::cassandra::bloom_filter::{
        BloomFilterState, CassandraBloomFilter, CassandraBloomFilterTableConfig,
    };
    use crate::transforms::cassandra::cassandra_ast::CassandraStatement::SelectStatement;
    use crate::transforms::cassandra::cassandra_ast::{
        CassandraAST, CassandraStatement, Named, Operand, RelationElement, RelationOperator,
        SelectElement, SelectStatementData,
    };
    use cassandra_protocol::frame::frame_result::{
        ColSpec, ColType, ColTypeOption, RowsMetadata, RowsMetadataFlags,
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

    #[test]
    pub fn test_use_modifies_select() {
        let select_stmt = "use HELLO".to_string();
        let ast = CassandraAST::new(select_stmt.clone());

        let mut tables: HashMap<String, CassandraBloomFilterTableConfig> = HashMap::new();
        tables.insert(
            "hello.mytable".to_string(),
            CassandraBloomFilterTableConfig {
                keyspace: "".to_string(),
                table: "myTable".to_string(),
                bloom_column: "filterColumn".to_string(),
                bits: 72,
                funcs: 3,
                columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
            },
        );

        let filter = CassandraBloomFilter::new(&tables, "testing_chain".to_string());

        let mut state = BloomFilterState::new(&ast, None);

        let msg = filter.process_query(&mut state);
        assert_eq!(None, msg);
        assert_eq!(
            "hello.mytable",
            state.fixup_table_name("MyTable".to_string())
        );

        let select_stmt = "select * from mytable where bfCol1 = 'foo'".to_string();
        let ast = CassandraAST::new(select_stmt.clone());
        state.statement = ast.statement.clone();
        state.original_msg = Some(build_message(&select_stmt));

        let msg = filter.process_query(&mut state);
        match msg {
            Some(mut m) => {
                let y = m.frame().unwrap().clone().into_cassandra().unwrap();
                let z = match y.operation {
                    CassandraOperation::Query { query, params } => query.to_query_string(),
                    _ => "ERROR".to_string(),
                };
                let new_ast = CassandraAST::new(z);
                assert!(!new_ast.has_error());
                assert_eq!(
                    "SELECT * FROM hello.mytable WHERE filterColumn = 0X0200000008000040",
                    new_ast.statement.to_string()
                );
            }
            None => assert!(false),
        }
    }

    #[test]
    pub fn test_limit_removed_when_select_processed() {
        let select_stmt = "use HELLO".to_string();
        let ast = CassandraAST::new(select_stmt.clone());

        let mut tables: HashMap<String, CassandraBloomFilterTableConfig> = HashMap::new();
        tables.insert(
            "hello.mytable".to_string(),
            CassandraBloomFilterTableConfig {
                keyspace: "".to_string(),
                table: "myTable".to_string(),
                bloom_column: "filterColumn".to_string(),
                bits: 72,
                funcs: 3,
                columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
            },
        );

        let filter = CassandraBloomFilter::new(&tables, "testing_chain".to_string());

        let mut state = BloomFilterState::new(&ast, None);

        let msg = filter.process_query(&mut state);
        assert_eq!(None, msg);
        assert_eq!(
            "hello.mytable",
            state.fixup_table_name("MyTable".to_string())
        );

        let select_stmt = "select * from mytable where bfCol1 = 'foo' LIMIT 5".to_string();
        let ast = CassandraAST::new(select_stmt.clone());
        state.statement = ast.statement.clone();
        state.original_msg = Some(build_message(&select_stmt));

        let msg = filter.process_query(&mut state);
        match msg {
            Some(mut m) => {
                let y = m.frame().unwrap().clone().into_cassandra().unwrap();
                let z = match y.operation {
                    CassandraOperation::Query { query, params } => query.to_query_string(),
                    _ => "ERROR".to_string(),
                };
                let new_ast = CassandraAST::new(z);
                assert!(!new_ast.has_error());
                assert_eq!(
                    "SELECT * FROM hello.mytable WHERE filterColumn = 0X0200000008000040",
                    new_ast.statement.to_string()
                );
            }
            None => assert!(false),
        }
    }

    #[test]
    fn test_process_rows() {
        let mut values: Vec<Vec<MessageValue>> = vec![];
        let mut row = vec![];
        row.push(MessageValue::Integer(5, IntSize::I32));
        row.push(MessageValue::Strings("Hello".to_string()));
        values.push(row);
        let mut row = vec![];
        row.push(MessageValue::Integer(5, IntSize::I32));
        row.push(MessageValue::Strings("World".to_string()));
        values.push(row);

        let mut row = vec![];
        row.push(MessageValue::Integer(11, IntSize::I32));
        row.push(MessageValue::Strings("Goodbye".to_string()));
        values.push(row);
        let mut row = vec![];
        row.push(MessageValue::Integer(12, IntSize::I32));
        row.push(MessageValue::Strings("Cruel".to_string()));
        values.push(row);

        let message_value = MessageValue::Rows(values);

        let mut column_specs = vec![];
        column_specs.push(ColSpec {
            table_spec: None,
            name: "bfCol".to_string(),
            col_type: ColTypeOption {
                id: ColType::Int,
                value: None,
            },
        });

        let result_col_spec = ColSpec {
            table_spec: None,
            name: "word".to_string(),
            col_type: ColTypeOption {
                id: ColType::Ascii,
                value: None,
            },
        };

        column_specs.push(result_col_spec.clone());

        let orig_metadata = RowsMetadata {
            flags: RowsMetadataFlags::empty(),
            columns_count: 2,
            paging_state: None,
            global_table_spec: None,
            col_specs: column_specs,
        };

        let mut verify_funcs = vec![];
        verify_funcs.push(RelationElement {
            obj: Operand::COLUMN("bfCol".to_string()),
            oper: RelationOperator::LT,
            value: {
                let mut v = vec![];
                v.push(Operand::CONST("10".to_string()));
                v
            },
        });

        let mut added_selects = vec![];
        added_selects.push(SelectElement::COLUMN(Named {
            name: "bfCol".to_string(),
            alias: None,
        }));

        let mut state = BloomFilterState {
            statement: CassandraStatement::AlterKeyspace,
            ignore: false,
            original_msg: None,
            verify_funcs,
            added_selects,
            added_where: vec![],
            added_values: vec![],
            default_keyspace: None,
        };

        let result = CassandraBloomFilter::process_rows(&message_value, &orig_metadata, &mut state);
        match result {
            None => assert!(false),
            Some(result) => match result {
                CassandraResult::Rows { value, metadata } => {
                    assert_eq!(1, metadata.columns_count);
                    let col_spec = metadata.col_specs.get(0).unwrap();
                    assert_eq!(&result_col_spec, col_spec);

                    match value {
                        MessageValue::Rows(values) => {
                            assert_eq!(2, values.len());
                            let cols = values.get(0).unwrap();
                            assert_eq!(1, cols.len());
                            let col = cols.get(0).unwrap();
                            match col {
                                MessageValue::Strings(txt) => assert_eq!(&"Hello".to_string(), txt),
                                _ => assert!(false),
                            }
                            let cols = values.get(1).unwrap();
                            assert_eq!(1, cols.len());
                            let col = cols.get(0).unwrap();
                            match col {
                                MessageValue::Strings(txt) => assert_eq!(&"World".to_string(), txt),
                                _ => assert!(false),
                            }
                        }
                        _ => assert!(false),
                    }
                }
                CassandraResult::SetKeyspace(_)
                | CassandraResult::Prepared(_)
                | CassandraResult::SchemaChange(_)
                | CassandraResult::Void => assert!(false),
            },
        }
    }
}
