use crate::error::ChainResponse;
use crate::frame::cassandra::{CQLStatement, CassandraMetadata};
use crate::frame::CassandraOperation::Error;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame, CQL};
use crate::message::{Message, MessageValue, Messages, Metadata};
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use bloomfilter::bloomfilter::hasher::{HasherType, SimpleHasher};
use bloomfilter::bloomfilter::{BloomFilter, BloomFilterType, Shape, Simple};
use bytes::Bytes;
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, ErrorBody};
use cassandra_protocol::frame::frame_result::{ColSpec, RowsMetadata};
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Operand, OrderClause, RelationElement, RelationOperator};
use cql3_parser::insert::{Insert, InsertValues};
use cql3_parser::select::{Named, Select, SelectElement};
use itertools::Itertools;
use murmur3::murmur3_x64_128;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Cursor;
use std::net::IpAddr;
use std::str::FromStr;
use uuid::Uuid;
use crate::frame::Frame::Cassandra;


/*
Cassandra selects can only appear as a single statement in a single message.
Cassandra inserts, updates and deletes can appear together in a single message or in separate messages.

This transform has to selects, inserts, updates and deletes on the to the server, and modify
the results of selects on the way back.  Results will be per message.

Our process is:

create a list of Option<status> objects, one for each msg.

rewrite the message on the way up.
    create an Option<status> object for each message.
        if it was a select keep a copy of the original select message
        if it was not a select store None.

process the messages on the way back.
    for each response message where there is a Some<status> in the status list (matching by position)
        process the response message.

 */
/// The configuration for a single bloom filter in a single table.
#[derive(Deserialize, Debug, Clone)]
pub struct CassandraBloomFilterTableConfig {
    /// the table holding the bloom filter
    pub table: FQName,
    /// the column name for the bloom filter
    pub bloom_column: String,
    /// the number of bits in the bloom filter
    pub bits: usize,
    /// the number of functions in the bloom filter
    pub funcs: usize,
    /// the names of the columns that are added to the bloom filter
    pub columns: Vec<String>,
}

impl CassandraBloomFilterTableConfig {
    pub fn get_shape(&self) -> Shape {
        Shape {
            k: self.funcs,
            m: self.bits,
        }
    }

    pub fn normalize(&mut self) {
        self.table = MessageState::normalize_fqname( &self.table );
        self.bloom_column = self.bloom_column.to_lowercase();
        self.columns.iter_mut().for_each( |c| *c=c.to_lowercase());
    }
}

#[derive(Debug)]
pub struct CassandraBloomFilter {
    /// the outbound connection for this filter
    //outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec>>,
    /// the name of the chain
    chain_name: String,
    /// a mapping of fully qualified table names to configurations.
    /// Fully qualified => keyspace . table
    tables: HashMap<FQName, CassandraBloomFilterTableConfig>,
}

impl Clone for CassandraBloomFilter {
    fn clone(&self) -> Self {
        CassandraBloomFilter {
            chain_name: self.chain_name.clone(),
            tables: self.tables.clone(),
        }
    }
}

impl CassandraBloomFilter {
    pub fn new(
        configs: &[CassandraBloomFilterTableConfig],
        chain_name: &str,
    ) -> CassandraBloomFilter {
        let tables: HashMap<FQName, CassandraBloomFilterTableConfig> = configs
            .iter()
            .map(|cfg| {
                let mut value = cfg.clone();
                value.normalize();
                return (
                    value.table.clone(),
                    value
                );
            })
            .collect();
        CassandraBloomFilter {
            chain_name: chain_name.into(),
            tables,
        }
    }

    pub fn get_name(&self) -> &'static str {
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

    /// process the CQL structure from the message.  This method has to handle all possible Queries that could
    /// impact the Bloom filter.
    /// The result is a vector that matches the cql.statements vector.
    /// Resulting truth table
    ///
    /// |      | Some Msg                              | No Msg                                          |
    /// | ---- | ----                               | ----                                          |
    /// | Some State |  * An error message or; <br>* processed message | * A query message that is not processed on return |
    /// | No State |  #not used#                          | * Not a bloom filter affected query             |
    ///
    ///
    fn process_cql(
        &self,
        session_state: &mut SessionState,
        cassandra_metadata: &CassandraMetadata,
        timestamp: Option<i64>,
        cql: &mut CQL,
    ) -> Vec<(Option<Message>, Option<MessageState>)> {
        cql.statements
            .iter_mut()
            .map(|stmnt| {
                let CQLStatement { statement, .. } = stmnt;
                if let Some(raw_table_name) = CQLStatement::get_table_name(&statement) {
                    let table_name = session_state.fixup_table_name(raw_table_name);
                    if let Some(config) = self.tables.get(&table_name) {
                        match &statement {
                            CassandraStatement::Select(select) => {
                                let mut message_state = MessageState::new(
                                    &select,
                                    cassandra_metadata.clone(),
                                    timestamp,
                                );

                                message_state.table_name = table_name;
                                match CassandraBloomFilter::process_select(
                                    &mut message_state,
                                    config,
                                ) {
                                    Err(message) => {
                                        let mut msg = CassandraBloomFilter::make_message(
                                            &message_state.meta,
                                            message_state.timestamp,
                                            Error(ErrorBody {
                                                error_code: 0x2200,
                                                message,
                                                additional_info: AdditionalErrorInfo::Invalid,
                                            }),
                                        );
                                        msg.return_to_sender = true;
                                        (Some(msg), Some(message_state))
                                    }
                                    Ok(result) => match result {
                                        Some(new_select) => (
                                            Some(CassandraBloomFilter::make_message(
                                                &message_state.meta,
                                                message_state.timestamp,
                                                CassandraOperation::Query {
                                                    query: CQL::parse_from_string(
                                                        &new_select.to_string(),
                                                    ),
                                                    params: Default::default(),
                                                },
                                            )),
                                            Some(message_state),
                                        ),
                                        None => (None, Some(message_state)),
                                    },
                                }
                            }

                            CassandraStatement::Use(keyspace) => {
                                session_state.default_keyspace = Some(keyspace.clone());
                                (None, None)
                            }

                            CassandraStatement::Insert(insert) => {
                                match CassandraBloomFilter::process_insert(
                                    &insert, table_name, config,
                                ) {
                                    Err(msg) => {
                                        let mut new_msg = CassandraBloomFilter::make_message(
                                            cassandra_metadata,
                                            timestamp,
                                            Error(ErrorBody {
                                                error_code: 0x2200,
                                                message: msg,
                                                additional_info: AdditionalErrorInfo::Invalid,
                                            }),
                                        );
                                        new_msg.return_to_sender = true;
                                        (Some(new_msg), None)
                                    }
                                    Ok(Some(new_insert)) => (
                                        Some(CassandraBloomFilter::make_message(
                                            cassandra_metadata,
                                            timestamp,
                                            CassandraOperation::Query {
                                                query: CQL::parse_from_string(
                                                    &new_insert.to_string(),
                                                ),
                                                params: Default::default(),
                                            },
                                        )),
                                        None,
                                    ),
                                    Ok(None) => (None, None),
                                }
                            }

                            /* TODO add these additional statements
                            DeleteStatement => self.process_delete(ast, message_state),
                            UseStatement => { self.process_use( ast, message_state); None},
                            */
                            _ => (None, None),
                        }
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            })
            .collect_vec()
    }

    fn make_message(
        cassandra_metadata: &CassandraMetadata,
        timestamp: Option<i64>,
        operation: CassandraOperation,
    ) -> Message {
        let cassandra_frame = CassandraFrame {
            version: cassandra_metadata.version.clone(),
            stream_id: cassandra_metadata.stream_id.clone(),
            tracing_id: cassandra_metadata.tracing_id.clone(),
            warnings: vec![],
            operation,
        };
        let frame = crate::frame::Frame::Cassandra(cassandra_frame);
        let mut new_msg = Message::from_frame(frame);
        new_msg.meta_timestamp = timestamp;
        new_msg
    }

    /// processes the insert statement.  The resulting insert statement will be into the specified table_name and not
    /// what is in the insert statement.
    fn process_insert(
        insert: &Insert,
        table_name: FQName,
        config: &CassandraBloomFilterTableConfig,
    ) -> Result<Option<Insert>, String> {
        // missing columns are set to null

        // if the bloom filter column is inserted assume the client knows what they are doing.
        if insert.columns.contains(&config.bloom_column) {
            Ok(None)
        } else {
            let mut new_insert = insert.clone();
            new_insert.table_name = table_name;
            match &mut new_insert.values {
                InsertValues::Json(_) => Err("Column values may not be set with JSON in inserts for Bloom filter based tables".to_string()),
                InsertValues::Values(operands) => {
                    if operands.len() != insert.columns.len() {
                        Err("There must be one value for each column".to_string())
                    } else {
                        let hashers = insert.columns
                            .iter()
                            .zip(operands.iter_mut())
                            .filter_map(|(column, operand)| {
                                if config.columns.contains(column) {
                                    Some(CassandraBloomFilter::make_hasher(operand.to_string()))
                                } else {
                                    None
                                }
                            }).collect_vec();
                        let mut filter = Simple::empty_instance(&config.get_shape());
                        for hasher in hashers {
                            filter.merge_hasher_in_place(&hasher);
                        }
                        new_insert.columns.push(config.bloom_column.clone());
                        operands.push(Operand::Const(CassandraBloomFilter::as_hex(&filter)));
                        Ok(Some(new_insert))
                    }
                }
            }
        }
    }

    fn process_select(
        message_state: &mut MessageState,
        config: &CassandraBloomFilterTableConfig,
    ) -> Result<Option<Select>, String> {
        // we need to
        // remove any where clauses that have the bloom filter columns
        // build a bloom filter with the values
        // insert the bloom filter where clause into the query
        // add the where clause bloom filter columns to the select so we can filter later

        // the columns we are interested in are in the where clause
        // we use this later
        let column_names = message_state
            .select
            .where_clause
            .iter()
            .filter_map(|re| {
                if let Operand::Column(name) = &re.obj {
                    Some(name)
                } else {
                    None
                }
            })
            .collect_vec();
        // get a list of all the column names.
        // see if some of the columns are in the bloom filter configuration.
        let some_columns: bool = column_names
            .iter()
            .find(|c| config.columns.contains(c))
            .is_some();

        if some_columns {
            // hashers for building bloom filter
            let mut hashers = vec![];

            // There is a weird case where the bloom filter is provided along with the search values
            // we will assume that the user knows what they want and not touch the filter but will
            // remove the columns from the query so the query will execute.  We will add the columns
            // to the selected columns
            let has_filter = column_names.contains(&&config.bloom_column);

            if !message_state.select.where_clause.is_empty()
                && message_state
                    .select
                    .where_clause
                    .iter()
                    .find( |re| match &re.obj {
                        Operand::List(_) => true,
                        _ => false,
                    })
                    .is_some()
            {
                message_state.error = true;
                return Err(
                    "bloom filter processing does not yet support (column...) = (values...)"
                        .to_string(),
                );
            }

            let interesting_relations: Vec<&RelationElement> = message_state
                .select
                .where_clause
                .iter()
                .filter(|re| match &re.obj {
                    Operand::Column(name) => {
                        !(has_filter && name.eq(&config.bloom_column))
                    }
                    _ => false,
                })
                .collect_vec();

            message_state.ignore = false;
            let selected_columns = message_state
                .select.columns
                .iter()
                .filter_map( |e|
                        if let SelectElement::Column(named) = e {
                            Some(&named.name )
                        } else {
                            None
                        }
                    )
                .collect_vec();
            for relation in interesting_relations {
                if !has_filter
                    && match relation.oper {
                        RelationOperator::Equal => false,
                        _ => true,
                    }
                {
                    message_state.error = true;
                    return Err(
                        "Only equality checks are allowed if bloom filter is not provided"
                            .to_string(),
                    );
                }

                // add the relation to the added selects
                match &relation.obj {
                    Operand::Column(name) => {
                        message_state
                            .bloomfilter
                            .added_selects
                            .push(SelectElement::Column(Named {
                                name: name.clone(),
                                alias: None,
                            }));
                        if !has_filter {
                            // only get here if we have equality statement and no filter defined
                            hashers.push(CassandraBloomFilter::make_hasher(
                                relation.value.to_string(),
                            ));
                        }
                    }
                    _ => (),
                }
                message_state
                    .bloomfilter
                    .verify_funcs
                    .push(relation.clone())
            }
            // remove duplicate select columns
            if message_state.select.columns.contains(&SelectElement::Star) {
                message_state.bloomfilter.added_selects.truncate(0);
            } else {
                message_state
                    .bloomfilter
                    .added_selects
                    .retain(|s| !selected_columns.contains(&&s.to_string()));
            }
            // build the bloom filter
            if !has_filter {
                let mut filter = Simple::empty_instance(&config.get_shape());
                for hasher in hashers {
                    filter.merge_hasher_in_place(&hasher);
                }

                let where_stmt = RelationElement {
                    obj: Operand::Column(config.bloom_column.clone()),
                    oper: RelationOperator::Equal,
                    value: Operand::Const(CassandraBloomFilter::as_hex(&filter)),
                };
                message_state.bloomfilter.added_where.push(where_stmt);
            }
            return Ok(Some(
                message_state
                    .bloomfilter
                    .rewrite_select(&message_state.select, &message_state.table_name),
            ));
        }
        message_state.ignore = true;
        return Ok(None);
    }

    fn as_hex(producer: &BloomFilterType) -> String {
        let mut hex = String::from("0x");
        producer
            .get_bitmaps()
            .iter()
            .for_each(|u| hex.push_str(format!("{:016x}", u).as_str()));
        hex
    }

    /// process a result message
    fn process_result(
        &mut self,
        session_state: &mut SessionState,
        message_state: &mut MessageState,
        query_response: &CassandraResult,
    ) -> Option<CassandraResult> {
        match query_response {
            CassandraResult::Rows { value, metadata } => {
                CassandraBloomFilter::process_rows(value, metadata, &mut message_state.bloomfilter)
            }
            CassandraResult::SetKeyspace(keyspace) => {
                session_state.default_keyspace = Some(keyspace.body.clone());
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
        bloomfilter_state: &mut BloomFilterState,
    ) -> Option<CassandraResult> {
        let added_columns = bloomfilter_state.get_added_column_names();

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
                bloomfilter_state
                    .verify_funcs
                    .iter()
                    .map(|re| {
                        if let Operand::Column(name) = &re.obj {
                            for idx in 0..metadata.columns_count {
                                let colspec: &ColSpec =
                                    metadata.col_specs.get(idx as usize).unwrap();
                                if colspec.name.eq(name) {
                                    return (idx as usize, re);
                                }
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
                        for vf in &bloomfilter_state.verify_funcs {
                            let name = match &vf.obj {
                                Operand::Column(name) => name,
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

        let relation_value = relation_element.value.to_string();

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
                Ok(v) => relation_element.oper.eval(i, &v),
                Err(_e) => false,
            },

            MessageValue::Double(d) => match relation_value.parse::<f64>() {
                Ok(v) => relation_element.oper.eval(d, &OrderedFloat(v)),
                Err(_e) => false,
            },

            MessageValue::Float(f) => match relation_value.parse::<f32>() {
                Ok(v) => relation_element.oper.eval(f, &OrderedFloat(v)),
                Err(_e) => false,
            },

            MessageValue::Boolean(b) => {
                let v = relation_value.to_uppercase().eq("TRUE");
                relation_element.oper.eval(b, &v)
            }

            MessageValue::Inet(ipaddr) => match IpAddr::from_str(relation_value.as_str()) {
                Ok(v) => relation_element.oper.eval(ipaddr, &v),
                Err(_e) => false,
            },

            MessageValue::Varint(i) => match relation_value.parse::<BigInt>() {
                Ok(v) => relation_element.oper.eval(i, &v),
                Err(_e) => false,
            },

            MessageValue::Decimal(d) => match relation_value.parse::<BigDecimal>() {
                Ok(v) => relation_element.oper.eval(d, &v),
                Err(_e) => false,
            },

            MessageValue::Date(i) => match relation_value.parse::<i32>() {
                Ok(v) => relation_element.oper.eval(i, &v),
                Err(_e) => false,
            },

            MessageValue::Timeuuid(u) | MessageValue::Uuid(u) => {
                match Uuid::parse_str(relation_value.as_str()) {
                    Ok(v) => relation_element.oper.eval(u, &v),
                    Err(_e) => false,
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

    /// modify the message if Bloom filter data is involved.
    fn process_bloom_data(
        &mut self,
        session_state: &mut SessionState,
        message_state: &mut MessageState,
        msg: &mut Message,
    ) {
        if let Some(Frame::Cassandra(cframe)) = msg.frame() {
            if let CassandraFrame {
                operation: CassandraOperation::Result(result),
                ..
            } = cframe
            {
                if let Some(new_result) = self.process_result(session_state, message_state, result)
                {
                    cframe.operation = CassandraOperation::Result(new_result);
                }
            }
        }
    }
}

#[async_trait]
impl Transform for CassandraBloomFilter {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut session_state = SessionState::new();
        let mut error_msg = None;
        /*
        a message wrapper has 1 or more messages
        a message has 0 or 1 Cassandra Queries
        a Cassandra Query has 1 CQL
        a CQL 0 or more CQLStatements.
        Each Cassandra Query may be modified and requires a matching BloomFilterState
        Each CQL requires matching CQLState that matches tracks the BloomFilterState for each query

         */

        // one state_list entry for each message, if None then the messages is not subsequently processed.
        let mut state_list = message_wrapper
            .messages
            .iter_mut()
            .map(|msg| {
                let timestamp = msg.meta_timestamp.clone();

                match &msg.metadata() {
                    Ok(Metadata::Cassandra(cassandra_metadata)) => {
                        match &mut msg.frame() {
                            Some(Frame::Cassandra(CassandraFrame {
                                operation: CassandraOperation::Query { query, .. },
                                ..
                            })) => {
                                let mut result = self.process_cql(
                                    &mut session_state,
                                    cassandra_metadata,
                                    timestamp,
                                    query,
                                );
                                if result.len() == 1 {
                                    match result.remove(0) {
                                        (Some(message), Some(state)) => {
                                            // error message or modified query
                                            if state.error {
                                                error_msg = Some(message);
                                            } else {
                                                *msg = message;
                                            }
                                            Some(state)
                                        }
                                        (Some(_), None) => {
                                            unreachable!()
                                        }
                                        (None, Some(state)) => {
                                            // query message that is not processed on return
                                            Some(state)
                                        }
                                        (None, None) => {
                                            // query message that is not processed on return
                                            None
                                        }
                                    }
                                } else {
                                    // if there is more than one then it can not be a select
                                    for (msg, _state) in result {
                                        if let Some(message) = msg {
                                            error_msg = Some(message);
                                        }
                                    }
                                    None
                                }
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                }
            })
            .collect_vec();

        if error_msg.is_some() {
            let m: Messages = vec![error_msg.unwrap()];
            Ok(m)
        } else {
            let mut response = message_wrapper.call_next_transform().await?;
            response
                .iter_mut()
                .zip(&mut state_list)
                .for_each(|(mut msg, s)| {
                    if let Some(message_state) = s {
                        self.process_bloom_data(&mut session_state, message_state, &mut msg)
                    }
                });
            Ok(response)
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionState {
    default_keyspace: Option<String>,
}

impl SessionState {
    pub fn new() -> SessionState {
        SessionState {
            default_keyspace: None,
        }
    }

    /// converts to lower case and adds default_keyspace if defined and keyspace not specified.
    pub fn fixup_table_name(&self, table_name: &FQName) -> FQName {
        if self.default_keyspace.is_none() || table_name.keyspace.is_some() {
            FQName {
                keyspace: if table_name.keyspace.is_some() {
                    Some(table_name.keyspace.as_ref().unwrap().to_lowercase())
                } else {
                    None
                },
                name: table_name.name.to_lowercase(),
            }
        } else {
            FQName {
                keyspace: Some(self.default_keyspace.as_ref().unwrap().to_lowercase()),
                name: table_name.name.to_lowercase(),
            }
        }
    }
}

pub struct MessageState {
    /// if true ignore the associated message
    ignore: bool,
    /// the original select statement
    select: Select,
    /// the metadata from the original frame
    meta: CassandraMetadata,
    /// the changes from the bloom filter change.
    bloomfilter: BloomFilterState,
    /// the timestamp from the original message.
    timestamp: Option<i64>,
    /// true if the associated message is an error message that should be returned to the caller.
    error: bool,
    /// the FQ table name for the associated message
    table_name: FQName,
}

impl MessageState {
    /// create a new MessageState
    pub(crate) fn new(
        select: &Select,
        meta: CassandraMetadata,
        timestamp: Option<i64>,
    ) -> MessageState {
        let msg_select = MessageState::normalize_select( select );
        MessageState {
            table_name: msg_select.table_name.clone(),
            ignore: false,
            select: msg_select,
            meta,
            bloomfilter: BloomFilterState::new(),
            timestamp,
            error: false,
        }
    }

    pub(crate) fn empty( meta: CassandraMetadata,
                         timestamp: Option<i64>,) -> MessageState {
        MessageState {
            ignore: true,
            select: Select {
                distinct: false,
                json: false,
                table_name: FQName::simple(""),
                columns: vec![],
                where_clause: vec![],
                order: None,
                limit: None,
                filtering: false,
            },
            meta,
            bloomfilter: BloomFilterState::new(),
            timestamp,
            error: false,
            table_name: FQName::simple(""),
        }
    }

    fn normalize_named( named : &Named ) -> Named {
        Named {
            name: named.name.to_lowercase(),
            alias: if let Some(x) = &named.alias {
                Some(x.to_lowercase())
            } else {
                None
            },
        }
    }

    fn normalize_select_element( select_element : &SelectElement) -> SelectElement {
        match select_element {
            SelectElement::Column(named) => SelectElement::Column(MessageState::normalize_named(named)),
            SelectElement::Star => SelectElement::Star,
            SelectElement::Function(named) => SelectElement::Function(MessageState::normalize_named(named)),
        }
    }

    fn normalize_operand( operand : &Operand ) -> Operand {
        match operand {
            Operand::Const(_)|
            Operand::Map(_)|
            Operand::Set(_) |
            Operand::List(_) =>  operand.clone(),
            Operand::Tuple( operands) => Operand::Tuple( operands.iter().map( MessageState::normalize_operand ).collect_vec() ),
            Operand::Column( name ) => Operand::Column( name.to_lowercase() ),
            Operand::Func( name ) => Operand::Func( name.to_lowercase() ),
            Operand::Param(name) => Operand::Param( name.to_lowercase()),
            Operand::Null => Operand::Null,
            Operand::Collection(operands) => Operand::Collection( operands.iter().map( MessageState::normalize_operand ).collect_vec() ),
        }
    }

    fn normalize_relation_element( relation_element : &RelationElement ) -> RelationElement {
        RelationElement{
            obj: MessageState::normalize_operand( &relation_element.obj ),
            oper: relation_element.oper.clone(),
            value: MessageState::normalize_operand( &relation_element.value ),
        }
    }

    fn normalize_fqname( fqname : &FQName ) -> FQName {
        FQName {
            keyspace : if let Some(keyspace) = &fqname.keyspace {
                Some( keyspace.to_lowercase() )
            } else { None },
            name : fqname.name.to_lowercase()
        }
    }
    fn normalize_order( order_option : &Option<OrderClause> ) -> Option<OrderClause> {
        match order_option {
            None => None,
            Some( order ) => Some( OrderClause { name: order.name.to_lowercase(), desc: order.desc }),
        }
    }

    fn normalize_select( select : &Select ) -> Select {
        Select {
            distinct: select.distinct,
            columns : select.columns.iter().map( MessageState::normalize_select_element ).collect_vec(),
            filtering : select.filtering,
            limit : select.limit,
            json : select.json,
            table_name : MessageState::normalize_fqname( &select.table_name ),
            where_clause : select.where_clause.iter().map( MessageState::normalize_relation_element).collect_vec(),
            order : MessageState::normalize_order( &select.order )
        }
    }

    pub fn rebuild_frame(&self) -> CassandraFrame {
        CassandraFrame {
            version: self.meta.version.clone(),
            stream_id: self.meta.stream_id.clone(),
            tracing_id: self.meta.tracing_id.clone(),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: CQL {
                    statements: vec![CQLStatement {
                        statement: CassandraStatement::Select(self.select.clone()),
                        has_error: false,
                    }],
                },
                params: Default::default(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilterState {
    ignore: bool,
    verify_funcs: Vec<RelationElement>,
    added_selects: Vec<SelectElement>,
    added_where: Vec<RelationElement>,
    added_values: Vec<(String, String)>,
    limit: Option<i32>,
}

impl BloomFilterState {
    /// create a new BloomFilterState
    pub fn new() -> BloomFilterState {
        BloomFilterState {
            ignore: false,
            verify_funcs: vec![],
            added_selects: vec![],
            added_where: vec![],
            added_values: vec![],
            limit: None,
        }
    }

    /// the column names as returned by a query for the columns we added to the query.
    pub fn get_added_column_names(&self) -> Vec<String> {
        self.added_selects
            .iter()
            .map(|se| match se {
                SelectElement::Column(named) => named.alias_or_name(),
                _ => unreachable!(),
            })
            .collect()
    }

    /// rewrite the select statement data as a _NEW_ select statement data object.
    pub fn rewrite_select(&mut self, select_data: &Select, table_name: &FQName) -> Select {
        self.limit = select_data.limit;
        let mut new_elements: Vec<SelectElement> = vec![];
        select_data
            .columns
            .iter()
            .for_each(|s| new_elements.push(s.clone()));
        self.added_selects
            .iter()
            .for_each(|s| new_elements.push(s.clone()));

        let mut new_where: Vec<RelationElement> = vec![];

        // add the where clauses that were not moved to verify_funcs.

        select_data
            .where_clause
            .iter()
            .filter(|f| !self.verify_funcs.contains(f))
            .for_each(|w| new_where.push(w.clone()));

        self.added_where
            .iter()
            .for_each(|w| new_where.push(w.clone()));

        let result = Select {
            distinct: select_data.distinct,
            table_name: table_name.clone(),
            where_clause: new_where,
            order: match &select_data.order {
                None => None,
                Some(x) => Some(x.clone()),
            },
            limit: None,
            json: select_data.json,
            columns: new_elements,
            filtering: select_data.filtering,
        };
        result
    }
}

#[cfg(test)]
mod test {
    use crate::frame::CassandraOperation::Query;
    use crate::frame::Frame::Cassandra;
    use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, CQL};
    use crate::message::{IntSize, Message, MessageValue};
    use crate::transforms::cassandra::bloom_filter::{
        BloomFilterState, CassandraBloomFilter, CassandraBloomFilterTableConfig, MessageState,
        SessionState,
    };

    use crate::frame::cassandra::{CQLStatement, CassandraMetadata};
    use cassandra_protocol::frame::frame_result::{
        ColSpec, ColType, ColTypeOption, RowsMetadata, RowsMetadataFlags,
    };
    use cassandra_protocol::frame::Version;
    use cql3_parser::cassandra_statement::CassandraStatement;
    use cql3_parser::common::{FQName, Operand, RelationElement, RelationOperator};
    use cql3_parser::insert::Insert;
    use cql3_parser::select::{Named, Select, SelectElement};
    use itertools::Itertools;
    use std::collections::HashMap;

    pub fn build_test_config() -> CassandraBloomFilterTableConfig {
        let mut result = CassandraBloomFilterTableConfig {
            table: FQName::simple("myTable"),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };
        result.normalize();
        result
    }

    fn build_message(select_stmt: &str) -> (Message, CassandraFrame, MessageState, CQL) {
        let cql = CQL::parse_from_string(select_stmt);
        let frame = CassandraFrame {
            version: Version::V3,
            stream_id: 10,
            tracing_id: None,
            warnings: vec![],
            operation: Query {
                query: cql.clone(),
                params: Default::default(),
            },
        };
        let metadata = frame.metadata();
        let msg = Message::from_frame(Cassandra(frame.clone()));
        let message_state = match &cql.statements[0].statement {
            CassandraStatement::Select(select) => {
                MessageState::new(&select, metadata, msg.meta_timestamp)
            }
            CassandraStatement::Insert(_) => MessageState::empty( metadata, msg.meta_timestamp ),
            _ => panic!("{:?}", &cql.statements[0].statement),
        };
        (msg, frame, message_state, cql)
    }

    fn get_select_result(
        message_state: &mut MessageState,
        config: &CassandraBloomFilterTableConfig,
        statement: &CQLStatement,
    ) -> Option<Select> {
        let result = match &statement.statement {
            CassandraStatement::Select(select_data) => {
                message_state.select = MessageState::normalize_select( select_data );
                CassandraBloomFilter::process_select(message_state, &config)
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

    /// process the insert and return the single result.
    fn get_insert_result(
        session_state: &SessionState,
        config: &CassandraBloomFilterTableConfig,
        statement: &CQLStatement,
    ) -> Option<Insert> {
        let result = match &statement.statement {
            CassandraStatement::Insert(insert) => {
                let table_name = session_state.fixup_table_name(&insert.table_name);
                CassandraBloomFilter::process_insert(insert, table_name, config)
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
    pub fn test_configuration_table_build() {
        let configs = [
            build_test_config(),
            CassandraBloomFilterTableConfig {
                table: FQName::new("myKeyspace", "myTable"),
                bloom_column: "filterColumn".to_string(),
                bits: 72,
                funcs: 3,
                columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
            },
        ];
        let expected = [
            FQName::simple("mytable"),
            FQName::new("mykeyspace", "mytable"),
        ];
        let mut result = CassandraBloomFilter::new(&configs, "test");
        assert_eq!( expected.len(), result.tables.len());
        for key in expected {
            assert!( result.tables.remove(&key).is_some() );
        }
    }
    #[test]
    pub fn test_process_select() {
        let config = build_test_config();
        let select_stmt = "SELECT foo FROM myTable WHERE bfCol1 = 'bar'";
        let (mut msg, _frame, mut message_state, cql) = build_message(select_stmt);

        let result = get_select_result(&mut message_state, &config, &cql.statements[0]);

        let expected_select = SelectElement::Column(Named {
            name: "bfcol1".to_string(),
            alias: None,
        });

        let expected_verify = RelationElement {
            obj: Operand::Column("bfcol1".to_string()),
            oper: RelationOperator::Equal,
            value: Operand::Const("'bar'".to_string()),
        };

        match result {
            None => assert!(false),
            Some(statement_data) => {
                assert!(!message_state.ignore);
                assert_eq!(vec![expected_select], message_state.bloomfilter.added_selects);
                assert_eq!(vec![expected_verify], message_state.bloomfilter.verify_funcs);
                assert_eq!(1, message_state.bloomfilter.added_where.len());
                assert_eq!(
                    config.bloom_column.as_str(),
                    message_state
                        .bloomfilter
                        .added_where
                        .get(0)
                        .unwrap()
                        .obj
                        .to_string()
                        .as_str()
                );
                let stmt = statement_data.to_string();
                assert_eq!( "SELECT foo, bfcol1 FROM mytable WHERE filtercolumn = 0x02000000000020000000000000000080", stmt.as_str());
            }
        }

        // verify original was not changed
        let cassandra_frame = msg.frame().unwrap().clone().into_cassandra().unwrap();

        match cassandra_frame.operation {
            CassandraOperation::Query { query, .. } => {
                assert_eq!(select_stmt, query.to_query_string());
            }
            _ => assert!(false),
        }
    }

    #[test]
    pub fn test_process_select_with_bloom_filter() {
        let config = build_test_config();

        let select_stmt = "SELECT foo FROM myTable WHERE bfCol1 <> 'bar' and filterColumn = 0x02000000000020000000000000000080".to_string();
        let (_msg, _frame, mut message_state, cql, ..) = build_message(&select_stmt);

        let result = get_select_result(&mut message_state, &config, &cql.statements[0]);

        let expected_select = SelectElement::Column(Named {
            name: "bfcol1".to_string(),
            alias: None,
        });

        let expected_verify = RelationElement {
            obj: Operand::Column("bfcol1".to_string()),
            oper: RelationOperator::NotEqual,
            value: Operand::Const("'bar'".to_string()),
        };

        match result {
            None => assert!(false),
            Some(statement_data) => {
                assert!(!message_state.ignore);
                assert_eq!(1, message_state.bloomfilter.added_selects.len());
                assert!(message_state
                    .bloomfilter
                    .added_selects
                    .contains(&expected_select));
                assert_eq!(1, message_state.bloomfilter.verify_funcs.len());
                assert!(message_state
                    .bloomfilter
                    .verify_funcs
                    .iter()
                    .contains(&expected_verify));
                assert_eq!(0, message_state.bloomfilter.added_where.len());
                let stmt = statement_data.to_string();
                assert_eq!( "SELECT foo, bfcol1 FROM mytable WHERE filtercolumn = 0x02000000000020000000000000000080", stmt.as_str());
            }
        }
    }

    #[test]
    pub fn test_process_select_without_filter_columns() {
        let config = CassandraBloomFilterTableConfig {
            table: FQName::simple("myTable"),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };

        let select_stmt = "SELECT foo FROM myTable WHERE junk <> 'bar'".to_string();
        let (_msg, _frame, mut message_state, cql, ..) = build_message(&select_stmt);

        let result = get_select_result(&mut message_state, &config, &cql.statements[0]);

        match result {
            None => assert_eq!(true, message_state.ignore),
            Some(_) => assert!(false),
        }
    }

    /*
        #[test]
        pub fn test_use_modifies_select() {
            let (msg, frame, _msg_state, mut cql ) = build_message( "use Hello" );

            let mut tables: HashMap<FQName, CassandraBloomFilterTableConfig> = HashMap::new();
            tables.insert(
                FQName::new("hello", "mytable"),
                CassandraBloomFilterTableConfig {
                    table: FQName::simple("myTable"),
                    bloom_column: "filterColumn".to_string(),
                    bits: 72,
                    funcs: 3,
                    columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
                },
            );

            let filter = CassandraBloomFilter::new(&tables, "testing_chain".to_string());

            let mut session_state = SessionState::new();

            let result = filter.process_query( &mut session_state, &frame.metadata(), msg.meta_timestamp, &mut cql);

            assert_eq!( Some("hello".to_string()), session_state.default_keyspace );
        }
    */
    #[test]
    pub fn test_setting_keyspace_modifies_queries() {
        // set the hello keyspace
        let mut session_state = SessionState::new();
        session_state.default_keyspace = Some("hello".to_string());

        let mut cfg = build_test_config();
        cfg.table = FQName::new( "hello", "mytable");
        let mut table_configs = [cfg];

        let filter = CassandraBloomFilter::new(&table_configs, "testing_chain");

        let select_stmt = "select * from myTable where bfCol1 = 'foo'";
        let (msg, frame, _message_state, mut cql) = build_message(select_stmt);
        let expected_name = FQName::new("hello", "mytable");
        let orig_name = FQName::simple("myTable");

        let mut result = filter.process_cql(
            &mut session_state,
            &frame.metadata(),
            msg.meta_timestamp,
            &mut cql,
        );

        let (msg, status) = result.remove(0);
        if let Some(Cassandra(CassandraFrame {
            operation: CassandraOperation::Query { query, .. },
            ..
        })) = msg.unwrap().frame()
        {
            let statement = &query.statements[0].statement;
            assert_eq!(
                &expected_name,
                CQLStatement::get_table_name(statement).unwrap()
            )
        } else {
            panic!("not a select statement")
        }

        //assert_eq!(orig_name, status.unwrap().select.table_name);
    }

    #[test]
    pub fn test_limit_removed_when_select_processed() {
        // set the hello keyspace
        let mut session_state = SessionState::new();

        let table_configs = [build_test_config()];

        let filter = CassandraBloomFilter::new(&table_configs, "testing_chain");

        let select_stmt = "select * from myTable where bfCol1 = 'foo' LIMIT 5";
        let (msg, frame, _message_state, mut cql) = build_message(select_stmt);

        let mut msg = filter.process_cql(
            &mut session_state,
            &frame.metadata(),
            msg.meta_timestamp,
            &mut cql,
        );
        assert_eq!(1, msg.len());
        match msg.remove(0) {
            (Some(mut msg), Some(_state)) => {
                let y = msg.frame().unwrap().clone().into_cassandra().unwrap();
                let z = match y.operation {
                    CassandraOperation::Query { query, .. } => query.to_query_string(),
                    _ => "ERROR".to_string(),
                };
                let cql = CQL::parse_from_string(&z);
                let statement = &cql.statements[0];
                assert!(!statement.has_error);
                assert_eq!(
                    "SELECT * FROM mytable WHERE filtercolumn = 0x0200000008000040",
                    statement.to_string().as_str()
                );
            }
            _ => panic!("Message and message state not found"),
        }
    }

    #[test]
    pub fn test_process_rows() {
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
            obj: Operand::Column("bfCol".to_string()),
            oper: RelationOperator::LessThan,
            value: Operand::Const("10".to_string()),
        });

        let mut added_selects = vec![];
        added_selects.push(SelectElement::Column(Named {
            name: "bfCol".to_string(),
            alias: None,
        }));

        let mut bloomfilter_state = BloomFilterState {
            ignore: false,
            verify_funcs,
            added_selects,
            added_where: vec![],
            added_values: vec![],
            limit: None,
        };

        let result = CassandraBloomFilter::process_rows(
            &message_value,
            &orig_metadata,
            &mut bloomfilter_state,
        );
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

    #[test]
    pub fn test_process_insert() {
        let mut session_state = SessionState::new();

        let config = CassandraBloomFilterTableConfig {
            table: FQName::simple("myTable"),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };

        let stmts = [
            "INSERT INTO myTable (bfCol1, bfCol2) VALUES ('foo', 'bar')",
            "INSERT INTO myTable (bfCol1, bfCol2, Col1) VALUES ('foo', 'bar', 5)",
            "INSERT INTO myTable (bfCol1) VALUES ('foo')",
        ];
        let expected = [
            "INSERT INTO mytable (bfCol1, bfCol2, filterColumn) VALUES ('foo', 'bar', 0x02000000080020400000000000000080)",
            "INSERT INTO mytable (bfCol1, bfCol2, Col1, filterColumn) VALUES ('foo', 'bar', 5, 0x02000000080020400000000000000080)",
            "INSERT INTO mytable (bfCol1, filterColumn) VALUES ('foo', 0x0200000008000040)",
        ];

        for idx in 0..stmts.len() {
            let (mut msg, _frame, mut message_state, cql) = build_message(stmts[idx]);
            let result = get_insert_result(&session_state, &config, &cql.statements[0]);

            match result {
                None => assert!(false),
                Some(insert) => {
                    let stmt = insert.to_string();
                    assert_eq!(expected[idx], stmt.as_str());
                }
            }

            // verify original was not changed
            let cassandra_frame = msg.frame().unwrap().clone().into_cassandra().unwrap();

            match cassandra_frame.operation {
                CassandraOperation::Query { query, .. } => {
                    assert_eq!(stmts[idx], query.to_query_string());
                }
                _ => assert!(false),
            }
        }
    }

    #[test]
    pub fn test_process_insert_with_keyspace() {
        let mut session_state = SessionState::new();
        session_state.default_keyspace = Some("mykeyspace".into());

        let config = CassandraBloomFilterTableConfig {
            table: FQName::new("mykeyspace", "myTable"),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        };

        let stmts = [
            "INSERT INTO myTable (bfCol1, bfCol2) VALUES ('foo', 'bar')",
            "INSERT INTO myTable (bfCol1, bfCol2, Col1) VALUES ('foo', 'bar', 5)",
            "INSERT INTO myTable (bfCol1) VALUES ('foo')",
            "INSERT INTO someother.myTable (bfCol1) VALUES ('foo')",
        ];
        let expected = [
            "INSERT INTO mykeyspace.mytable (bfCol1, bfCol2, filterColumn) VALUES ('foo', 'bar', 0x02000000080020400000000000000080)",
            "INSERT INTO mykeyspace.mytable (bfCol1, bfCol2, Col1, filterColumn) VALUES ('foo', 'bar', 5, 0x02000000080020400000000000000080)",
            "INSERT INTO mykeyspace.mytable (bfCol1, filterColumn) VALUES ('foo', 0x0200000008000040)",
            "INSERT INTO someother.mytable (bfCol1, filterColumn) VALUES ('foo', 0x0200000008000040)",
        ];

        for idx in 0..stmts.len() {
            let (mut msg, _frame, mut message_state, cql) = build_message(stmts[idx]);
            let result = get_insert_result(&session_state, &config, &cql.statements[0]);

            match result {
                None => assert!(false),
                Some(insert) => {
                    let stmt = insert.to_string();
                    assert_eq!(expected[idx], stmt.as_str());
                }
            }

            // verify original was not changed
            let cassandra_frame = msg.frame().unwrap().clone().into_cassandra().unwrap();

            match cassandra_frame.operation {
                CassandraOperation::Query { query, .. } => {
                    assert_eq!(stmts[idx], query.to_query_string());
                }
                _ => assert!(false),
            }
        }
    }

    struct CQLProcessData {
        query: &'static str,
        result: &'static str,
        no_msg: bool,
        added_selects: Vec<&'static str>,
        added_verify: Vec<&'static str>,
        added_where: Vec<&'static str>,
        limit : Option<i32>,
        ignore: bool,
    }

    #[test]
    pub fn test_process_cql() {
        let configs = [CassandraBloomFilterTableConfig {
            table: FQName::simple("myTable"),
            bloom_column: "filterColumn".to_string(),
            bits: 72,
            funcs: 3,
            columns: Vec::from(["bfCol1".to_string(), "bfCol2".to_string()]),
        }];

        let bloomfilter = CassandraBloomFilter::new(&configs, "test_chain");
        let mut session_state = SessionState::new();
        let mut cassandra_metadata = CassandraMetadata {
            version: Version::V3,
            stream_id: 0,
            tracing_id: None,
        };
        let timestamp = Some(500_i64);

        let test_data : Vec<CQLProcessData> = vec![
            CQLProcessData {
                query: "SELECT * FROM myTable",
                result: "SELECT col1 FROM myTable where bfCol1='foo'",
                no_msg : true,
                added_selects: vec!(),
                added_verify: vec!(),
                added_where: vec!(),
                limit : None,
                ignore: true
            },

            CQLProcessData {
                query: "SELECT col1 FROM myTable where bfCol1='foo'",
                result: "SELECT col1, bfcol1 FROM mytable WHERE filtercolumn = 0x0200000008000040",
                no_msg: false,
                added_selects: vec!["bfcol1"],
                added_verify: vec!["bfcol1 = 'foo'"],
                added_where: vec!["filtercolumn = 0x0200000008000040"],
                limit : None,
                ignore: false
            },

            CQLProcessData {
                query: "SELECT bfCol1 FROM myTable where bfCol2='foo'",
                result: "SELECT bfcol1, bfcol2 FROM mytable WHERE filtercolumn = 0x0200000008000040",
                no_msg: false,
                added_selects: vec!["bfcol2"],
                added_verify: vec!["bfcol2 = 'foo'"],
                added_where: vec!["filtercolumn = 0x0200000008000040"],
                limit : None,
                ignore: false
            },

            CQLProcessData {
                query: "SELECT filtercolumn, bfCol1 FROM myTable where bfCol2='foo'",
                result: "SELECT filtercolumn, bfcol1, bfcol2 FROM mytable WHERE filtercolumn = 0x0200000008000040",
                no_msg: false,
                added_selects: vec!["bfcol2"],
                added_verify: vec!["bfcol2 = 'foo'"],
                added_where: vec!["filtercolumn = 0x0200000008000040"],
                limit : None,
                ignore: false
            }
        ];

        for process_data in test_data {
            let mut cql = CQL::parse_from_string(process_data.query);
            let result =
                bloomfilter.process_cql(&mut session_state, &cassandra_metadata, timestamp, &mut cql);
            // first query
            assert_eq!(1, result.len(), "result length issue: {}", process_data.query);
            if process_data.no_msg {
                assert_eq!(None, result[0].0, "no_msg issue: {}", process_data.query);
            } else {
                if let Some(mut message) = result[0].0.clone() {
                    if let Some(Cassandra(CassandraFrame { operation: cql, .. })) = message.frame() {
                        if let CassandraOperation::Query { query: CQL { statements, .. }, .. } = cql {
                            assert_eq!(1, statements.len(), "too many statements: {}", process_data.query);
                            if let CQLStatement { statement: CassandraStatement::Select(select), .. } = &statements[0] {
                                assert_eq!(process_data.result, select.to_string(), "wrong result: {}", process_data.query);
                            } else {
                                panic!("wrong CassandraStatement: {}", process_data.query);
                            }
                        } else {
                            panic!("wrong CassandraOperation: {}", process_data.query);
                        }
                    } else {
                        panic!("wrong frame: {}", process_data.query);
                    }
                } else {
                    panic!("no message: {}", process_data.query);
                }
            }

            if let Some(message_state) = &result[0].1 {
                assert_eq!(process_data.ignore, message_state.ignore, "Ignore issue: {}", process_data.query);
                if !process_data.ignore {
                    assert_eq!(0, message_state.bloomfilter.added_values.len(), "added_values issue: {}", process_data.query);
                    assert_eq!(timestamp, message_state.timestamp, "timestamp issue: {}", process_data.query);
                    assert_eq!(false, message_state.error, "error issue: {}", process_data.query);
                    assert_eq!(process_data.added_selects.len(), message_state.bloomfilter.added_selects.len(), "added_selects count issue: {}", process_data.query);
                    for idx in 0..process_data.added_selects.len() {
                        assert_eq!(process_data.added_selects[idx], message_state.bloomfilter.added_selects[idx].to_string(), "added_select {} issue: {}", idx, process_data.query);
                    }

                    assert_eq!(process_data.added_verify.len(), message_state.bloomfilter.verify_funcs.len(), "added_verify count issue: {}", process_data.query);
                    for idx in 0..process_data.added_verify.len() {
                        assert_eq!(process_data.added_verify[idx], message_state.bloomfilter.verify_funcs[idx].to_string(), "added_verify {} issue: {}", idx, process_data.query);
                    }

                    assert_eq!(process_data.added_where.len(), message_state.bloomfilter.added_where.len(), "added_where count issue: {}", process_data.query);
                    for idx in 0..process_data.added_where.len() {
                        assert_eq!(process_data.added_where[idx], message_state.bloomfilter.added_where[idx].to_string(),"added_where {} issue: {}", idx,  process_data.query);
                    }
                    assert_eq!(process_data.limit, message_state.bloomfilter.limit, "limit issue: {}", process_data.query);
                }
            } else {
                panic!("No message state: {}", process_data.query);
            }
        }
    }
}
