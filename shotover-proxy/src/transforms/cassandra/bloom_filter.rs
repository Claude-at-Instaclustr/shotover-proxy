use std::io::Cursor;
use murmur3::murmur3_x64_128;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::message;
use crate::message::{Message, MessageDetails, Messages, QueryMessage, QueryResponse, Value};
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
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, trace};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraBloomFilterTableConfig {
    #[serde(rename = "remote_address")]
    pub keyspace: String,
    pub table: String,
    pub bloomColumn: String,
    pub bits: usize,
    pub funcs: usize,
    pub columns: Vec<String>,
}


#[derive(Debug)]
pub struct CassandraBloomFilter {
    outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec>>,
    cassandra_ks: HashMap<String, Vec<String>>,
    bypass: bool,
    chain_name: String,
    tables: HashMap<String, CassandraBloomFilterTableConfig>,
    messages : HashMap<StreamId, QueryMessage>,
}

impl Clone for CassandraBloomFilter {
    fn clone(&self) -> Self {
        CassandraBloomFilter::new(self.tables, self.bypass, self.chain_name.clone())
    }
}

enum ColumnType {
    Custom( String ),
    ASCII(),
    Bigint(),
    Blob(),
    Boolean(),
    Counter(),
    Decimal(),
    Double(),
    Float(),
    Int(),
    Timestamp(),
    Uuid(),
    Varchar(),
    Varint(),
    Timeuuid(),
    Inet(),
    Date(),
    Time(),
    Smallint(),
    Tinyint(),
    List( ColumnType ),
    Map( Vec<ColumnType>),
    Set( ColumnType ),
    UDT( UdtType ),
    Tuple( Vec<ColumnType> ),
    unknown( u16 ),
}

struct ColMetadata {
    key_space : Optional<String>,
    table_name : Optional<String>,
    column_name : String,
    column_type : ColumnType,
}

struct RowMetadata {
    flags : i32,
    paging_state : Optional<Vec<u8>>,
    key_space : Optional<String>,
    table_name : Optional<String>,
    columns: Vec<ColMetadata>,
}

struct UdtType {
    key_space : String,
    udt_name : String,
    names : Vec<String>,
    types : Vec<ColumnType>,
}

impl CassandraBloomFilter {
    pub fn new(tables: &HashMap<String, CassandraBloomFilterTableConfig>, bypass: bool, chain_name: String) -> CassandraBloomFilter {
        let sink_single = CassandraBloomFilter {
            tables,
            outbound: None,
            cassandra_ks: HashMap::new(),
            bypass,
            chain_name: chain_name.clone(),
            messages : HashMap.new(),
        };

        register_counter!("failed_requests", Unit::Count, "chain" => chain_name, "transform" => sink_single.get_name());

        sink_single
    }

    fn get_name(&self) -> &'static str {
        "CassandraBloomFilter"
    }

    fn make_hasher(value : String) -> HasherType {
        let hash_result = murmur3_x64_128(&mut Cursor::new(value), 0)?;
        let mask : u64 = (-1 : i64) as u64;
        let initial = ((hash_result >> 64) & mask) as u64;
        let incr = (hash_result & mask) as u64;
        SimpleHasher::new( initial, incr )
    }

    fn make_filter_column( bloomFilter : Box<dyn BloomFilter> ) -> String {
        let bitMaps = bloomFilter.get_bitmaps() ;

        let mut parts: Vec<String> = Vec::with_capacity( bitMaps.len()+1);
        parts.push( "0x")
        for word in bitMaps {
            parts.push( format!("{:X}", word))
        }
        parts.join("")
    }

    fn process_query(&self, msg : &QueryMessage) -> msg {
        let fq_table_name = msg.namespace.join( ".");
        let config = self.tables.get( f2_table_name )?;
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

    fn read_string(body_cursor: &mut Cursor<&[u8]> ) -> String {
        let mut len = body_cursor.get_i32();
        let mut buff = Vec::with_capacity( len );
        body_cursor.read_exact( &buff );
        String::from_utf8( buff ).unwrap()
    }


    fn parse_type_udt(body_cursor: &mut Cursor<&[u8]>) -> ColumnType::UDT {
        let key_space = CassandraBloomFilter::read_string( body_cursor );
        let udt_name = CassandraBloomFilter::read_string( body_cursor );
        let count = body_cursor.get_i8();
        let mut names = Vec::with_capacity(count);
        let mut types :Vec<ColumnType> = Vec::with_capacity(count);
        for i in 0..count {
            names.push(CassandraBloomFilter::read_string(body_cursor));
            types.push(CassandraBloomFilter::parse_type(body_cursor))
        }
        ColumnType::UDT( UdtType {
            key_space,
            udt_name,
            names,
            types,
        } )
    }

    fn parse_type_tuple(body_cursor: &mut Cursor<&[u8]>) -> ColumnType::Tuple {
        let count = body_cursor.get_i32();
        let mut types  :Vec<ColumnType> = Vec::with_capacity(count);
        for i in 0..count {
            types.push( CassandraBloomFilter::parse_type(body_cursor));
        }
        ColumnType::Tuple( types )
    }

    fn parse_type(body_cursor: &mut Cursor<&[u8]>) -> ColumnType {
        let id = body_cursor.read_u16() ;
        match  id {
            0x0000 => ColumnType::Custom( CassandraBloomFilter::read_string(body_cursor)),
            0x0001 => ColumnType::ASCII() ,
            0x0002 => ColumnType::Bigint(),
            0x0003 => ColumnType::Blob(),
            0x0004 => ColumnType::Boolean(),
            0x0005 => ColumnType::Counter(),
            0x0006 => ColumnType::Decimal(),
            0x0007 => ColumnType::Double(),
            0x0008 => ColumnType::Float(),
            0x0009 => ColumnType::Int(),
            0x000B => ColumnType::Timestamp(),
            0x000C => ColumnType::Uuid(),
            0x000D => ColumnType::Varchar(),
            0x000E => ColumnType::Varint(),
            0x000F => ColumnType::Timeuuid(),
            0x0010 => ColumnType::Inet(),
            0x0011 => ColumnType::Date(),
            0x0012 => ColumnType::Time(),
            0x0013 => ColumnType::Smallint(),
            0x0014 => ColumnType::Tinyint(),
            0x0020 => ColumnType::List( CassandraBloomFilter::parse_type(body_cursor)),
            0x0021 => {
                let mut result : Vec<ColumnType> = Vec::with_capacity(2);
                result[0] = CassandraBloomFilter::parse_type(body_cursor);
                result[1] = CassandraBloomFilter::parse_type(body_cursor);
                return ColumnType::Map( result );
            },
            0x0022 => ColumnType::Set( CassandraBloomFilter::parse_type( body_cursor)),
            0x0030 => ColumnType::UDT( CassandraBloomFilter::parse_type_udt() ),
            0x0031 => ColumnType::Tuple( CassandraBloomFilter::parse_type_tuple() ),
            _ => ColumnType::unknown( id ),
        }
    }

    fn read_bytes( body_cursor: &mut Cursor<&[u8]> ) -> Vec<u8> {
        let len = body_cursor.get_i32();
        let buff = Vec::with_capacity( len );
        body_cursor.read_exact( &buff );
        buff
    }

    fn parse_row_metadata( body_cursor: &mut Cursor<&[u8]>  ) -> RowMetadata
    {
        let flags = body_cursor.get_i32();
        let column_count = body_cursor.get_i32();
        let mut metadata = RowMetadata {
            flags,
            paging_state: None,
            key_space: None,
            table_name: None,
            columns: Vec::with_capacity(column_count),
        };
        if (flags & 0x0002) > 0 {
            metadata.paging_state = Some(CassandraBloomFilter::read_bytes( body_cursor ));
        }
        match (flags & 0x0001) > 0 {
            true => {
                metadata.key_space = Some( CassandraBloomFilter::read_string( body_cursor) );
                metadata.table_name = Some( CassandraBloomFilter::read_string( body_cursor) );
            },
            false => {}
        }
        for column_number in 0..column_count {
            let mut result = ColMetadata {
                key_space: metadata.key_space.clone(),
                table_name: metadata.table_name.clone(),
                column_name: "".to_string(),
                column_type: ColType { id: 0, value: () },
            };
            if (flags & 0x0001) != 0x0001 {
                result.key_space = Some( CassandraBloomFilter::read_string( body_cursor) );
                result.table_name = Some( CassandraBloomFilter::read_string( body_cursor) );
            }
            result.column_name = CassandraBloomFilter::read_string( body_cursor);
            result.column_type = CassandraBloomFilter::parse_type ( body_cursor),
            metadata.columns.push( result );
        }
        metadata
    }


    fn parse_row_data( body_cursor: &mut Cursor<&[u8]>, column_count : usize ) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity( column_count );
        for i in 0..column_count {
            result.push( CassandraBloomFilter::read_bytes( body_cursor ));
        }
        result;
    }

    fn remove_false_positives(row_data : &mut Vec<Vec<Vec<u8>>>, row_metadata: &RowMetadata, query_values : HashMap<String,Value>) {
        row_data.retain( |row| {
            for column_number in 0..row.len() {
                let column_meta = row_metadata.columns.get(column_number);
                if let expected = query_values.get(&*column_meta.column_name)? {
                    if expected != row[column_number] {
                        return false;
                    }
                }
            }
            true
        });
    }

    fn remove_unwanted_columns(row_data : &mut Vec<Vec<Vec<u8>>>, row_metadata: &mut RowMetadata, projection : Vec<String>) {
        for row in row_data {
            let mut column_number = 0;
            let mut column = row.iter();
            match column.next() {
                Some(column_data) => {
                    let column_meta = row_metadata.columns.get(column_number);
                    if !projection.contains(column_meta.column_name) {
                        column.remove(column_number);
                    }
                    column_number += 1;
                },
                _ => {
                    column_number += 1;
                }
            }
        }
        let mut meta = row_meta.columns.iter();
        let mut column_number = 0;
        match meta.next() {
            Some(column_meta) {
                if !projection.contains(column_meta.column_name) {
                    column_meta.remove(column_number);
                }
                column_number += 1;
            },
            _ => {
                column_number += 1;
            }
        }
    }

    fn process_result( &mut self, msg : &mut Message, stream : &StreamId) {
        let old_msg = self.messages.get(stream)?;
        let RawFrame::Cassandra(frame) = &msg.original;
        let mut body_cursor = Cursor::new(frame.body.as_slice());
        /* The first element of the body of a RESULT message is an [int] representing the
  `kind` of result. The rest of the body depends on the kind. 0x0002 is the row flag*/
        if body_cursor.get_i32() == 0x0002 {
            let mut row_metadata = CassandraBloomFilter::parse_row_metadata( &mut body_cursor );
            let row_count = body_cursor.get_i32();
            let mut row_data = Vec::with_capacity(row_count as usize);
            for i in 0..row_count {
                row_data.push( CassandraBloomFilter::parse_row_data(&mut body_cursor, row_metadata.columns.len()) );
            }

            CassandraBloomFilter::remove_false_positives(  &mut row_data,&row_metadata, old_msg.query_values? );
            CassandraBloomFilter::remove_unwanted_columns( &mut row_data,&mut row_metadata,old_msg.projection?);
            let new_frame = Frame {
                version: frame.version,
                direction: frame.direction,
                flags: frame.flags,
                opcode: frame.opcode,
                stream_id: frame.stream_id,
                body: CassandraBloomFilter::rebuild_body( & row_data, &row_metadata ),
                tracing_id: frame.tracing_id,
                warnings: frame.warnings.clone(),
            };
            msg.original = RawFrame::Cassandra( new_frame );
        }
    }

    fn extract_bloom_data( &mut self, messages : Messages ) -> Messages {
        let mut newMsgs : Messages = vec![];
        for msg in messages {
            let stream = if let RawFrame::Cassandra(frame) = &msg.original {
                frame.stream_id
            } else {
                info!("no cassandra frame found");
                newMsgs.push(msg);
            };
            match msg.details {
                MessageDetails::Query(x) => {
                    self.messages.insert(streamId, x.clone());
                    newMsgs.push(self.process_query(&x));
                },
                MessageDetails::Response(x) => {
                    self.process_result(&mut msg, &stream);
                    newMsgs.push(msg.clone());
                },
                _ => newMsgs.push(msg.clone()),
            };
        }
        newMsgs
    }


    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        loop {
            match self.outbound {
                None => {
                    trace!("creating outbound connection {:?}", self.address);
                    let mut conn_pool = OwnedUnorderedConnectionPool::new(
                        self.address.clone(),
                        CassandraCodec::new(self.cassandra_ks.clone(), self.bypass),
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

#[async_trait]
impl Transform for CassandraBloomFilter {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(self.extract_bloom_data(message_wrapper.messages)).await
    }

    fn is_terminating(&self) -> bool {
        true
    }







}
