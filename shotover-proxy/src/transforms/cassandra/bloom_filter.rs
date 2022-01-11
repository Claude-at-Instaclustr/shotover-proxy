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
        let old_msg = self.messages.get(stream)?;
        match query_response.result.unwrap() {
            Value::NamedRows(mut rows) => {
                CassandraBloomFilter::remove_unwanted_data( &mut rows, &old_msg );
            },
            _ => {},
        }
    }

    fn process_bloom_data(&mut self, messages : Messages ) -> Messages {
        let mut newMsgs : Messages = vec![];
        for mut msg in messages {
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
                    self.process_result(& x, &stream);
                    msg.modified = true;
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
        let messages = self.send_message(self.process_bloom_data(message_wrapper.messages)).await?
        Ok(self.process_bloom_data( messages ) )
    }

    fn is_terminating(&self) -> bool {
        false
    }

}
