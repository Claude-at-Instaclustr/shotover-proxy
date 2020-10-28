use core::fmt;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use anyhow::Result;
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{ASTHolder, Messages, Value as ShotoverValue};
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use sqlparser::ast::{BinaryOperator, DateTimeField, Expr, SetExpr, Statement, Value};
use std::borrow::Borrow;

const TRUE: [u8; 1] = [0x1];
const FALSE: [u8; 1] = [0x0];

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisConfig {
    #[serde(rename = "config_values")]
    pub uri: String,
    pub caching_schema: HashMap<String, PrimaryKey>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PrimaryKey {
    partition_key: Vec<String>,
    range_key: Vec<String>,
}

#[async_trait]
impl TransformsFromConfig for RedisConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisCache(
            SimpleRedisCache::new_from_config(&self.uri).await,
        ))
    }
}

#[derive(Clone)]
pub struct SimpleRedisCache {
    name: &'static str,
    con: MultiplexedConnection,
    caching_schema: HashMap<String, PrimaryKey>,
}

impl Debug for SimpleRedisCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Name: {}, conversions: {:?}",
            self.name, self.caching_schema
        )
    }
}

impl SimpleRedisCache {
    //"redis://127.0.0.1/"
    pub fn new(connection: MultiplexedConnection) -> SimpleRedisCache {
        SimpleRedisCache {
            name: "SimpleRedisCache",
            con: connection,
            caching_schema: HashMap::new(),
        }
    }

    pub async fn new_from_config(params: &str) -> SimpleRedisCache {
        let client = redis::Client::open(params).unwrap();
        let con = client.get_multiplexed_tokio_connection().await.unwrap();
        SimpleRedisCache {
            name: "SimpleRedisCache",
            con,
            caching_schema: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ValueHelper(#[serde(with = "SQLValueDef")] Value);

impl ValueHelper {
    fn as_bytes(&self) -> &[u8] {
        return match &self.0 {
            Value::Number(v) => v.as_bytes(),
            Value::SingleQuotedString(v) => v.as_bytes(),
            Value::NationalStringLiteral(v) => v.as_bytes(),
            Value::HexStringLiteral(v) => v.as_bytes(),
            Value::Boolean(v) => {
                if *v {
                    &TRUE
                } else {
                    &FALSE
                }
            }
            Value::Date(v) => v.as_bytes(),
            Value::Time(v) => v.as_bytes(),
            Value::Timestamp(v) => v.as_bytes(),
            Value::Null => &[],
            _ => unreachable!(),
        };
    }
}

fn build_redis_commands(expr: &Expr, pks: &Vec<String>, min: &mut Vec<u8>, max: &mut Vec<u8>) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // first check if this is a related to PK
            if let Expr::Identifier(i) = left.borrow() {
                let id_string = i.to_string();
                if pks.iter().find(|&v| v == &id_string).is_some() {
                    //Ignore this as we build the pk constraint elsewhere
                    return;
                }
            }

            match op {
                BinaryOperator::Plus => {}
                BinaryOperator::Minus => {}
                BinaryOperator::Multiply => {}
                BinaryOperator::Divide => {}
                BinaryOperator::Modulus => {}
                BinaryOperator::Gt => {
                    // we shift the value for Gt so that it works with other GtEq operators
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut minrv = Vec::from(vh.as_bytes());
                        let len = minrv.len();

                        let last_byte = minrv.get_mut(len - 1).unwrap();
                        *last_byte += 1;

                        min.append(&mut minrv);
                        min.push(':' as u8);
                    }
                }
                BinaryOperator::Lt => {
                    // we shift the value for Lt so that it works with other LtEq operators
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut maxrv = Vec::from(vh.as_bytes());
                        let len = maxrv.len();

                        let last_byte = maxrv.get_mut(len - 1).unwrap();
                        *last_byte -= 1;

                        max.append(&mut maxrv);
                        max.push(':' as u8);
                    }
                }
                BinaryOperator::GtEq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut minrv = Vec::from(vh.as_bytes());

                        min.append(&mut minrv);
                        min.push(':' as u8);
                    }
                }
                BinaryOperator::LtEq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut maxrv = Vec::from(vh.as_bytes());

                        max.append(&mut maxrv);
                        max.push(':' as u8);
                    }
                }
                BinaryOperator::Eq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut minrv = Vec::from(vh.as_bytes());
                        let mut maxrv = minrv.clone();

                        min.append(&mut minrv);
                        min.push(':' as u8);

                        max.append(&mut maxrv);
                        max.push(':' as u8);
                    }
                }
                BinaryOperator::NotEq => {}
                BinaryOperator::And => {
                    build_redis_commands(left, pks, min, max);
                    build_redis_commands(right, pks, min, max)
                }
                BinaryOperator::Or => {}
                BinaryOperator::Like => {}
                BinaryOperator::NotLike => {}
            }
        }
        _ => {}
    }
}

fn build_redis_ast_from_sql(
    ast: ASTHolder,
    primary_key_values: &HashMap<String, ShotoverValue>,
    pk_schema: &PrimaryKey,
) -> ASTHolder {
    match &ast {
        ASTHolder::SQL(sql) => {
            if let Statement::Query(box sqlparser::ast::Query {
                ctes: _,
                body:
                    SetExpr::Select(box sqlparser::ast::Select {
                        distinct,
                        projection,
                        from,
                        selection: Some(expr),
                        group_by,
                        having,
                    }),
                order_by: _,
                limit: _,
                offset: _,
                fetch: _,
            }) = sql
            {
                let mut commands_buffer: Vec<ShotoverValue> = Vec::new();
                let mut min: Vec<u8> = Vec::new();
                min.push('[' as u8);
                let mut max: Vec<u8> = Vec::new();
                max.push(']' as u8);
                let pks = primary_key_values.keys().cloned().collect_vec();

                build_redis_commands(expr, &pks, &mut min, &mut max);

                commands_buffer.push(ShotoverValue::Strings("ZRANGEBYLEX".to_string()));
                let pk =
                    primary_key_values
                        .values()
                        .cloned()
                        .fold(BytesMut::new(), |mut acc, v| {
                            acc.extend(v.into_bytes());
                            acc
                        });
                commands_buffer.push(ShotoverValue::Bytes(pk.freeze()));
                commands_buffer.push(ShotoverValue::Bytes(Bytes::from(min)));
                commands_buffer.push(ShotoverValue::Bytes(Bytes::from(max)));
                return ASTHolder::Commands(ShotoverValue::List(commands_buffer));
            } else {
                panic!("woops");
            }
        }
        ASTHolder::Commands(a) => {
            return ast;
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Value")]
pub enum SQLValueDef {
    /// Numeric literal
    #[cfg(not(feature = "bigdecimal"))]
    Number(String),
    #[cfg(feature = "bigdecimal")]
    Number(BigDecimal),
    /// 'string value'
    SingleQuotedString(String),
    /// N'string value'
    NationalStringLiteral(String),
    /// X'hex value'
    HexStringLiteral(String),
    /// Boolean value true or false
    Boolean(bool),
    /// `DATE '...'` literals
    Date(String),
    /// `TIME '...'` literals
    Time(String),
    /// `TIMESTAMP '...'` literals
    Timestamp(String),
    /// INTERVAL literals, roughly in the following format:
    /// `INTERVAL '<value>' <leading_field> [ (<leading_precision>) ]
    /// [ TO <last_field> [ (<fractional_seconds_precision>) ] ]`,
    /// e.g. `INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)`.
    ///
    /// The parser does not validate the `<value>`, nor does it ensure
    /// that the `<leading_field>` units >= the units in `<last_field>`,
    /// so the user will have to reject intervals like `HOUR TO YEAR`.
    #[serde(skip)]
    Interval {
        value: String,
        leading_field: DateTimeField,
        leading_precision: Option<u64>,

        last_field: Option<DateTimeField>,
        /// The seconds precision can be specified in SQL source as
        /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
        /// will be `Second` and the `last_field` will be `None`),
        /// or as `__ TO SECOND(x)`.
        fractional_seconds_precision: Option<u64>,
    },
    /// `NULL` value
    Null,
}

#[async_trait]
impl Transform for SimpleRedisCache {
    // #[instrument]
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        // let responses = Messages::new();
        // for m in &qd.message.messages {
        //     if let Query(qm) = &m.details {
        //         qm.primary_key
        //     }
        // }
        // for message in &qd.message.messages {
        //     let wrapped_message = Wrapper::new_with_next_transform(
        //         Messages::new_from_message(message.clone()),
        //         0,
        //     );
        //     if let MessageDetails::Query(qm) = &message.details {
        //         if qm.primary_key.is_empty() {
        //             responses
        //                 .messages
        //                 .append(&mut t.call_next_transform(wrapped_message).await?.messages);
        //         } else {
        //             if let Some(ASTHolder::SQL(ast)) = &qm.ast {
        //                 match ast {
        //                     Query(_) => {
        //                         let mut client_copy = self.con.clone();
        //
        //                         //TODO: something something what happens if hset fails.
        //                         // let f: RedisFuture<HashMap<String, String>> = client_copy.hgetall(&qm.get_primary_key());
        //                         let p = &mut pipe();
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(values) = &qm.projection {
        //                                 for v in values {
        //                                     p.hget(&pk, v);
        //                                 }
        //                             }
        //                         }
        //
        //                         let result: RedisResult<Vec<String>> =
        //                             p.query_async(&mut client_copy).await;
        //                         println!("{:?}", result);
        //
        //                         if let Ok(ok_result) = result {
        //                             if !ok_result.is_empty() {
        //                                 //TODO a type translation function should be generalised here
        //                                 let some = ok_result
        //                                     .into_iter()
        //                                     .map(|x| serde_json::from_str(x.as_str()).unwrap())
        //                                     .collect::<Vec<MValue>>();
        //
        //                                 responses.messages.push(Message::new_response(
        //                                     QueryResponse {
        //                                         matching_query: Some(qm.clone()),
        //                                         result: Some(MValue::Rows(vec![some])), //todo: Translate function
        //                                         error: None,
        //                                         response_meta: None,
        //                                     },
        //                                     true,
        //                                     RawFrame::NONE,
        //                                 ));
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //
        //                     /*
        //                     Query String: INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES ('6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47', 'KRUIKSWIJK', 'Steven')
        //                     AST: [Insert {
        //                             table_name: ObjectName(["cycling", "cyclist_name"]),
        //                             columns: ["id", "lastname", "firstname"],
        //                             source: Query {
        //                                 ctes: [],
        //                                 body: Values(
        //                                     Values(
        //                                         [[Value(SingleQuotedString("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47")), Value(SingleQuotedString("KRUIKSWIJK")), Value(SingleQuotedString("Steven"))]]
        //                                         )
        //                                       ),
        //                                       order_by: [],
        //                                       limit: None,
        //                                       offset: None,
        //                                       fetch: None }
        //                             }]
        //                     */
        //                     Insert {
        //                         table_name: _,
        //                         columns: _,
        //                         source: _,
        //                     } => {
        //                         let mut insert_values: Vec<(String, String)> = Vec::new();
        //
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(value_map) = qm.query_values.borrow() {
        //                                 for (k, v) in value_map {
        //                                     insert_values.push((
        //                                         k.clone(),
        //                                         serde_json::to_string(&v).unwrap(),
        //                                     ));
        //                                 }
        //
        //                                 let mut client_copy = self.con.clone();
        //
        //                                 //TODO: something something what happens if hset fails.
        //                                 let (cache_update, chain_r): (
        //                                     RedisResult<()>,
        //                                     ChainResponse,
        //                                 ) = tokio::join!(
        //                                     client_copy.hset_multiple(pk, insert_values.as_slice()),
        //                                     t.call_next_transform(wrapped_message)
        //                                 );
        //
        //                                 responses.messages.append(&mut chain_r?.messages);
        //
        //                                 // TODO: We update the cache asynchronously - currently errors on cache update are ignored
        //
        //                                 if let Err(e) = cache_update {
        //                                     trace!("Cache update failed {:?} !", e);
        //                                 } else {
        //                                     trace!("Cache update success !");
        //                                 }
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //                     Update {
        //                         table_name: _,
        //                         assignments: _,
        //                         selection: _,
        //                     } => {
        //                         let mut insert_values: Vec<(String, String)> = Vec::new();
        //
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(value_map) = qm.query_values.borrow() {
        //                                 for (k, v) in value_map {
        //                                     insert_values.push((
        //                                         k.clone(),
        //                                         serde_json::to_string(&v).unwrap(),
        //                                     ));
        //                                 }
        //
        //                                 let mut client_copy = self.con.clone();
        //
        //                                 //TODO: something something what happens if hset fails.
        //
        //                                 let (cache_update, chain_r): (
        //                                     RedisResult<()>,
        //                                     ChainResponse,
        //                                 ) = tokio::join!(
        //                                     client_copy.hset_multiple(pk, insert_values.as_slice()),
        //                                     t.call_next_transform(wrapped_message)
        //                                 );
        //
        //                                 // TODO: We update the cache asynchronously - currently errors on cache update are ignored
        //                                 responses.messages.append(&mut chain_r?.messages);
        //
        //                                 if let Err(e) = cache_update {
        //                                     trace!("Cache update failed {:?} !", e);
        //                                 } else {
        //                                     trace!("Cache update success !");
        //                                 }
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //                     Delete {
        //                         table_name: _,
        //                         selection: _,
        //                     } => {
        //                         let p = &mut pipe();
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(value_map) = qm.query_values.borrow() {
        //                                 for k in value_map.keys() {
        //                                     p.hdel(pk.clone(), k.clone());
        //                                 }
        //
        //                                 let mut client_copy = self.con.clone();
        //
        //                                 let (cache_update, chain_r): (
        //                                     RedisResult<Vec<i32>>,
        //                                     ChainResponse,
        //                                 ) = tokio::join!(
        //                                     p.query_async(&mut client_copy),
        //                                     t.call_next_transform(wrapped_message)
        //                                 );
        //
        //                                 // TODO: We update the cache asynchronously - currently errors on cache update are ignored
        //                                 responses.messages.append(&mut chain_r?.messages);
        //
        //                                 if let Err(e) = cache_update {
        //                                     trace!("Cache update failed {:?} !", e);
        //                                 } else {
        //                                     trace!("Cache update success !");
        //                                 }
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //                     _ => {}
        //                 }
        //             } else {
        //                 responses
        //                     .messages
        //                     .append(&mut t.call_next_transform(wrapped_message).await?.messages);
        //             }
        //         }
        //     }
        // }
        // Ok(responses)
        unimplemented!()
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use crate::message::ASTHolder;
    use crate::message::{Messages, Value as ShotoverValue};
    use crate::transforms::redis_transforms::redis_cache::{build_redis_ast_from_sql, PrimaryKey};
    use anyhow::Result;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    fn build_query(query_string: &str) -> Result<ASTHolder> {
        let dialect = GenericDialect {}; //TODO write CQL dialect
        let parsed_sql = Parser::parse_sql(&dialect, query_string.to_string())?.remove(0);
        Ok(ASTHolder::SQL(parsed_sql))
    }

    #[test]
    fn equal_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec![],
            range_key: vec![],
        };

        let query = build_redis_ast_from_sql(
            build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965")?,
            &pks,
            &pk_holder,
        );

        println!("{:#?}", query);

        Ok(())
    }

    #[test]
    fn check_deterministic_order_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec![],
            range_key: vec![],
        };

        let query_one = build_redis_ast_from_sql(
            build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965")?,
            &pks,
            &pk_holder,
        );

        let query_two = build_redis_ast_from_sql(
            build_query("SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123")?,
            &pks,
            &pk_holder,
        );

        println!("{:#?}", query_one);
        println!("{:#?}", query_two);

        // Semantically databases treat the order of AND clauses differently, Cassandra however requires clustering key predicates be in order
        // So here we will just expect the order is correct in the query. TODO: we may need to revisit this as support for other databases is added
        assert_ne!(query_one, query_two);

        Ok(())
    }

    #[test]
    fn range_exclusive_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec![],
            range_key: vec![],
        };

        let query = build_redis_ast_from_sql(
            build_query("SELECT * FROM foo WHERE z = 1 AND x > 123 AND y < 123")?,
            &pks,
            &pk_holder,
        );

        println!("{:#?}", query);

        Ok(())
    }

    #[test]
    fn range_inclusive_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec![],
            range_key: vec![],
        };

        let query = build_redis_ast_from_sql(
            build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 123")?,
            &pks,
            &pk_holder,
        );

        println!("{:#?}", query);

        Ok(())
    }
}
