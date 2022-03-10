use std::borrow::Borrow;
use std::ops::Deref;
use itertools::Itertools;
use pktparse::arp::Operation;
use regex::Regex;
use tree_sitter::{
    Language, LogType, Node, Parser, Query, QueryCapture, QueryCursor, QueryMatch, Tree, TreeCursor,
};
use crate::transforms::cassandra::cassandra_ast::RelationValue::COL;

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraStatement {
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
    DeleteStatement(DeleteStatementData),
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
    InsertStatement(InsertStatementData),
    ListPermissions,
    ListRoles,
    Revoke,
    SelectStatement(SelectStatementData),
    Truncate(String),
    Update,
    UseStatement(String),
    UNKNOWN(String),
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeleteColumn {
    column : String,
    value : Option<String>,
}

impl ToString for DeleteColumn {
    fn to_string(&self) -> String {
        match &self.value {
            Some(x) => format!( "{} [{}]", self.column, x),
            None => self.column.clone(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeleteStatementData {
    pub modifiers : StatementModifiers,
    pub begin_batch : Option<BeginBatch>,
    pub columns: Option<Vec<DeleteColumn>>,
    pub table_name: String,
    pub timestamp : Option<u64>,
    pub where_clause : Vec<RelationElement>,
    pub if_spec : Option<Vec<(String,String)>>,
}

impl ToString for DeleteStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        if self.begin_batch.is_some() {
            result.push_str( self.begin_batch.as_ref().unwrap().to_string().as_str() );
        }
        result.push_str( "DELETE ");
        if self.columns.is_some() {
            result.push( '(' );
            result.push_str( self.columns.as_ref().unwrap().iter().map(|x| x.to_string()).join(",").as_str());
            result.push_str( ") " );
        }
        result.push_str( "FROM ");
        result.push_str( &self.table_name.as_str() );
        if self.timestamp.is_some() {
            result.push_str( format!("USING TIMESTAMP {} ", self.timestamp.unwrap()).as_str());
        }
        result.push_str( "WHERE ");
        result.push_str( self.where_clause.iter().map( |w| w.to_string() ).join(" AND ").as_str());

        if self.if_spec.is_some() {
            result.push_str( " IF ");
            result.push_str(self.if_spec.as_ref().unwrap().iter().map( |(x,y)| format!("{} = {}", x, y)).join( " AND ").as_str());
        } else  if self.modifiers.exists {
            result.push_str( " IF EXISTS");
        }

        result
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct InsertStatementData {
    pub begin_batch : Option<BeginBatch>,
    pub modifiers : StatementModifiers,
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Option<InsertValues>,
    pub using_ttl : Option<TtlTimestamp>
}

impl ToString for InsertStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        if self.begin_batch.is_some() {
            result.push_str( self.begin_batch.as_ref().unwrap().to_string().as_str() );
        }
        result.push_str( "INSERT INTO ");
        result.push_str( &self.table_name.as_str() );
        if self.columns.is_some() {
            result.push_str( " (" );
            result.push_str( self.columns.as_ref().unwrap().iter().join(",").as_str());
            result.push( ')' );
        }
        result.push( ' ');
        result.push_str( self.values.as_ref().unwrap().to_string().as_str() );
        if self.modifiers.not_exists {
            result.push_str( " IF EXISTS");
        }
        if self.using_ttl.is_some() {
            result.push_str( self.using_ttl.as_ref().unwrap().to_string().as_str());
        }
        result
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct TtlTimestamp {
    ttl : u64,
    timestamp : u64,
}

impl ToString for TtlTimestamp {
    fn to_string(&self) -> String {
        format!(" USING TTL {} AND TIMESTAMP {}", self.ttl, self.timestamp)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct BeginBatch {
    logged: bool,
    unlogged: bool,
    timestamp: Option<u64>,
}

impl BeginBatch {
    pub fn new() -> BeginBatch {
        BeginBatch {
            logged: false,
            unlogged: false,
            timestamp: None,
        }
    }
}
impl ToString for BeginBatch {
    fn to_string(&self) -> String {
        let mut result = format!( "BEGIN{}BATCH ", if self.logged {
            " LOGGED "
        } else if self.unlogged {
            " UNLOGGED "
        } else {
            " "
        }).to_string();
        if (self.timestamp.is_some()) {
            result.push_str( format!( "USING TIMESTAMP {} ", self.timestamp.unwrap()).as_str());
        }
        result
    }

}

#[derive(PartialEq, Debug, Clone)]
pub enum InsertValues {
    VALUES(Vec<InsertExpression>),
    JSON(String),
}

impl ToString for InsertValues {
    fn to_string(&self) -> String {
        match self {
            InsertValues::VALUES(columns) => {
                let mut result = String::from( " VALUES ");
                result.push_str(columns.iter().map( |c| c.to_string() ).join(",").as_str());
                result
            },
            InsertValues::JSON(text) => {
                format!( "JSON {}", text ).to_string()
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum InsertExpression {
    CONST(String),
    MAP(Vec<(String,String)>),
    SET(Vec<String>),
    LIST(Vec<String>),
    TUPLE(Vec<InsertExpression>)
}

impl ToString for InsertExpression {
    fn to_string(&self) -> String {
        match self {
            InsertExpression::CONST(text) => text.clone(),
            InsertExpression::MAP(entries) => {
                let mut result = String::from('{');
                result.push_str(entries.iter().map( |(x,y)| format!("{}:{}", x, y )).join(",").as_str());
                result.push('}');
                result
            },
            InsertExpression::SET(values) => {
                let mut result = String::from('{');
                result.push_str(values.iter().join(",").as_str());
                result.push('}');
                result
            },
            InsertExpression::LIST(values) => {
                let mut result = String::from('[');
                result.push_str(values.iter().join(",").as_str());
                result.push(']');
                result
            },
            InsertExpression::TUPLE(values) => {
                let mut result = String::from('(');
                result.push_str(values.iter().map( |v| v.to_string()).join(",").as_str());
                result.push(')');
                result
            },
        }
    }
}



/*
        assignment_map : $ => seq("{", commaSep1( seq( $.constant, ":", $.constant)),"}"),
        assignment_set : $ => seq("{", optional( commaSep1( $.constant ) ),"}"),
        assignment_list : $  => seq( "[", commaSep1( $.constant ), "]"),

        assignment_tuple : $ =>
            seq(
                "(",
                $.constant,
                choice(
                    repeat( seq( ",", $.constant)),
                    repeat( seq( ",", $.assignment_tuple)),
                    commaSep1( $.assignment_tuple ),
                ),
                ")",
            ),

        assignment_map : $ => seq("{", commaSep1( seq( $.constant, ":", $.constant)),"}"),
        assignment_set : $ => seq("{", optional( commaSep1( $.constant ) ),"}"),
        assignment_list : $  => seq( "[", commaSep1( $.constant ), "]"),
*/

#[derive(PartialEq, Debug, Clone)]
pub struct SelectStatementData {
    pub modifiers : StatementModifiers,
    pub table_name: String,
    pub elements : Vec<SelectElement>,
    pub where_clause : Vec<RelationElement>,
    pub order : Option<OrderClause>,
}

impl SelectStatementData {
    /// return the column names selected
    pub fn select_names(&self ) -> Vec<String> {
        self.elements.iter().map( |e| match e {
            SelectElement::STAR => None,
            SelectElement::DOT_STAR(_) => None,
            SelectElement::COLUMN(named) => Some(named.name.clone()),
            SelectElement::FUNCTION(_) => None,
        }).filter( |e| e.is_some() ).map( |e| e.unwrap()).collect()
    }

    /// return the aliased column names.  If the column is not aliased the
    /// base column name is returned.
    pub fn select_alias(&self ) -> Vec<String> {
        self.elements.iter().map( |e| match e {
            SelectElement::COLUMN( named ) => if named.alias.is_some() {
                named.alias.clone()
                //Some(named.alias..as_ref().unwrap().clone())
            } else {
                Some(named.name.clone())
            },
            _ => None
        }).filter( |e| e.is_some()).map(|e| e.unwrap()).collect()
    }
    /// return the column names selected
    pub fn where_columns(&self ) -> Vec<String> {
        self.where_clause.iter().map( |e| match &e.obj {
            COL(name) => Some(name.clone()),
            _ => None,
        }).filter( |e| e.is_some()).map( |e| e.unwrap()).collect()
    }
}

impl ToString for SelectStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        result.push_str( "SELECT ");
        if self.modifiers.distinct {
            result.push_str( "DISTINCT ");
        }
        if self.modifiers.json {
            result.push_str( "JSON ");
        }
        result.push_str( self.elements.iter().map( |e| e.to_string() ).join(",").as_str() );
        result.push_str( " FROM ");
        result.push_str( self.table_name.as_str() );
        result.push_str( " WHERE ");
        result.push_str( self.where_clause.iter().map( |w| w.to_string() ).join(" AND ").as_str());
        if self.modifiers.limit.is_some() {
            result.push_str( self.modifiers.limit.unwrap().to_string().as_str() );
        }
        if self.modifiers.filtering {
            result.push_str( " ALLOW FILTERING");
        }
        result
    }
}


#[derive(PartialEq, Debug, Clone)]
pub enum SelectElement {
    STAR,
    DOT_STAR(String),
    COLUMN(Named),
    FUNCTION(Named),
}

impl ToString for SelectElement {
    fn to_string(&self) -> String {
        match self {
            SelectElement::STAR => String::from("*"),
            SelectElement::DOT_STAR(column) => format!( "{}.*", column).to_string(),
            SelectElement::COLUMN( named ) |
            SelectElement::FUNCTION(named) => named.to_string(),
        }
    }
}


#[derive(PartialEq, Debug, Clone)]
pub struct Named {
    pub(crate) name: String,
    pub(crate) alias: Option<String>,
}

impl ToString for Named {
    fn to_string(&self) -> String {
        match &self.alias {
            None => self.name.clone(),
            Some(a) => format!("{}.{}", a, self.name).to_string(),
        }
    }
}


#[derive(PartialEq, Debug, Clone)]
pub struct OrderClause {
    name : String,
    desc : bool,
}

#[derive(PartialEq, Debug, Clone)]
pub struct RelationElement {
    /// the column, function or column list on the left side
    pub obj : RelationValue,
    /// the relational operator
    pub oper : RelationOperator,
    /// the value, func, argument list, tuple list or tuple
    pub value : RelationValue,
}

impl ToString for RelationElement {
    fn to_string(&self) -> String {
        format!( "{} {} {}", self.obj.to_string(), self.oper.to_string(), self.value.to_string()).to_string()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RelationValue {
    CONST(String),
    FUNC(String),
    COL(String),
    LIST(Vec<RelationValue>),
}
impl ToString for RelationValue {
    fn to_string(&self) -> String {
        match self {
            RelationValue::CONST( s ) |
            RelationValue::FUNC(s) |
            COL(s) => s.clone(),
            RelationValue::LIST( lst ) => {
                let mut result = String::from( "(");
                result.push_str( lst.iter().map( |e| e.to_string() ).join(" AND ").as_str());
                result.push( ')');
                result
            }
        }
    }
}


#[derive(PartialEq, Debug, Clone)]
pub enum RelationOperator {
    LT,
    LE,
    EQ,
    NE,
    GE,
    GT,
    IN,
    CONTAINS(String),
    CONTAINS_KEY(String),
}

impl ToString for RelationOperator {
    fn to_string(&self) -> String {
        match self {
            RelationOperator::LT => String::from("<"),
            RelationOperator::LE => String::from( "<="),
            RelationOperator::EQ => String::from("="),
            RelationOperator::NE => String::from("<>"),
            RelationOperator::GE => String::from(">="),
            RelationOperator::GT => String::from(">"),
            RelationOperator::IN => String::from("IN"),
            RelationOperator::CONTAINS( s ) => format!( "CONTAINS {}", s).to_string(),
            RelationOperator::CONTAINS_KEY(s) => format!( "CONTAINS KEY {}", s).to_string(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct StatementModifiers {
    distinct : bool,
    json : bool,
    limit : Option<i32>,
    filtering : bool,
    not_exists : bool,
    exists : bool,
}

impl StatementModifiers {
    pub fn new() -> StatementModifiers {
        StatementModifiers {
            distinct : false,
            json : false,
            limit : None,
            filtering : false,
            not_exists : false,
            exists : false,
        }
    }
}

struct NodeFuncs {}

impl NodeFuncs {
    pub fn as_string(node :&Node, source : &String) -> String {
        node.utf8_text(source.as_bytes()).unwrap().to_string()
    }
}
impl CassandraStatement {
    pub fn from_node(node: &Node, source: &String) -> CassandraStatement {
        let kind = node.kind();
        match kind {
            "alter_keyspace" => CassandraStatement::AlterKeyspace,
            "alter_materialized_view" => CassandraStatement::AlterMaterializedView,
            "alter_role" => CassandraStatement::AlterRole,
            "alter_table" => CassandraStatement::AlterTable,
            "alter_type" => CassandraStatement::AlterType,
            "alter_user" => CassandraStatement::AlterUser,
            "apply_batch" => CassandraStatement::ApplyBatch,
            "create_aggregate" => CassandraStatement::CreateAggregate,
            "create_function" => CassandraStatement::CreateFunction,
            "create_index" => CassandraStatement::CreateIndex,
            "create_keyspace" => CassandraStatement::CreateKeyspace,
            "create_materialized_view" => CassandraStatement::CreateMaterializedView,
            "create_role" => CassandraStatement::CreateRole,
            "create_table" => CassandraStatement::CreateTable,
            "create_trigger" => CassandraStatement::CreateTrigger,
            "create_type" => CassandraStatement::CreateType,
            "create_user" => CassandraStatement::CreateUser,
            "delete_statement" => CassandraStatement::DeleteStatement( CassandraParser::build_delete_statement(node, source)),
            "drop_aggregate" => CassandraStatement::DropAggregate,
            "drop_function" => CassandraStatement::DropFunction,
            "drop_index" => CassandraStatement::DropIndex,
            "drop_keyspace" => CassandraStatement::DropKeyspace,
            "drop_materialized_view" => CassandraStatement::DropMaterializedView,
            "drop_role" => CassandraStatement::DropRole,
            "drop_table" => CassandraStatement::DropTable,
            "drop_trigger" => CassandraStatement::DropTrigger,
            "drop_type" => CassandraStatement::DropType,
            "drop_user" => CassandraStatement::DropUser,
            "grant" => CassandraStatement::Grant,
            "insert_statement" => CassandraStatement::InsertStatement( CassandraParser::build_insert_statement(node, source)),
            "list_permissions" => CassandraStatement::ListPermissions,
            "list_roles" => CassandraStatement::ListRoles,
            "revoke" => CassandraStatement::Revoke,
            "select_statement" => CassandraStatement::SelectStatement( CassandraParser::build_select_statement(node, source)),
            "truncate" => {
                let mut cursor = node.walk();
                cursor.goto_first_child();
                // consume until 'table_name'
                while !cursor.node().kind().eq( "table_name") {
                    cursor.goto_next_sibling();
                }
                CassandraStatement::Truncate( CassandraParser::parse_table_name( &cursor.node(), source ))
            },
            "update" => CassandraStatement::Update,
            "use" => {
                let mut cursor = node.walk();
                cursor.goto_first_child();
                // consume 'USE'
                if cursor.goto_next_sibling() {
                    CassandraStatement::UseStatement(NodeFuncs::as_string(&cursor.node(), source))
                } else {
                    CassandraStatement::UNKNOWN("Keyspace not provided with USE statement".to_string())
                }
            },
            _ => CassandraStatement::UNKNOWN(node.kind().to_string()),
        }
    }
}

struct CassandraParser{}
impl CassandraParser {

    pub fn build_delete_statement( node : &Node, source : &String) -> DeleteStatementData {
        /*
                optional( $.begin_batch ),
                kw("DELETE"),
                optional( $.delete_column_list ),
                $.from_spec,
                optional( $.using_timestamp_spec),
                $.where_spec,
                optional( choice( if_exists, $.if_spec))
         */
        let mut statement_data = DeleteStatementData {
            begin_batch : None,
            modifiers : StatementModifiers::new(),
            table_name: String::from(""),
            columns: None,
            timestamp: None,
            where_clause : vec!(),
            if_spec : None,
        };

        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            let mut node = cursor.node();
            let mut kind = node.kind();
            match kind {
                "begin_batch" =>
                    statement_data.begin_batch = Some(CassandraParser::parse_begin_batch(&cursor.node(), source)),
                "delete_column_list" => {
                    // goto delete_column_item
                    let mut delete_columns = vec!();
                    process = cursor.goto_first_child();
                    while process {
                        delete_columns.push(CassandraParser::parse_delete_column_item(&cursor.node(), source));
                        process = cursor.goto_next_sibling();
                    }
                    // bring the cursor back to delete_column_list
                    cursor.goto_parent();
                    statement_data.columns = Some(delete_columns);
                },
                "from_spec" => {
                    statement_data.table_name = CassandraParser::parse_from_spec( &cursor.node(), source );
                },
                "using_timestamp_spec" => {
                    statement_data.timestamp = CassandraParser::parse_using_timestamp( &cursor.node(), source );
                }
                "where_spec" => {
                    statement_data.where_clause = CassandraParser::parse_where_spec( &cursor.node(), source );
                },
                "IF" => {
                    // consume EXISTS
                    cursor.goto_next_sibling();
                    statement_data.modifiers.exists = true;
                },
                "if_spec" => {
                    cursor.goto_first_child();
                    // consume IF
                    cursor.goto_next_sibling();
                    statement_data.if_spec = CassandraParser::parse_if_condition_list( &cursor.node(), source );
                    cursor.goto_parent();
                }
                _ => {}

            }
            process = cursor.goto_next_sibling();
        }
        statement_data

    }

    fn parse_if_condition_list( node : &Node, source : &String ) -> Option<Vec<(String,String)>> {
        let mut result = vec!();
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        while process {
            cursor.goto_first_child();
            let column = NodeFuncs::as_string( &cursor.node(), &source);
            // consume the '='
            cursor.goto_next_sibling();
            cursor.goto_next_sibling();
            let value = NodeFuncs::as_string( &cursor.node(), &source);
            result.push( (column,value) );
            cursor.goto_parent();
            process = cursor.goto_next_sibling();
            if process {
                // we found 'AND' so get real next node
                cursor.goto_next_sibling();
            }
        }
        Some(result)
    }

    fn parse_delete_column_item(node : &Node, source : &String) -> DeleteColumn {
        let mut result: Vec<String> = vec!();
        let mut cursor = node.walk();
        cursor.goto_first_child();
        DeleteColumn {
            column: NodeFuncs::as_string(&cursor.node(), &source),

            value: if cursor.goto_next_sibling() {
                // consume '['
                cursor.goto_next_sibling();
                Some(NodeFuncs::as_string(&cursor.node(), &source))
            } else {
                None
            },
        }
    }


    pub fn build_insert_statement( node : &Node, source : &String ) -> InsertStatementData {
        /*
         optional( $.begin_batch),
                kw("INSERT"),
                kw("INTO"),
                $.table_name,
                optional( $.insert_column_spec ),
                $.insert_values_spec,
                optional( if_not_exists ),
                optional( $.using_ttl_timestamp )
         */
        let mut statement_data = InsertStatementData {
            begin_batch : None,
            modifiers : StatementModifiers::new(),
            table_name: String::from(""),
            columns: None,
            values: None,
            using_ttl : None,
        };

        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            match cursor.node().kind() {
                "begin_batch" =>
                    statement_data.begin_batch = Some(CassandraParser::parse_begin_batch(&cursor.node(), source)),
                "table_name" => {
                    statement_data.table_name = CassandraParser::parse_table_name(&cursor.node(), source );
                },
                "insert_column_spec" => {
                    cursor.goto_first_child();
                    // now on column_list
                    statement_data.columns = Some(CassandraParser::parse_column_list(&cursor.node(), source));
                    cursor.goto_parent();
                },
                "insert_values_spec" => {
                    cursor.goto_first_child();
                    match cursor.node().kind() {
                        "VALUES" => {
                            cursor.goto_next_sibling();
                            let expression_list = CassandraParser::parse_expression_list( &cursor.node(), source );
                            statement_data.values = Some(InsertValues::VALUES(expression_list));
                        }
                        "JSON" => {
                            cursor.goto_next_sibling();
                            statement_data.values= Some(InsertValues::JSON( NodeFuncs::as_string( &cursor.node(), source ) ));
                        }
                        _ => {}
                    }
                    cursor.goto_parent();
                },
                "IF" => {
                    // consume NOT
                    cursor.goto_next_sibling();
                    // consume EXISTS
                    cursor.goto_next_sibling();
                    statement_data.modifiers.not_exists = true;
                },
                "using_ttl_timestamp" => {
                    statement_data.using_ttl = Some(CassandraParser::parse_ttl_timestamp(&cursor.node(), source));
                }
            _ => {}

            }
        }
        statement_data
    }

    // on column_list
    fn parse_column_list( node : &Node, source : &String ) -> Vec<String> {
        let mut result:Vec<String> = vec!();
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // we are now on '('
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("column") {
                result.push(NodeFuncs::as_string(&cursor.node(), &source));
            }
        }
        result
    }

    fn parse_using_timestamp( node : &Node, source : &String) -> Option<u64> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "USING"
        cursor.goto_next_sibling();
        // consume TIMESTAMP
        cursor.goto_next_sibling();
        Some(NodeFuncs::as_string(&cursor.node(), &source).parse::<u64>().unwrap())
    }

    fn parse_ttl_timestamp( node : &Node, source : &String ) -> TtlTimestamp {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "USING"
        let mut ttl :Option<u64>= None;
        let mut timestamp : Option<u64> = None;
        while (ttl.is_none() || timestamp.is_none()) && cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "ttl" => {
                    // consume "TTL"
                    cursor.goto_next_sibling();
                    ttl = Some(NodeFuncs::as_string(&cursor.node(), source ).parse::<u64>().unwrap());
                },
                "timestamp" => {
                    // consume TIMESTAMP
                    cursor.goto_next_sibling();
                    timestamp = Some(NodeFuncs::as_string(&cursor.node(),source).parse::<u64>().unwrap());
                },
                _ => {},
            }
        }
        TtlTimestamp{ ttl: ttl.unwrap(), timestamp: timestamp.unwrap() }
    }

    fn parse_from_spec( node : &Node, source : &String) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'FROM'
        cursor.goto_next_sibling();
        CassandraParser::parse_table_name( &cursor.node(), &source )
    }

    fn parse_table_name( node :&Node, source : &String) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = NodeFuncs::as_string(&cursor.node(), source);
        if cursor.goto_next_sibling() {
            // we have fully qualified name
            result.push('.');
            // consume '.'
            cursor.goto_next_sibling();
            result.push_str( NodeFuncs::as_string(&cursor.node(), source).as_str());
        }
        result
    }

    fn parse_expression_list( node : &Node, source : &String ) -> Vec<InsertExpression> {
        let mut result = vec!();
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            let mut kind = cursor.node().kind();
            if kind.eq("expression") {
                result.push(CassandraParser::parse_expression(&cursor.node(), source));
            }
            process = cursor.goto_next_sibling();
        }
        result
    }

    fn parse_expression(  node : &Node, source : &String) -> InsertExpression {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        match cursor.node().kind() {
            "assignment_map" =>
                InsertExpression::MAP( CassandraParser::parse_assignment_map( &cursor.node(), source)),
            "assignment_list" =>
                InsertExpression::LIST(CassandraParser::parse_assignment_list( &cursor.node(), source)),
            "assignment_set" =>
                InsertExpression::SET(CassandraParser::parse_assignment_set( &cursor.node(), source)),
            "assignment_tuple" =>
                InsertExpression::TUPLE(CassandraParser::parse_assignment_tuple(&cursor.node(), source)),
            "constant" => InsertExpression::CONST( NodeFuncs::as_string( &cursor.node(), source )),
            _ => InsertExpression::CONST( NodeFuncs::as_string( &cursor.node(), source )),
        }
    }

    fn parse_assignment_map(  node : &Node, source : &String) -> Vec<(String, String)>{
        let mut cursor = node.walk();
        // { const : const, ... }
        let mut entries: Vec<(String, String)> = vec!();
        // we are on the '{' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "}" | "," => {},
                _ => {
                    let key = NodeFuncs::as_string(&cursor.node(), &source);
                    // consume the ':'
                    cursor.goto_next_sibling();
                    cursor.goto_next_sibling();
                    let value = NodeFuncs::as_string(&cursor.node(), &source);
                    entries.push((key, value));
                }
            }
        }
       entries

    }

    fn parse_assignment_list( node : &Node, source : &String ) -> Vec<String> {
        let mut cursor = node.walk();
        // [ const, const, ... ]
        let mut entries: Vec<String> = vec!();
        // we are on the '[' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "]" | "," => {},
                _ => {
                    entries.push(NodeFuncs::as_string(&cursor.node(), &source));
                }
            }
        }
        entries
    }

    fn parse_assignment_set( node : &Node, source : &String ) -> Vec<String> {
        let mut cursor = node.walk();
        // { const, const, ... }
        let mut entries: Vec<String> = vec!();
        // we are on the '{' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "}" | "," => {},
                _ => {
                    entries.push(NodeFuncs::as_string(&cursor.node(), &source));
                }
            }
        }
        entries

    }

    fn parse_assignment_tuple(  node : &Node, source : &String ) -> Vec<InsertExpression> {
        let mut result = vec!();
        // ( expression, expresson ... )
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        while process {
            if cursor.node().kind().eq("expression") {
                result.push( CassandraParser::parse_expression( &cursor.node(), source ));
            }
            process = cursor.goto_next_sibling();
        };
        result
    }
    /// walker on "begin_batch"
    fn parse_begin_batch(  node : &Node, source : &String) -> BeginBatch {
        let mut result = BeginBatch::new();

        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume BEGIN
        cursor.goto_next_sibling();

        let mut node = cursor.node();
        result.logged = node.kind().eq( "LOGGED");
        result.unlogged = node.kind().eq( "UNLOGGED");
        if result.logged || result.unlogged {
            // used a node so advance
            cursor.goto_next_sibling();
        }
        // consume BATCH
        if cursor.goto_next_sibling() {
            // we should have using_timestamp_spec
            result.timestamp = CassandraParser::parse_using_timestamp( &cursor.node(), source )
        }

        result
    }

    pub fn build_select_statement( node : &Node, source : &String) -> SelectStatementData {
        /*
        seq(
                kw("SELECT"),
                optional( kw("DISTINCT")),
                optional( kw("JSON") ),
                $.select_elements,
                $.from_spec,
                optional($.where_spec),
                optional($.order_spec),
                optional($.limit_spec ),
                optional(seq( kw("ALLOW"), kw("FILTERING"))),
            ),
         */
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let mut statement_data = SelectStatementData {
            modifiers: StatementModifiers::new(),
            elements: vec!(),
            table_name: String::new(),
            where_clause: vec!(),
            order : None,
        };
        // we are on SELECT so we can just start
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "DISTINCT" => statement_data.modifiers.distinct = true,
                "JSON" => statement_data.modifiers.json = true,
                "select_elements" => {
                    let mut process = cursor.goto_first_child();
                    while process {
                        let kind = cursor.node().kind();
                        match cursor.node().kind() {
                            "select_element" => statement_data.elements.push(CassandraParser::parse_select_element(&cursor.node(), &source)),
                            _ => {},
                        }
                        process = cursor.goto_next_sibling();
                    }
                    cursor.goto_parent();
                },
                "from_spec" => statement_data.table_name = CassandraParser::parse_from_spec(&cursor.node(), source),
                "where_spec" => statement_data.where_clause = CassandraParser::parse_where_spec(&cursor.node(), source),
                "order_spec" => statement_data.order = CassandraParser::parse_order_spec(&cursor.node(), source),
                "limit_spec" => {
                    cursor.goto_first_child();
                    // consume LIMIT
                    cursor.goto_next_sibling();
                    statement_data.modifiers.limit=Some(NodeFuncs::as_string(&cursor.node(), &source).parse::<i32>().unwrap());
                    cursor.goto_parent();
                },
                "ALLOW" => {
                    // consume 'FILTERING'
                    cursor.goto_next_sibling();
                    statement_data.modifiers.filtering = true
                },
                _ => {}
            }
        }
        return statement_data;
    }

    fn parse_where_spec(  node : &Node, source : &String ) -> Vec<RelationElement> {
        // (where_spec (relation_elements (relation_element (constant))))
        let mut result = vec!();
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume the "WHERE"
        cursor.goto_next_sibling();
        // now on relation_elements.
        cursor.goto_first_child();
        // now on first relation.
        result.push( CassandraParser::parse_relation_element( &cursor.node(), source ));
        result
    }

    fn parse_relation_element( node : &Node, source : &String ) -> RelationElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        RelationElement {
            obj: { let x = CassandraParser::parse_relation_value(&mut cursor, source);
                cursor.goto_next_sibling();
                x},
            oper: { let x = CassandraParser::parse_operator(&mut cursor, source);
                cursor.goto_next_sibling();
                x },
            value: CassandraParser::parse_relation_value( &mut cursor, source),
        }
    }
    /// walker positioned before operator symbol
    fn parse_operator(  cursor : &mut TreeCursor, source : &String ) -> RelationOperator {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "<" => RelationOperator::LT,
            "<=" => RelationOperator::LE,
            "<>" => RelationOperator::NE,
            "=" => RelationOperator::EQ,
            ">=" => RelationOperator::GE,
            ">" => RelationOperator::GT,
            "IN" => RelationOperator::IN,
            "CONTAINS" => {
                cursor.goto_next_sibling();
                if cursor.node().kind().eq("KEY") {
                    // consume the "KEY"
                    cursor.goto_next_sibling();
                    RelationOperator::CONTAINS_KEY( NodeFuncs::as_string(&cursor.node(), &source))
                } else {
                    RelationOperator::CONTAINS( NodeFuncs::as_string(&cursor.node(), &source ))
                }
            },

            _ => {
                // can not happen -- so this is a nasty result
                let mut s = "Unknown operator: ".to_string();
                s.push_str( kind );
                RelationOperator::CONTAINS( s )
            },
        }
    }
    fn parse_relation_value( cursor : &mut TreeCursor, source : &String ) -> RelationValue {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "column" => RelationValue::COL( NodeFuncs::as_string( &node, &source )),
            "function_call" => RelationValue::FUNC(NodeFuncs::as_string( &node, &source)),
            "(" => {
                let mut values: Vec<RelationValue> = Vec::new();
                // consume '('
                cursor.goto_next_sibling();
                while !cursor.node().kind().eq(")") {
                    values.push(CassandraParser::parse_relation_value( cursor, source));
                    cursor.goto_next_sibling();
                }
                RelationValue::LIST(values)
            },
            _ => RelationValue::CONST( NodeFuncs::as_string( &node, source) ),
        }
    }

    fn parse_order_spec(  node : &Node, source : &String) -> Option<OrderClause> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "ORDER"
        cursor.goto_next_sibling();
        // consume "BY"
        cursor.goto_next_sibling();
        Some(OrderClause {
            name: NodeFuncs::as_string( &cursor.node(), &source ),
            desc: {
                if cursor.goto_next_sibling() {
                    cursor.node().kind().eq("DESC")
                } else {
                    false
                }
            },
        })
    }

    /// walker on "select_element"
    fn parse_select_element(  node : &Node, source : &String )  -> SelectElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let type_ = cursor.node();

        let alias = if cursor.goto_next_sibling() {
            // we have an alias
            // consume 'AS'
            cursor.goto_next_sibling();
            Some(NodeFuncs::as_string( &cursor.node(), source ))
        }  else {
            None
        };
        match type_.kind() {
            "column" =>
                    SelectElement::COLUMN(
                        Named {
                         name : NodeFuncs::as_string( &type_, source ),
                         alias
                        }
                    ),
            "function_call" => SelectElement::FUNCTION(
                        Named {
                            name: NodeFuncs::as_string(&type_, source),
                            alias
                        }),
            "*" => SelectElement::STAR,
            _ => {
                SelectElement::DOT_STAR( NodeFuncs::as_string(&type_, source ))
            },
        }
    }
}

impl ToString for CassandraStatement {
    fn to_string(&self) -> String {
        // TODO remove this
        let unimplemented = String::from("Unimplemented");
        match self {
            CassandraStatement::AlterKeyspace => unimplemented,
            CassandraStatement::AlterMaterializedView => unimplemented,
            CassandraStatement::AlterRole => unimplemented,
            CassandraStatement::AlterTable => unimplemented,
            CassandraStatement::AlterType => unimplemented,
            CassandraStatement::AlterUser => unimplemented,
            CassandraStatement::ApplyBatch => String::from( "APPLY BATCH"),
            CassandraStatement::CreateAggregate => unimplemented,
            CassandraStatement::CreateFunction => unimplemented,
            CassandraStatement::CreateIndex => unimplemented,
            CassandraStatement::CreateKeyspace => unimplemented,
            CassandraStatement::CreateMaterializedView => unimplemented,
            CassandraStatement::CreateRole => unimplemented,
            CassandraStatement::CreateTable => unimplemented,
            CassandraStatement::CreateTrigger => unimplemented,
            CassandraStatement::CreateType => unimplemented,
            CassandraStatement::CreateUser => unimplemented,
            CassandraStatement::DeleteStatement(statement_data) => statement_data.to_string(),
            CassandraStatement::DropAggregate => unimplemented,
            CassandraStatement::DropFunction => unimplemented,
            CassandraStatement::DropIndex => unimplemented,
            CassandraStatement::DropKeyspace => unimplemented,
            CassandraStatement::DropMaterializedView => unimplemented,
            CassandraStatement::DropRole => unimplemented,
            CassandraStatement::DropTable => unimplemented,
            CassandraStatement::DropTrigger => unimplemented,
            CassandraStatement::DropType => unimplemented,
            CassandraStatement::DropUser => unimplemented,
            CassandraStatement::Grant => unimplemented,
            CassandraStatement::InsertStatement(statement_data) => statement_data.to_string(),
            CassandraStatement::ListPermissions => unimplemented,
            CassandraStatement::ListRoles => unimplemented,
            CassandraStatement::Revoke => unimplemented,
            CassandraStatement::SelectStatement(statement_data) => statement_data.to_string(),
            CassandraStatement::Truncate(table) => format!( "TRUNCATE TABLE {}", table ).to_string(),
            CassandraStatement::Update => unimplemented,
            CassandraStatement::UseStatement(keyspace) => format!("USE {}",keyspace).to_string(),
            CassandraStatement::UNKNOWN(_) => unimplemented,
        }
    }
}
pub struct CassandraAST {
    /// The query string
    text: String,
    /// the tree-sitter tree
    pub(crate) tree: Tree,
    /// the statement type of the query
    pub statement: CassandraStatement,
    /// The default keyspace if set.  Used when keyspace not specified in query.
    default_keyspace: Option<String>,
}

impl CassandraAST {
    /// create an AST from the query string
    pub fn new(cassandra_statement: String) -> CassandraAST {
        let language = tree_sitter_cql::language();
        let mut parser = tree_sitter::Parser::new();
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
        let tree = parser.parse(&cassandra_statement, None).unwrap();

        CassandraAST {
            statement: if tree.root_node().has_error() {
                CassandraStatement::UNKNOWN(cassandra_statement.clone())
            } else {
                CassandraAST::extract_statement(&tree, &cassandra_statement)
            },
            default_keyspace: None,
            text: cassandra_statement,
            tree,
        }
    }

    /// returns true if the parsing exposed an error in the query
    pub fn has_error(&self) -> bool {
        self.tree.root_node().has_error()
    }

    /// retrieves the query value for the node (word or phrase enclosed by the node)
    pub fn node_text(&self, node: &Node) -> String {
        node.utf8_text(&self.text.as_bytes()).unwrap().to_string()
    }
/*
    ///
    /// Gets the fully qualified table name specified in the query.
    ///
    /// Returns an empty string if no table name is specified.
    ///
    ///  * `default_keyspace` is the defined default keyspace for the execution context.
    /// If the query does not specify a keyspace for the table this one will be prepended.
    /// If it is `None` then the "naked" table name is returned.
    pub fn get_table_name(&self, default_keyspace: &Option<String>) -> String {
        let nodes = self.search("table_name");
        match nodes.first() {
            None => "".to_string(),
            Some(node) => {
                let candidate_name = self.node_text(node);
                if candidate_name.contains(".") {
                    candidate_name.to_string()
                } else {
                    match default_keyspace {
                        None => candidate_name,
                        Some(keyspace) => format!("{}.{}", keyspace, candidate_name),
                    }
                }
            }
        }
    }
    */

/*
    /// Retrieves all the nodes that match the end of the path.
    ///
    /// * `path` the path to search for.  see `SearchPattern` for explanation of path structure.
    ///
    /// returns a vector of matching nodes.
    ///
    pub fn search<'a>(&'a self, path: &'static str) -> Box<Vec<Node<'a>>> {
        CassandraAST::search_cursor(self.tree.walk(), path)
    }

    /// Retrieves all the nodes that match the end of the path starting at the specified node.
    ///
    /// * `node` The node to start searching from.
    /// * `path` the path to search for.  see `SearchPattern` for explanation of path structure.
    ///
    /// returns a vector of matching nodes.
    pub fn search_node<'a>(node: &'a Node, path: &'static str) -> Box<Vec<Node<'a>>> {
        let mut nodes = Box::new(vec![*node]);
        for segment in path.split('/').map(|tok| tok.trim()) {
            let mut found_nodes = Box::new(Vec::new());
            let pattern = SearchPattern::from_str(segment);
            for node in nodes.iter() {
                CassandraAST::_find(&mut found_nodes, &mut node.walk(), &pattern);
            }
            nodes = found_nodes;
        }
        nodes
    }


    /// Retrieves all the nodes that match the end of the path starting at the specified node.
    ///
    /// * `node` The node to start searching from.
    /// * `path` the path to search for.  see `SearchPattern` for explanation of path structure.
    ///
    /// returns a vector of matching nodes.
    pub fn search_cursor<'a>(cursor : TreeCursor<'a>, path: &'static str) -> Box<Vec<Node<'a>>> {
        let mut nodes = Box::new(vec![cursor.node()]);
        for segment in path.split('/').map(|tok| tok.trim()) {
            let mut found_nodes = Box::new(Vec::new());
            let pattern = SearchPattern::from_str(segment);
            for node in nodes.iter() {
                CassandraAST::_find(&mut found_nodes, &mut node.walk(), &pattern);
            }
            nodes = found_nodes;
        }
        nodes
    }

    // performs a recursive search in the tree
    fn _find<'a>(
        nodes: & mut Vec<Node<'a>>,
        cursor: & mut TreeCursor<'a>,
        pattern: &SearchPattern,
    ) {
        let node = cursor.node();
        if pattern.name.is_match(node.kind()) {
            match &pattern.child {
                None => nodes.push(node),
                Some(child) => {
                    if CassandraAST::_has(cursor, child) {
                        nodes.push(node);
                    }
                }
            }
        } else {
            if cursor.goto_first_child() {
                CassandraAST::_find(nodes, cursor, pattern);
                while cursor.goto_next_sibling() {
                    CassandraAST::_find(nodes, cursor, pattern);
                }
                cursor.goto_parent();
            }
        }
    }

    /// checks if a node has a specific child node
    fn _has(cursor: &mut TreeCursor, name: &Regex) -> bool {
        if cursor.goto_first_child() {
            if name.is_match(cursor.node().kind()) || CassandraAST::_has(cursor, name) {
                cursor.goto_parent();
                return true;
            }
            while cursor.goto_next_sibling() {
                if name.is_match(cursor.node().kind()) || CassandraAST::_has(cursor, name) {
                    cursor.goto_parent();
                    return true;
                }
            }
        }
        cursor.goto_parent();
        false
    }

    /// Determines if any node matches the end of the path.
    ///
    /// * `path` the path to search for.  see `SearchPattern` for explanation of path structure.
    ///
    /// returns `true` if there is at least one matching node, `false` otherwise
    pub fn has(&self, path: &'static str) -> bool {
        return !self.search(path).is_empty();
    }

    /// Determines if the specified node has a match for the end of the path.
    ///
    /// * `node` The node to start searching from.
    /// * `path` the path to search for.  see `SearchPattern` for explanation of path structure.
    ///
    /// returns `true` if there is at least one matching node, `false` otherwise
    pub fn has_node(node: &Node, path: &'static str) -> bool {
        return !CassandraAST::search_node(node, path).is_empty();
    }

    /// extracts the nodes that match the selector
    ///
    /// * node the node to start searching from
    /// * selector a `fn(Node)` that returns true for matching elements.
    ///
    /// returns `true` if there is at least one matching node, `false` otherwise
    pub fn extract_node(node: Node, selector: fn(Node) -> bool) -> Vec<Node> {
        let mut result: Vec<Node> = vec![];
        CassandraAST::_extract_cursor(&mut node.walk(), selector, &mut result);
        result
    }

    fn _extract_cursor<'a>(
        cursor: &mut TreeCursor<'a>,
        selector: fn(Node) -> bool,
        result: &mut Vec<Node<'a>>,
    ) {
        if selector(cursor.node()) {
            result.push(cursor.node());
        }
        if cursor.goto_first_child() {
            CassandraAST::_extract_cursor(cursor, selector, result);
            while cursor.goto_next_sibling() {
                CassandraAST::_extract_cursor(cursor, selector, result);
            }
            cursor.goto_parent();
        }
    }

    /// extracts the nodes that match the selector
    ///
    /// * selector a `fn(Node)` that returns true for matching elements.
    ///
    /// returns `true` if there is at least one matching node, `false` otherwise
    pub fn extract(&self, f: fn(Node) -> bool) -> Vec<Node> {
        CassandraAST::extract_node(self.tree.root_node(), f)
    }
    /*
        pub fn apply<F>(&'tree self, selector : fn(Node)->bool, mut action : F ) where
            F : FnMut(Node)  + Copy,
        {
            CassandraAST::apply_node(self.tree.root_node(), selector, action );
        }

        pub fn apply_node<F>(node : Node, selector : fn(Node) ->bool, mut action : F ) where
            F : FnMut(Node)  + Copy,
        {
            CassandraAST::apply_cursor(&mut node.walk(), selector,  action);
        }

        pub fn apply_cursor<F>(cursor : &mut TreeCursor, selector : fn(Node) ->bool, mut action : F ) where
            F : FnMut(Node),
        {
            if selector( cursor.node() ) {
                action(cursor.node());
            }
            if cursor.goto_first_child() {
                CassandraAST::apply_cursor(cursor, selector, action );
                while cursor.goto_next_sibling() {
                    CassandraAST::apply_cursor(cursor, selector, action );
                }
                cursor.goto_parent();
            }
        }
    */

    ///  determines if any node in the tree matches the selector.
    ///
    /// * selector a `fn(Node)` that returns true for matching elements.
    ///
    /// Returns `true` if there is a match, `false` otherwise.
    pub fn contains<F: Fn(&Node)->bool>(&self, selector: F) -> bool
    {
        CassandraAST::contains_node(&self.tree.root_node(), selector)
    }

    ///  determines if any node from the provided node down matches the selector.
    ///
    /// * node the node of the current position.
    /// * selector a `fn(Node)` that returns true for matching elements.
    ///
    /// Returns `true` if there is a match, `false` otherwise.
    pub fn contains_node<F: Fn(&Node)->bool>(node: &Node, selector: F ) -> bool
    {
        CassandraAST::contains_cursor(node.walk(), selector)
    }

    ///  determines if any node from the cursor position down matches the selector.
    ///
    /// * cursor A cursor of the current position.
    /// * selector a `fn(Node)` that returns true for matching elements.
    ///
    /// Returns `true` if there is a match, `false` otherwise.
    pub fn contains_cursor<F: Fn(&Node)->bool>( cursor: TreeCursor,  selector: F) -> bool
    {
        let mut walker = CursorWalker::new( cursor );
        let mut next = walker.next();
        while next.is_some() {
            let n = next.unwrap();
            let k = n.kind();
            let i = n.id();
            if selector(&n) {
                return true;
            }
            next = walker.next();
        }
        false
    }
*/
    /// Determines the statement type from the tree
    ///
    /// * `tree` the tree to extract the statement type from.
    ///
    /// returns a `CassandraASTStatementType` for the statement.
    pub fn extract_statement(tree: &Tree, source : &String ) -> CassandraStatement {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child(0).unwrap();
        }
        CassandraStatement::from_node(&node, source)
    }


}

/*
struct CursorWalker<'a> {
    cursor : TreeCursor<'a>,
    id : usize,
    next : Option<Node<'a>>,
    next_kind : String,
    last : Option<Node<'a>>,
}

impl <'a> CursorWalker<'a> {
    fn new(cursor : TreeCursor<'a>) -> CursorWalker<'a> {
        CursorWalker {
            id : cursor.node().id(),
            next_kind : cursor.node().kind().to_string(),
            next : Some(cursor.node()),
            last : None,
            cursor,
        }
    }

    fn peek(&mut self) -> Option<Node<'a>> {
        self.next
    }

    fn current(&self) -> Option<Node<'a>> {
        self.last
    }

    fn next(&mut self) -> Option<Node<'a>> {
        self.last = self.next;
        self.next = None;
        if self.last.is_some() {
            if self.cursor.goto_first_child() {
                self.next = Some(self.cursor.node());
            } else if self.cursor.goto_next_sibling() {
                self.next = Some(self.cursor.node());
            }
            let mut scanning = self.cursor.node().id() != self.id;
            while scanning && self.next.is_none() {
                self.cursor.goto_parent();
                if self.cursor.node().id() == self.id {
                    scanning = false;
                    self.next = None;
                }
                if scanning && self.cursor.goto_next_sibling() {
                    self.next = Some( self.cursor.node());
                    scanning = false;
                }
            }

        }
        match self.next {
            Some(x) => self.next_kind = x.kind().to_string(),
            None => self.next_kind = String::new(),
        }
        self.last
    }
}
*/

/// The SearchPattern object used for string pattern matching
pub struct SearchPattern {
    /// the plain text version of the name to search for.
    pub name_str: String,
    /// the regex version of the name to search for.
    pub name: Regex,
    /// the plain text version of  the child name to search for
    pub child_str: Option<String>,
    /// the regex version of the child name to search for.
    pub child: Option<Regex>,
}

impl SearchPattern {
    /// Creates a SearchPattern from a string.
    ///
    /// The string is a series of names separated by slashes
    /// (e.g. ` foo / bar` )  This will match all `bar`s somewhere under
    /// `foo`.
    /// The string is a regular expression so `foo|bar` will match either 'foo' or 'bar'.
    ///
    /// There is a child pattern (also a regular expression) that will verify if a node has
    /// the child but still retur nthe node.  (e.g. `foo[bar]` will return all `foo` nodes
    /// that have a `bar` somewhere below them.
    pub fn from_str(pattern: &str) -> SearchPattern {
        let parts: Vec<&str> = pattern.split("[").collect();
        let name_pattern = format!("^{}$", parts[0].trim());
        let child_pattern = if parts.len() == 2 {
            let name: Vec<&str> = parts[1].split("]").collect();
            Some(format!("^{}$", name[0].trim()))
        } else {
            None
        };
        SearchPattern {
            name_str: name_pattern.clone(),
            name: Regex::new(name_pattern.as_str()).unwrap(),
            child: match &child_pattern {
                Some(pattern) => Some(Regex::new(pattern.as_str()).unwrap()),
                None => None,
            },
            child_str: child_pattern,
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::test_utils::table;
    use tree_sitter::Node;
    use crate::transforms::cassandra::cassandra_ast::{CassandraAST, CassandraStatement, RelationOperator, RelationValue, SelectElement};

    /*
    #[test]
    fn test_get_table_name() {
        let ast = CassandraAST::new("SELECT foo from bar.baz where fu='something'".to_string());
        let keyspace: Option<String> = None;
        assert_eq!("bar.baz", ast.get_table_name(&keyspace));
        let ast = CassandraAST::new("Use keyspace'".to_string());
        assert_eq!("", ast.get_table_name(&keyspace));
    }
*/
    #[test]
    fn test_select() {
        let stmt =  "SELECT column FROM table WHERE col = $$ a code's block $$;";
        let ast = CassandraAST::new(stmt.to_string());
        let foo = ast.statement;
        let foo_str = foo.to_string();
        assert_eq!( "SELECT column FROM table WHERE col = $$ a code's block $$", foo_str );
        print!( "{:?}", foo );
        match foo {
            CassandraStatement::SelectStatement(statement_data) =>
                {
                    assert!( ! statement_data.modifiers.json );
                    assert!( ! statement_data.modifiers.filtering );
                    assert_eq!( None, statement_data.modifiers.limit);
                    assert!( ! statement_data.modifiers.distinct);

                    assert_eq!( "table", statement_data.table_name.as_str() );

                    let mut element = statement_data.elements.get(0);
                    match element {
                        Some(SelectElement::COLUMN(named)) => {
                            assert_eq!( "column", named.name);
                            assert_eq!( None, named.alias );
                        },
                        _ => assert!( false ),
                    };

                    let mut relation = statement_data.where_clause.get(0);
                    match &relation {
                        Some(relation_element) => {
                            match &relation_element.obj {
                                RelationValue::COL(name) => {
                                    assert_eq!( "col", name );
                                },
                                _ => assert!(false),
                            };
                            match &relation_element.oper {
                                RelationOperator::EQ => assert!(true),
                                _ => assert!(false),
                            };
                            match &relation_element.value {
                                RelationValue::CONST(value) => {
                                    assert_eq!("$$ a code's block $$", value );
                                },
                                _ => assert!(false),
                            };
                        },
                        _ => assert!(false),
                    }
                    assert_ne!( None, element );

                },
            _ => assert!( false ),
        }
    }

        #[test]
    fn test_get_statement_type() {
        let stmts = [
            "ALTER KEYSPACE keyspace WITH REPLICATION = { 'foo' : 'bar', 'baz' : 5};",
            "ALTER MATERIALIZED VIEW 'keyspace'.mview;",
            "ALTER ROLE 'role' WITH PASSWORD = 'password';",
            "ALTER TABLE keyspace.table DROP column1, column2;",
            "ALTER TYPE type ALTER column TYPE UUID;",
            "ALTER USER username WITH PASSWORD 'password' superuser;",
            "APPLY BATCH;",
            "CREATE AGGREGATE keyspace.aggregate  ( ASCII ) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND (( 5, 'text', 6.3),(4,'foo',3.14));",
            "CREATE FUNCTION IF NOT EXISTS func ( param1 int , param2 text) CALLED ON NULL INPUT RETURNS INT LANGUAGE javascript AS $$ return 5; $$;",
            "CREATE INDEX index_name ON keyspace.table (column);",
            "CREATE KEYSPACE keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1  };",
            "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH option1 = 'option' AND option2 = 3.5 AND CLUSTERING ORDER BY (col2 DESC);",
            "CREATE ROLE role WITH OPTIONS = { 'option1' : 'value', 'option2' : 4.5 };",
            "CREATE TABLE table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) ) WITH option = 'option' AND option2 = 3.5;",
            "CREATE TRIGGER if not exists keyspace.trigger_name USING 'trigger_class';",
            "CREATE TYPE type ( col1 'foo');",
            "CREATE USER newuser WITH PASSWORD 'password';",
            //"BEGIN UNLOGGED BATCH DELETE column [ 6 ] from keyspace.table USING TIMESTAMP 5 WHERE column2='foo' IF column3 = 'stuff'",
            "DROP AGGREGATE keyspace.aggregate;",
            "DROP FUNCTION keyspace.func;",
            "DROP INDEX IF EXISTS idx;",
            "DROP KEYSPACE if exists keyspace;",
            "DROP MATERIALIZED VIEW cycling.cyclist_by_age;",
            "DROP ROLE IF EXISTS role;",
            "DROP TABLE IF EXISTS keyspace.table",
            "DROP TRIGGER trigger_name ON ks.table_name;",
            "DROP TYPE IF EXISTS keyspace.type ;",
            "DROP USER if exists user_name;",
            "GRANT ALL ON 'keyspace'.table TO role;",
            //"INSERT INTO table (col1, col2) VALUES (( 5, 6 ), 'foo');",
            "LIST ALL;",
            "LIST ROLES;",
            "REVOKE ALL ON ALL ROLES FROM role;",
            //"SELECT column FROM table WHERE col = $$ a code's block $$;",
            //"TRUNCATE keyspace.table;",
            "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2=5;",
            //"USE key_name;",
            "Not a valid statement"];
        let types = [
            CassandraStatement::AlterKeyspace,
            CassandraStatement::AlterMaterializedView,
            CassandraStatement::AlterRole,
            CassandraStatement::AlterTable,
            CassandraStatement::AlterType,
            CassandraStatement::AlterUser,
            CassandraStatement::ApplyBatch,
            CassandraStatement::CreateAggregate,
            CassandraStatement::CreateFunction,
            CassandraStatement::CreateIndex,
            CassandraStatement::CreateKeyspace,
            CassandraStatement::CreateMaterializedView,
            CassandraStatement::CreateRole,
            CassandraStatement::CreateTable,
            CassandraStatement::CreateTrigger,
            CassandraStatement::CreateType,
            CassandraStatement::CreateUser,
            //CassandraStatement::DeleteStatement,
            CassandraStatement::DropAggregate,
            CassandraStatement::DropFunction,
            CassandraStatement::DropIndex,
            CassandraStatement::DropKeyspace,
            CassandraStatement::DropMaterializedView,
            CassandraStatement::DropRole,
            CassandraStatement::DropTable,
            CassandraStatement::DropTrigger,
            CassandraStatement::DropType,
            CassandraStatement::DropUser,
            CassandraStatement::Grant,
            //CassandraStatement::InsertStatement,
            CassandraStatement::ListPermissions,
            CassandraStatement::ListRoles,
            CassandraStatement::Revoke,
            //CassandraStatement::SelectStatement(data),
            //CassandraStatement::Truncate,
            CassandraStatement::Update,
            //CassandraStatement::UseStatement(keyspace),
            CassandraStatement::UNKNOWN("Not a valid statement".to_string()),
        ];

        for i in 0..stmts.len() {
            let ast = CassandraAST::new(stmts.get(i).unwrap().to_string());
            assert_eq!(*types.get(i).unwrap(), ast.statement);
        }
    }

    #[test]
    fn test_has_error() {
        let ast = CassandraAST::new("SELECT foo from bar.baz where fu='something'".to_string());
        assert!(!ast.has_error());
        let ast = CassandraAST::new("Not a valid statement".to_string());
        assert!(ast.has_error());
    }

    /*
    #[test]
    fn test_search() {
        let ast = CassandraAST::new("SELECT column AS column2, func(*) AS func2, col3 FROM table where col3 = 6".to_string());
        // the above will produce the following tree
        // (source_file (select_statement (select_elements (select_element) (select_element (function_call)) (select_element)) (from_spec (table_name)) (where_spec (relation_elements (relation_element (constant))))))

        let expected = ["column AS column2",
            "func(*) AS func2",
            "col3"];
        let result = ast.search(" select_element ");

        for i in 0..result.len() {
            assert_eq!(expected.get(i).unwrap().to_string(), ast.node_text(result.get(i).unwrap()));
        }

        let expected = ["column", "col3"];
        let result = ast.search(" select_element / column ");

        for i in 0..result.len() {
            assert_eq!(expected.get(i).unwrap().to_string(), ast.node_text(result.get(i).unwrap()));
        }

        let expected = ["column2"];
        let result = ast.search(" select_element[column] / alias ");

        for i in 0..result.len() {
            assert_eq!(expected.get(i).unwrap().to_string(), ast.node_text(result.get(i).unwrap()));
        }

        let expected = ["column AS column2", "func(*) AS func2"];
        let result = ast.search(" select_element[alias] ");

        for i in 0..result.len() {
            assert_eq!(expected.get(i).unwrap().to_string(), ast.node_text(result.get(i).unwrap()));
        }
    }

    #[test]
    fn test_has() {
        let ast = CassandraAST::new("SELECT column AS column2, func(*) AS func2, col3 FROM table where col3 = 6".to_string());
        // the above will produce the following tree
        // (source_file (select_statement (select_elements (select_element) (select_element (function_call)) (select_element)) (from_spec (table_name)) (where_spec (relation_elements (relation_element (constant))))))
        assert!(ast.has("select_element"));
        assert!(!ast.has("expression_list"));
    }

    #[test]
    fn test_contains() {
        let ast = CassandraAST::new("SELECT column AS column2, func(*) AS func2, col3 FROM table where col3 = 6".to_string());
        // the above will produce the following tree
        // (source_file (select_statement (select_elements (select_element) (select_element (function_call)) (select_element)) (from_spec (table_name)) (where_spec (relation_elements (relation_element (constant))))))
        let selector = |n:&Node| n.is_named();
        assert!(ast.contains( selector ));
        assert!(!ast.contains( |_| {return false;} ));
        let selector = |n:&Node| n.kind().eq( "AS");
        assert!(ast.contains( selector ));
    }

    #[test]
    fn test_extract() {
        let ast = CassandraAST::new("SELECT column AS column2, func(*) AS func2, col3 FROM table where col3 = 6".to_string());
        // the above will produce the following tree
        // (source_file (select_statement (select_elements (select_element) (select_element (function_call)) (select_element)) (from_spec (table_name)) (where_spec (relation_elements (relation_element (constant))))))
        let result =ast.extract( |n:Node| n.kind().eq( "AS") );
        assert_eq!( 2, result.len() );
        let result = ast.extract( |_|{ return false; } );
        assert!( result.is_empty() );
    }

    #[test]
    fn test_node_text() {
        let ast = CassandraAST::new("SELECT column AS column2, func(*) AS func2, col3 FROM table where col3 = 6".to_string());
        // the above will produce the following tree
        // (source_file (select_statement (select_elements (select_element) (select_element (function_call)) (select_element)) (from_spec (table_name)) (where_spec (relation_elements (relation_element (constant))))))

        let result =ast.search( "constant" );
        assert_eq!( 1, result.len() );
        assert_eq!( "6", ast.node_text( result.get(0).unwrap() ));
    }
*/
    #[test]
    fn test_truncate() {
        let ast = CassandraAST::new("TRUNCATE TABLE foo".to_string());
        match ast.statement {
            CassandraStatement::Truncate(table) => assert_eq!("foo", table),
            _ => assert!(false),
        }
        let ast = CassandraAST::new("TRUNCATE foo".to_string());
        match ast.statement {
            CassandraStatement::Truncate(table) => assert_eq!("foo", table),
            _ => assert!(false),
        }

        let ast = CassandraAST::new("TRUNCATE TABLE foo.bar".to_string());
        match ast.statement {
            CassandraStatement::Truncate(table) => assert_eq!("foo.bar", table),
            _ => assert!(false),
        }
        let ast = CassandraAST::new("TRUNCATE foo.bar".to_string());
        match ast.statement {
            CassandraStatement::Truncate(table) => assert_eq!("foo.bar", table),
            _ => assert!(false),
        }
    }
}