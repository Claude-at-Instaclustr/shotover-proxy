use std::borrow::Borrow;
use regex::Regex;
use tree_sitter::{
    Language, LogType, Node, Parser, Query, QueryCapture, QueryCursor, QueryMatch, Tree, TreeCursor,
};

#[derive(PartialEq, Debug, Clone)]
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
    SelectStatement {
        modifiers : SelectModifiers,
        table_name: String,
        elements : Vec<Element>,
        where_clause : Vec<RelationElement>,
        order : Option<OrderClause>,
    },
    Truncate,
    Update,
    UseStatement,
    UNKNOWN(String),
}

enum Element {
    STAR,
    DOT_STAR(String),
    COLUMN{
        name : String,
        alias : Option<String>,
    },
    FUNCTION {
        name : String,
        args : String,
        alias : Option<String>,
    },
}

struct OrderClause {
    name : String,
    desc : bool,
}

struct RelationElement {
    obj : RelationValue,
    oper : Operator,
    value : RelationValue,
}

enum RelationValue {
    CONST(String),
    FUNC(String),
    COL(String),
    LIST{
        values : Vec<RelationValue>,
    },

}

enum Operator {
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

struct SelectModifiers {
    distinct : bool,
    json : bool,
    limit : Option<i32>,
    filtering : bool,
}

impl CassandraASTStatementType {
    pub fn from_node(node: &Node, source : &String) -> CassandraASTStatementType {
        let kind = node.kind();
        match kind {
            "alter_keyspace" => CassandraASTStatementType::AlterKeyspace,
            "alter_materialized_view" => CassandraASTStatementType::AlterMaterializedView,
            "alter_role" => CassandraASTStatementType::AlterRole,
            "alter_table" => CassandraASTStatementType::AlterTable,
            "alter_type" => CassandraASTStatementType::AlterType,
            "alter_user" => CassandraASTStatementType::AlterUser,
            "apply_batch" => CassandraASTStatementType::ApplyBatch,
            "create_aggregate" => CassandraASTStatementType::CreateAggregate,
            "create_function" => CassandraASTStatementType::CreateFunction,
            "create_index" => CassandraASTStatementType::CreateIndex,
            "create_keyspace" => CassandraASTStatementType::CreateKeyspace,
            "create_materialized_view" => CassandraASTStatementType::CreateMaterializedView,
            "create_role" => CassandraASTStatementType::CreateRole,
            "create_table" => CassandraASTStatementType::CreateTable,
            "create_trigger" => CassandraASTStatementType::CreateTrigger,
            "create_type" => CassandraASTStatementType::CreateType,
            "create_user" => CassandraASTStatementType::CreateUser,
            "delete_statement" => CassandraASTStatementType::DeleteStatement,
            "drop_aggregate" => CassandraASTStatementType::DropAggregate,
            "drop_function" => CassandraASTStatementType::DropFunction,
            "drop_index" => CassandraASTStatementType::DropIndex,
            "drop_keyspace" => CassandraASTStatementType::DropKeyspace,
            "drop_materialized_view" => CassandraASTStatementType::DropMaterializedView,
            "drop_role" => CassandraASTStatementType::DropRole,
            "drop_table" => CassandraASTStatementType::DropTable,
            "drop_trigger" => CassandraASTStatementType::DropTrigger,
            "drop_type" => CassandraASTStatementType::DropType,
            "drop_user" => CassandraASTStatementType::DropUser,
            "grant" => CassandraASTStatementType::Grant,
            "insert_statement" => CassandraASTStatementType::InsertStatement,
            "list_permissions" => CassandraASTStatementType::ListPermissions,
            "list_roles" => CassandraASTStatementType::ListRoles,
            "revoke" => CassandraASTStatementType::Revoke,
            "select_statement" => build_select_statement(node, source),
            "truncate" => CassandraASTStatementType::Truncate,
            "update" => CassandraASTStatementType::Update,
            "use" => CassandraASTStatementType::UseStatement,
            _ => CassandraASTStatementType::UNKNOWN(node.kind().to_string()),
        }
    }

    fn build_select_statement(& cursor :TreeCursor, source : &String ) -> CassandraASTStatementType::SelectStatement {
        let mut walker = CursorWalker::new(cursor );
        let mut prelude = true;
        let mut elements = false;
        let mut from = false;
        let mut where_ = false;
        let mut suffix = false;


        let mut modifiers = SelectModifiers{
            distinct : false,
            json : false,
            limit : None,
            filtering : false,
        };
        let mut element_list:Vec<Element> = vec!();
        let mut table_name = String::new();
        let mut where_clause :Vec<RelatinElement> = vec!();
        let mut order = None;

        let mut next = walker.next();
        while next.is_some() {
            let node = next.unwrap();
            if prelude {
                modifiers.distinct = modifiers.distinct || node.kind().eq("DISTINCT");
                modifiers.json = modifiers.json || node.kind().eq( "JSON");
                elements = node.kind().eq( "select_elements");
                prelude = !elements;
            }
            if elements {
                if  node.kind().eq( "select_element") {
                    element_list.push( parse_select_element( node, source ));
                }
                from = node.kind().eq( "from_spec");
                elements = !from;
            }
            if from {
                if node.kind().eq("keyspace") {
                    table_name.push_str( node.utf8_text( source.as_bytes()));
                    table_name.push('.')
                }
                if node.kind().eq("table") {
                    table_name.push_str(node.utf8_text(source.as_bytes()))
                }
                where_ = node.kind().eq("where_spec");
                from = !where_;
            }
            if where_ {
                if  node.kind().eq( "relation_element") {
                    where_clause.push( parse_relation_element( node, source ));
                }
                suffix = node.kind().eq("order_spec") || node.kind().eq("limit_spec") ||
                    node.kind().eq("ALLOW");
                where_ = !suffix;
            }
            if suffix {
                if node.kind().eq("limit_value") {
                    modifiers.limit = node.utf8_text(source.as_bytes());
                } else if node.kind().eq("ORDER") {
                    order = parse_order_by( node, source );
                }
                modifiers.filtering = modifiers.filtering || node.kind().eq( "FILTERING");
            }
            next = walker.next();
        }
        CassandraASTStatementType::SelectStatement {
            modifiers,
            table_name,
            elements : element_list,
            where_clause ,
            order,
        }
    }

    fn parse_order_by( node : &Node, source : &String ) -> OrderClause {
        OrderClause {
            name: node.child(2).unwrap().utf8_text(source.as_bytes()),
            desc: node.child_count()> 3 &&
                node.child(3).unwrap().utf8_text(source.as_bytes()).eq("DESC"),
        }
    }

    fn parse_select_element( node : &Node, source : &String )  -> Element {

        let mut alias :Option<String> = None;
        let type_ = node.child(0).unwrap();
        let is_column = type_.kind().eq("column");
        if node.child_count() > 2 {
            if type_.kind().eq("column") || type_.kind().eq("function_call") {
                alias = Some( node.child(2).unwrap().utf8_text( source.as_bytes() ).unwrap().to_string() );
            }
        }
        if type_.kind().eq("column") {
            Element::COLUMN {
                name: type_.utf8_text(source.as_bytes()).unwrap().to_string(),
                alias: alias,
            }
        } else if type_.kind().eq("function_call") {
            Element::FUNCTION {
                name: type_.utf8_text(source.as_bytes()).unwrap().to_string(),
                alias: alias,
            }
        } else if type_kind().eq("*") {
            Element::STAR
        } else {
            Element::DOT_STAR( type_.utf8_text(source.as_bytes()).unwrap().to_string() )
        }
    }
}

pub struct CassandraAST {
    /// The query string
    text: String,
    /// the tree-sitter tree
    pub(crate) tree: Tree,
    /// the statement type of the query
    pub statement_type: CassandraASTStatementType,
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
            statement_type: if tree.root_node().has_error() {
                CassandraASTStatementType::UNKNOWN(cassandra_statement.clone())
            } else {
                CassandraAST::extract_statement_type(&tree, &cassandra_statement)
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

    /// Determines the statement type from the tree
    ///
    /// * `tree` the tree to extract the statement type from.
    ///
    /// returns a `CassandraASTStatementType` for the statement.
    pub fn extract_statement_type(tree: &Tree, source : &String ) -> CassandraASTStatementType {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child(0).unwrap();
        }
        CassandraASTStatementType::from_node(&node, source)
    }
}

struct CursorWalker<'a> {
    cursor : TreeCursor<'a>,
    id : usize,
    next : Option<Node<'a>>,
}

impl <'a> CursorWalker<'a> {
    fn new(cursor : TreeCursor<'a>) -> CursorWalker<'a> {
        CursorWalker {
            id : cursor.node().id(),
            next : Some(cursor.node()),
            cursor,
        }
    }

    fn next(&mut self) -> Option<Node<'a>> {
        let result = self.next;
        self.next = None;
        if result.is_some() {
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
        result
    }
}


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
    use tree_sitter::Node;
    use crate::transforms::cassandra::cassandra_ast::{CassandraAST, CassandraASTStatementType};

    #[test]
    fn test_get_table_name() {
        let ast = CassandraAST::new("SELECT foo from bar.baz where fu='something'".to_string());
        let keyspace: Option<String> = None;
        assert_eq!("bar.baz", ast.get_table_name(&keyspace));
        let ast = CassandraAST::new("Use keyspace'".to_string());
        assert_eq!("", ast.get_table_name(&keyspace));
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
            "BEGIN UNLOGGED BATCH DELETE column [ 6 ] from keyspace.table USING TIMESTAMP 5 WHERE column2='foo' IF column3 = 'stuff'",
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
            "INSERT INTO table (col1, col2) VALUES (( 5, 6 ), 'foo');",
            "LIST ALL;",
            "LIST ROLES;",
            "REVOKE ALL ON ALL ROLES FROM role;",
            "SELECT column FROM table WHERE col = $$ a code's block $$;",
            "TRUNCATE keyspace.table;",
            "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2=5;",
            "USE key_name;",
            "Not a valid statement"];
        let types = [
            CassandraASTStatementType::AlterKeyspace,
            CassandraASTStatementType::AlterMaterializedView,
            CassandraASTStatementType::AlterRole,
            CassandraASTStatementType::AlterTable,
            CassandraASTStatementType::AlterType,
            CassandraASTStatementType::AlterUser,
            CassandraASTStatementType::ApplyBatch,
            CassandraASTStatementType::CreateAggregate,
            CassandraASTStatementType::CreateFunction,
            CassandraASTStatementType::CreateIndex,
            CassandraASTStatementType::CreateKeyspace,
            CassandraASTStatementType::CreateMaterializedView,
            CassandraASTStatementType::CreateRole,
            CassandraASTStatementType::CreateTable,
            CassandraASTStatementType::CreateTrigger,
            CassandraASTStatementType::CreateType,
            CassandraASTStatementType::CreateUser,
            CassandraASTStatementType::DeleteStatement,
            CassandraASTStatementType::DropAggregate,
            CassandraASTStatementType::DropFunction,
            CassandraASTStatementType::DropIndex,
            CassandraASTStatementType::DropKeyspace,
            CassandraASTStatementType::DropMaterializedView,
            CassandraASTStatementType::DropRole,
            CassandraASTStatementType::DropTable,
            CassandraASTStatementType::DropTrigger,
            CassandraASTStatementType::DropType,
            CassandraASTStatementType::DropUser,
            CassandraASTStatementType::Grant,
            CassandraASTStatementType::InsertStatement,
            CassandraASTStatementType::ListPermissions,
            CassandraASTStatementType::ListRoles,
            CassandraASTStatementType::Revoke,
            CassandraASTStatementType::SelectStatement,
            CassandraASTStatementType::Truncate,
            CassandraASTStatementType::Update,
            CassandraASTStatementType::UseStatement,
            CassandraASTStatementType::UNKNOWN("Not a valid statement".to_string()),
        ];

        for i in 0..stmts.len() {
            let ast = CassandraAST::new(stmts.get(i).unwrap().to_string());
            assert_eq!(*types.get(i).unwrap(), ast.statement_type);
        }
    }

    #[test]
    fn test_has_error() {
        let ast = CassandraAST::new("SELECT foo from bar.baz where fu='something'".to_string());
        assert!(!ast.has_error());
        let ast = CassandraAST::new("Not a valid statement".to_string());
        assert!(ast.has_error());
    }

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
}