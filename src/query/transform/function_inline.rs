//! Function body inlining (PGC-579). A `LANGUAGE sql` function that is
//! STABLE or IMMUTABLE and otherwise within PostgreSQL's own
//! `inline_function` rules is replaced in the tree by its body, the
//! arguments substituted for its parameters under casts to the declared
//! types and the result cast to the declared return type. A FROM-less body
//! becomes the expression itself, any other body a scalar subquery limited
//! to one row, which is what a call returns. Inlining happens before the
//! verdict, so the session terms a body reads are fragments the proxy folds
//! (ADR-066) and the ordinary rules judge what the body computes; the
//! inlined functions travel with the query by oid, for the writer's path
//! check and for eviction when one is redefined.

#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use ecow::EcoString;
use error_set::error_set;

use super::term_fold::bare_select_item;
use crate::catalog::{FunctionFacts, FunctionKind, FunctionRow, FunctionVolatility};
use crate::oid::FunctionOid;
use crate::query::ast::rope::{
    Body, Node, NodeKind, NodeRef, QueryTree, Replacement, Replacements, ScalarKind, SelectItem,
    TreeBuilder, View,
};
use crate::query::ast::{LiteralValue, NodeId, SubLinkType, query_tree_parse};
use crate::query::cast::CastTarget;

/// Passes over the tree, each inlining the calls the previous one exposed,
/// before a body still holding an inlinable call counts as recursion.
const INLINE_DEPTH: usize = 8;

error_set! {
    FunctionInlineError := {
        /// A body still holds an inlinable call after the last pass: the
        /// function calls itself, directly or through another.
        #[display("function {function} inlines deeper than {depth} levels")]
        TooDeep {
            function: EcoString,
            depth: usize,
        },
    }

    /// Why a function's body is not inlined: PostgreSQL's `inline_function`
    /// rules, and the proxy's fail-closed ones where it lacks what the
    /// planner has.
    #[derive(Clone, PartialEq, Eq)]
    FunctionBodyError := {
        #[display("the function has no sql body")]
        NoSqlBody,
        #[display("not a plain function")]
        NotPlainFunction,
        #[display("volatile")]
        Volatile,
        #[display("security definer")]
        SecurityDefiner,
        #[display("sets configuration parameters")]
        SetsConfiguration,
        #[display("returns a set")]
        ReturnsSet,
        #[display("has variadic or defaulted parameters")]
        VariadicOrDefaulted,
        #[display("strict with arguments")]
        StrictWithArguments,
        #[display("parameter list does not match the arity")]
        ArityMismatch,
        #[display("polymorphic type {type_name}")]
        PolymorphicType {
            type_name: EcoString,
        },
        #[display("unparseable type name {type_name}")]
        UnparseableType {
            type_name: EcoString,
        },
        #[display("the body is not a single SELECT")]
        NotSingleSelect,
        #[display("the body is a set operation or VALUES")]
        SetOperation,
        #[display("the body returns a row")]
        ReturnsRow,
        #[display("the body returns other than one column")]
        NotOneColumn,
        #[display("the body limits to other than its first row")]
        LimitNotOne,
        #[display("refers to a parameter it does not declare")]
        UndeclaredParameter,
        #[display("refers to a column without a FROM")]
        ColumnWithoutFrom,
        #[display("names a parameter in a body with a FROM")]
        NamedParameterWithFrom,
    }
}

/// The inlining inputs `pg_proc` holds for a function, kept on its row when
/// the body text is available at all (a sql function with a `prosrc` body
/// and a base, domain, enum or range return type).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionSource {
    pub text: EcoString,
    pub strict: bool,
    /// `proconfig` is set: the function runs under its own settings.
    pub has_config: bool,
    /// `proargnames`; empty when the parameters are unnamed.
    pub arg_names: Vec<EcoString>,
    /// The declared parameter types, as `format_type` spells them.
    pub arg_types: Vec<EcoString>,
    /// The declared return type, as `format_type` spells it.
    pub return_type: EcoString,
}

/// A function's body in the form the inliner emits.
#[derive(Debug, Clone)]
pub struct FunctionBody {
    pub oid: FunctionOid,
    /// A hash of the `pg_proc` row the body came from, carried by each
    /// inlined call so the writer can tell an inlining from a superseded
    /// row apart from the function's current one.
    pub definition: u64,
    tree: QueryTree,
    shape: BodyShape,
    /// The body has a FROM, or a subquery with one: a scope whose names
    /// would capture a column reference substituted into it.
    scoped: bool,
    /// The positions of `tree` that refer to a parameter, with its index.
    parameters: Vec<(NodeId, usize)>,
    /// The parameters referred to more than once: their arguments are
    /// copied per use.
    repeated_parameters: Vec<usize>,
    parameter_casts: Vec<CastTarget>,
    return_cast: CastTarget,
}

/// The cast targets of the type names one catalog load meets, each parsed
/// once however many functions declare it.
#[derive(Debug, Default)]
pub struct TypeCasts(HashMap<EcoString, Result<CastTarget, FunctionBodyError>>);

impl TypeCasts {
    fn target(&mut self, type_name: &EcoString) -> Result<CastTarget, FunctionBodyError> {
        self.0
            .entry(type_name.clone())
            .or_insert_with(|| cast_target_of(type_name))
            .clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyShape {
    /// `SELECT <expr>` and nothing else: the expression at the position.
    Expression(NodeId),
    /// Any other single-column SELECT, as a scalar subquery; a call returns
    /// the first row, so `LIMIT 1` is added when the body has no limit.
    Subquery { add_limit: bool },
}

/// A call the inliner replaced: what the writer resolves on the session's
/// search_path to check it names the inlined function, the oid the entry
/// depends on, and the definition the body was taken from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InlinedCall {
    pub schema: Option<EcoString>,
    pub name: EcoString,
    pub arg_count: u32,
    pub oid: FunctionOid,
    pub definition: u64,
}

/// A tree with its inlinable calls replaced, and the calls.
#[derive(Debug, Clone)]
pub struct Inlined {
    pub tree: QueryTree,
    pub calls: Vec<InlinedCall>,
}

/// The inlinable form of `row`'s body, or why it has none. The rules are
/// PostgreSQL's for `inline_function`, fail-closed where the proxy lacks
/// what PostgreSQL has at hand: a STRICT function's NULL test, a named
/// parameter told from a column, a polymorphic type.
pub fn function_body_parse(
    row: &FunctionRow,
    casts: &mut TypeCasts,
) -> Result<FunctionBody, FunctionBodyError> {
    let Some(source) = &row.source else {
        return Err(FunctionBodyError::NoSqlBody);
    };
    if row.kind != FunctionKind::Function {
        return Err(FunctionBodyError::NotPlainFunction);
    }
    if row.volatility == FunctionVolatility::Volatile {
        return Err(FunctionBodyError::Volatile);
    }
    if row.security_definer {
        return Err(FunctionBodyError::SecurityDefiner);
    }
    if source.has_config {
        return Err(FunctionBodyError::SetsConfiguration);
    }
    if row.returns_set {
        return Err(FunctionBodyError::ReturnsSet);
    }
    if row.variadic || row.default_count > 0 {
        return Err(FunctionBodyError::VariadicOrDefaulted);
    }
    let arg_count = usize::from(row.arg_count);
    if source.strict && arg_count > 0 {
        return Err(FunctionBodyError::StrictWithArguments);
    }
    if source.arg_types.len() != arg_count
        || (!source.arg_names.is_empty() && source.arg_names.len() != arg_count)
    {
        return Err(FunctionBodyError::ArityMismatch);
    }
    let parameter_casts = source
        .arg_types
        .iter()
        .map(|name| casts.target(name))
        .collect::<Result<Vec<CastTarget>, _>>()?;
    let return_cast = casts.target(&source.return_type)?;
    let tree = query_tree_parse(body_statement(&source.text))
        .map_err(|_| FunctionBodyError::NotSingleSelect)?;
    let shape = body_shape(&tree)?;
    let scoped = matches!(shape, BodyShape::Subquery { .. })
        || tree
            .nodes()
            .filter(|node| matches!(node.kind, NodeKind::Query { .. }))
            .count()
            > 1;
    let parameters = body_parameters(&tree, shape, &source.arg_names, arg_count)?;
    let repeated_parameters = (0..arg_count)
        .filter(|index| parameters.iter().filter(|(_, at)| at == index).count() > 1)
        .collect();
    let mut hasher = DefaultHasher::new();
    row.hash(&mut hasher);
    Ok(FunctionBody {
        oid: row.oid,
        definition: hasher.finish(),
        tree,
        shape,
        scoped,
        parameters,
        repeated_parameters,
        parameter_casts,
        return_cast,
    })
}

/// The body text as one statement: surrounding whitespace and a trailing
/// semicolon dropped, as `CREATE FUNCTION ... AS $$ select ...; $$` leaves
/// them.
fn body_statement(text: &str) -> &str {
    text.trim().trim_end_matches(';').trim_end()
}

/// The cast target of a type as `format_type` spells it, through the
/// parser so the spelling is the one the converter gives a cast in a
/// query (`integer` and `int4` alike). A polymorphic type has no cast.
fn cast_target_of(type_name: &EcoString) -> Result<CastTarget, FunctionBodyError> {
    if type_name
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("any"))
    {
        return Err(FunctionBodyError::PolymorphicType {
            type_name: type_name.clone(),
        });
    }
    let unparseable = || FunctionBodyError::UnparseableType {
        type_name: type_name.clone(),
    };
    let tree =
        query_tree_parse(&format!("SELECT c::{type_name} FROM t")).map_err(|_| unparseable())?;
    let Body::Select(select) = tree.root().body() else {
        return Err(unparseable());
    };
    let item = select
        .items()
        .and_then(|mut items| items.next())
        .ok_or_else(unparseable)?;
    match item {
        SelectItem::Expr(expr) => match expr.expr().kind() {
            ScalarKind::TypeCast(cast) => Ok(cast.target().clone()),
            _ => Err(unparseable()),
        },
        SelectItem::Star(_) => Err(unparseable()),
    }
}

fn body_shape(tree: &QueryTree) -> Result<BodyShape, FunctionBodyError> {
    let query = tree.root();
    if let Some(expr) = bare_select_item(query) {
        return Ok(BodyShape::Expression(expr.id()));
    }
    let Body::Select(select) = query.body() else {
        return Err(FunctionBodyError::SetOperation);
    };
    let mut items = select.items().ok_or(FunctionBodyError::ReturnsRow)?;
    match (items.next(), items.next()) {
        (Some(SelectItem::Expr(_)), None) => {}
        _ => return Err(FunctionBodyError::NotOneColumn),
    }
    match (query.limit_count(), query.limit_offset()) {
        (None, None) => Ok(BodyShape::Subquery { add_limit: true }),
        (Some(count), None) if *count.value() == LiteralValue::Integer(1) => {
            Ok(BodyShape::Subquery { add_limit: false })
        }
        _ => Err(FunctionBodyError::LimitNotOne),
    }
}

/// Where the body refers to its parameters: `$n` anywhere; a bare name only
/// in a FROM-less body, since with a FROM a column of the same name wins at
/// origin and the proxy cannot tell the two apart. A FROM-less body naming
/// anything else has nothing to bind it to.
fn body_parameters(
    tree: &QueryTree,
    shape: BodyShape,
    arg_names: &[EcoString],
    arg_count: usize,
) -> Result<Vec<(NodeId, usize)>, FunctionBodyError> {
    let mut parameters = Vec::new();
    for node in tree.root().node_ref().subtree() {
        match node.kind() {
            NodeKind::Literal(LiteralValue::Parameter(placeholder)) => {
                let index = placeholder
                    .strip_prefix('$')
                    .and_then(|number| number.parse::<usize>().ok())
                    .filter(|number| (1..=arg_count).contains(number))
                    .ok_or(FunctionBodyError::UndeclaredParameter)?;
                parameters.push((node.id(), index - 1));
            }
            NodeKind::Column { table, column } => {
                let named = table
                    .is_none()
                    .then(|| arg_names.iter().position(|name| name == column))
                    .flatten();
                match (shape, named) {
                    (BodyShape::Expression(_), Some(index)) => parameters.push((node.id(), index)),
                    (BodyShape::Expression(_), None) => {
                        return Err(FunctionBodyError::ColumnWithoutFrom);
                    }
                    (BodyShape::Subquery { .. }, Some(_)) => {
                        return Err(FunctionBodyError::NamedParameterWithFrom);
                    }
                    (BodyShape::Subquery { .. }, None) => {}
                }
            }
            _ => {}
        }
    }
    Ok(parameters)
}

/// `tree` with every inlinable call replaced by its body, pass after pass
/// until none remains; `None` when the tree held none. A body still
/// holding one after the last pass is recursion, and an error.
pub fn query_tree_functions_inline(
    tree: &QueryTree,
    functions: &dyn FunctionFacts,
) -> Result<Option<Inlined>, FunctionInlineError> {
    let mut inlined: Option<Inlined> = None;
    for _ in 0..INLINE_DEPTH {
        let base = inlined.as_ref().map_or(tree, |inlined| &inlined.tree);
        let sites = inline_sites(base, functions);
        if sites.is_empty() {
            return Ok(inlined);
        }
        let mut calls = inlined
            .as_ref()
            .map_or_else(Vec::new, |inlined| inlined.calls.clone());
        let mut replacements = Replacements::new();
        for site in sites {
            calls.push(site.call);
            replacements.push(site.at, Replacement::Fragment(site.fragment));
        }
        let mut builder = TreeBuilder::over(base);
        builder.copy(base.root().node_ref(), &mut replacements);
        let (tree, _) = builder.finish();
        inlined = Some(Inlined { tree, calls });
    }
    let Some(last) = &inlined else {
        return Ok(None);
    };
    match inline_sites(&last.tree, functions).into_iter().next() {
        Some(site) => Err(FunctionInlineError::TooDeep {
            function: site.call.name,
            depth: INLINE_DEPTH,
        }),
        None => Ok(inlined),
    }
}

/// One call to replace: its position, the fragment that takes its place and
/// the record of it.
struct Site {
    at: NodeId,
    fragment: Vec<Node>,
    call: InlinedCall,
}

/// The outermost inlinable calls of `tree`, in position order. A call
/// inside another's argument is copied with the argument and inlined by the
/// next pass; a call whose arguments its body cannot take stays, and the
/// calls inside those arguments are inlined in its place.
fn inline_sites(tree: &QueryTree, functions: &dyn FunctionFacts) -> Vec<Site> {
    let mut sites = Vec::new();
    sites_collect(tree.root().node_ref(), false, functions, &mut sites);
    sites
}

fn sites_collect(
    node: NodeRef<'_>,
    where_position: bool,
    functions: &dyn FunctionFacts,
    sites: &mut Vec<Site>,
) {
    if let NodeKind::Function {
        schema,
        name,
        arg_count,
        agg_star: false,
        agg_distinct: false,
        order_count: 0,
        has_filter: false,
        has_over: false,
    } = node.kind()
        && let Some(body) = functions.inline_body(schema.as_deref(), name, *arg_count)
        && arguments_inlinable(body, node)
    {
        sites.push(Site {
            at: node.id(),
            fragment: body_fragment(body, node, where_position),
            call: InlinedCall {
                schema: schema.clone(),
                name: name.clone(),
                arg_count: *arg_count,
                oid: body.oid,
                definition: body.definition,
            },
        });
        return;
    }
    // A child of a predicate connective is in a WHERE position, where the
    // converter spells `col = (select …)` as a sublink; everywhere else a
    // subquery is a scalar.
    let where_position = matches!(
        node.kind(),
        NodeKind::Binary { .. } | NodeKind::Unary { .. } | NodeKind::Multi { .. }
    );
    for child in node.children() {
        sites_collect(child, where_position, functions, sites);
    }
}

/// Whether `body` can take the arguments of `call`. A body with a FROM, at
/// its top or in a subquery, brings its tables' names into scope over the
/// arguments, so a column reference in one would bind to them instead
/// (`owner_of(id)` with a body over `docs.id` would compare `docs.id` to
/// itself): such a call keeps its arguments column-free. A parameter used
/// more than once copies its argument per use, which PostgreSQL does not do
/// for a subquery.
fn arguments_inlinable(body: &FunctionBody, call: NodeRef<'_>) -> bool {
    call.children().enumerate().all(|(index, argument)| {
        let captured = body.scoped
            && argument
                .subtree()
                .any(|node| matches!(node.kind(), NodeKind::Column { .. }));
        let duplicated_subquery = body.repeated_parameters.contains(&index)
            && argument
                .subtree()
                .any(|node| matches!(node.kind(), NodeKind::Query { .. }));
        !captured && !duplicated_subquery
    })
}

/// `body` over the arguments of `call`, as fresh nodes in pre-order.
fn body_fragment(body: &FunctionBody, call: NodeRef<'_>, where_position: bool) -> Vec<Node> {
    let arguments: Vec<NodeRef<'_>> = call.children().collect();
    let mut emitter = FragmentEmitter {
        body,
        arguments: &arguments,
        nodes: Vec::new(),
    };
    let root = body.tree.root().node_ref();
    match body.shape {
        BodyShape::Expression(at) => {
            let expr = root.at_position(at);
            let cast = (!is_cast_to(expr, &body.return_cast)).then(|| emitter.open_return_cast());
            emitter.emit(expr);
            if let Some(cast) = cast {
                emitter.close(cast);
            }
        }
        BodyShape::Subquery { add_limit } => {
            let item_is_cast =
                single_item(root).is_some_and(|item| is_cast_to(item, &body.return_cast));
            let cast = (!item_is_cast).then(|| emitter.open_return_cast());
            let subquery = if where_position && cast.is_none() {
                emitter.open(NodeKind::WhereSubquery {
                    sublink_type: SubLinkType::Expr,
                    has_test_expr: false,
                })
            } else {
                emitter.open(NodeKind::ScalarSubquery)
            };
            emitter.emit_query(root, add_limit);
            emitter.close(subquery);
            if let Some(cast) = cast {
                emitter.close(cast);
            }
        }
    }
    emitter.nodes
}

/// The one SELECT item of the body at `root`, when the body is a SELECT.
fn single_item(root: NodeRef<'_>) -> Option<NodeRef<'_>> {
    let Body::Select(select) = crate::query::ast::rope::Query::from_ref(root).body() else {
        return None;
    };
    match select.items()?.next()? {
        SelectItem::Expr(expr) => Some(expr.expr().node_ref()),
        SelectItem::Star(_) => None,
    }
}

fn is_cast_to(node: NodeRef<'_>, target: &CastTarget) -> bool {
    matches!(node.kind(), NodeKind::TypeCast { target: cast } if cast == target)
}

/// Builds a fragment: a node's length is set when it closes.
struct FragmentEmitter<'a> {
    body: &'a FunctionBody,
    arguments: &'a [NodeRef<'a>],
    nodes: Vec<Node>,
}

impl FragmentEmitter<'_> {
    fn open(&mut self, kind: NodeKind) -> usize {
        self.nodes.push(Node { len: 1, kind });
        self.nodes.len() - 1
    }

    fn open_return_cast(&mut self) -> usize {
        self.open(NodeKind::TypeCast {
            target: self.body.return_cast.clone(),
        })
    }

    fn close(&mut self, open: usize) {
        let len = u32::try_from(self.nodes.len() - open).unwrap_or(u32::MAX);
        if let Some(node) = self.nodes.get_mut(open) {
            node.len = len;
        }
    }

    /// A body node and its subtree; a parameter reference becomes the
    /// argument, cast to the declared type.
    fn emit(&mut self, node: NodeRef<'_>) {
        let parameter = self
            .body
            .parameters
            .iter()
            .find(|(at, _)| *at == node.id())
            .map(|(_, index)| *index);
        if let Some(index) = parameter
            && let (Some(argument), Some(target)) = (
                self.arguments.get(index),
                self.body.parameter_casts.get(index),
            )
        {
            let cast = self.open(NodeKind::TypeCast {
                target: target.clone(),
            });
            self.nodes
                .extend(argument.subtree().map(|node| node.node().clone()));
            self.close(cast);
            return;
        }
        let open = self.open(node.kind().clone());
        for child in node.children() {
            self.emit(child);
        }
        self.close(open);
    }

    /// The body's query, `LIMIT 1` appended when asked: the limit count is
    /// the query node's last child but for an offset, which the body has
    /// none of.
    fn emit_query(&mut self, root: NodeRef<'_>, add_limit: bool) {
        if !add_limit {
            self.emit(root);
            return;
        }
        let kind = match root.kind() {
            NodeKind::Query {
                cte_count,
                order_by_count,
                has_limit_offset,
                ..
            } => NodeKind::Query {
                cte_count: *cte_count,
                order_by_count: *order_by_count,
                has_limit_count: true,
                has_limit_offset: *has_limit_offset,
            },
            other => other.clone(),
        };
        let open = self.open(kind);
        for child in root.children() {
            self.emit(child);
        }
        self.nodes.push(Node {
            len: 1,
            kind: NodeKind::Literal(LiteralValue::Integer(1)),
        });
        self.close(open);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::FunctionCatalog;
    use crate::oid::TypeOid;

    fn source(text: &str, arg_types: &[&str], return_type: &str) -> FunctionSource {
        FunctionSource {
            text: EcoString::from(text),
            strict: false,
            has_config: false,
            arg_names: Vec::new(),
            arg_types: arg_types.iter().map(|t| EcoString::from(*t)).collect(),
            return_type: EcoString::from(return_type),
        }
    }

    fn function(
        oid: u32,
        schema: &str,
        name: &str,
        volatility: FunctionVolatility,
        source: FunctionSource,
    ) -> FunctionRow {
        FunctionRow {
            oid: FunctionOid::from_raw(oid),
            schema: EcoString::from(schema),
            name: EcoString::from(name),
            arg_count: u16::try_from(source.arg_types.len()).unwrap_or(0),
            default_count: 0,
            variadic: false,
            volatility,
            language: EcoString::from("sql"),
            security_definer: false,
            returns_set: false,
            return_type: TypeOid::UNSPECIFIED,
            kind: FunctionKind::Function,
            identity: EcoString::from(format!("{schema}.{name}")),
            source_digest: EcoString::from("digest"),
            source: Some(source),
        }
    }

    fn stable(oid: u32, schema: &str, name: &str, source: FunctionSource) -> FunctionRow {
        function(oid, schema, name, FunctionVolatility::Stable, source)
    }

    /// Supabase's `auth.uid()` as shipped, with its own `::uuid`.
    const AUTH_UID: &str = "\n  select coalesce(\n    nullif(current_setting('request.jwt.claim.sub', true), ''),\n    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')\n  )::uuid\n";

    fn catalog() -> FunctionCatalog {
        FunctionCatalog::from_rows(vec![
            stable(20_001, "auth", "uid", source(AUTH_UID, &[], "uuid")),
            stable(
                20_002,
                "public",
                "current_tenant",
                source(
                    "select (current_setting('app.claims', true)::jsonb ->> 'tenant')::int",
                    &[],
                    "integer",
                ),
            ),
            function(
                20_003,
                "public",
                "double_it",
                FunctionVolatility::Immutable,
                source(" SELECT $1 * 2; ", &["integer"], "integer"),
            ),
            stable(
                20_004,
                "public",
                "tenant_name",
                source(
                    "select name from tenants where id = current_setting('app.tenant')::int",
                    &[],
                    "text",
                ),
            ),
            stable(
                20_005,
                "public",
                "owner_of",
                source(
                    "select owner_id from docs where id = $1",
                    &["bigint"],
                    "bigint",
                ),
            ),
            stable(
                20_006,
                "public",
                "self_ref",
                source("select self_ref()", &[], "integer"),
            ),
            stable(
                20_011,
                "public",
                "docs_over",
                source(
                    "select (select count(*) from docs where 1 > $1)",
                    &["bigint"],
                    "bigint",
                ),
            ),
            function(
                20_010,
                "public",
                "square",
                FunctionVolatility::Immutable,
                source("select $1 * $1", &["integer"], "integer"),
            ),
            stable(
                20_007,
                "public",
                "tenant_doc_count",
                source(
                    "select count(*) from docs where tenant = current_tenant()",
                    &[],
                    "bigint",
                ),
            ),
            stable(20_008, "a", "pick", source("select 1", &[], "integer")),
            stable(20_009, "b", "pick", source("select 2", &[], "integer")),
        ])
    }

    fn inline(sql: &str) -> (String, Vec<InlinedCall>) {
        let tree = query_tree_parse(sql).expect("parse");
        let inlined = query_tree_functions_inline(&tree, &catalog())
            .expect("inline")
            .expect("something to inline");
        (inlined.tree.sql(), inlined.calls)
    }

    fn names(calls: &[InlinedCall]) -> Vec<&str> {
        calls.iter().map(|call| call.name.as_str()).collect()
    }

    fn parse(row: &FunctionRow) -> Result<FunctionBody, FunctionBodyError> {
        function_body_parse(row, &mut TypeCasts::default())
    }

    /// The inlined form of `sql`, or `None` when nothing in it inlines.
    fn inline_option(sql: &str) -> Option<String> {
        let tree = query_tree_parse(sql).expect("parse");
        query_tree_functions_inline(&tree, &catalog())
            .expect("inline")
            .map(|inlined| inlined.tree.sql())
    }

    /// A FROM-less body is the expression itself; a body already cast to
    /// the return type is not cast again, so the fold sees one cast.
    #[test]
    fn test_expression_body_replaces_the_call() {
        let (sql, calls) = inline("SELECT id FROM docs WHERE owner = auth.uid()");
        assert_eq!(
            sql,
            "SELECT id FROM docs WHERE owner = (COALESCE(NULLIF(current_setting('request.jwt.claim.sub', true), ''), \
             ((NULLIF(current_setting('request.jwt.claims', true), ''))::jsonb ->> 'sub')))::uuid"
        );
        assert_eq!(names(&calls), ["uid"]);
        assert_eq!(calls[0].schema.as_deref(), Some("auth"));
        assert_eq!(calls[0].arg_count, 0);
        assert_eq!(calls[0].oid, FunctionOid::from_raw(20_001));
    }

    /// An argument is substituted for `$n` under a cast to the declared
    /// type, once per use; the result is cast to the return type.
    #[test]
    fn test_arguments_are_substituted_under_their_casts() {
        let (sql, calls) = inline("SELECT id FROM items WHERE double_it(n + 1) = 20");
        assert_eq!(
            sql,
            "SELECT id FROM items WHERE ((((n + 1))::int4 * 2))::int4 = 20"
        );
        assert_eq!(names(&calls), ["double_it"]);
        assert_eq!(calls[0].schema, None);
    }

    /// A body with a FROM becomes a subquery limited to its first row: a
    /// sublink in a WHERE position, a scalar subquery under a cast or in
    /// the SELECT list.
    #[test]
    fn test_from_body_becomes_a_one_row_subquery() {
        let (sql, _) = inline("SELECT id FROM tenant_rows WHERE label = tenant_name()");
        assert_eq!(
            sql,
            "SELECT id FROM tenant_rows WHERE label = ((SELECT name FROM tenants \
             WHERE id = (current_setting('app.tenant'))::int4 LIMIT 1))::text"
        );
        let (sql, _) = inline("SELECT owner_of(5) AS owner FROM items");
        assert_eq!(
            sql,
            "SELECT ((SELECT owner_id FROM docs WHERE id = (5)::int8 LIMIT 1))::int8 AS owner FROM items"
        );
    }

    /// A column argument to a body with a FROM would be captured by the
    /// body's tables (`docs.id = docs.id`): the call stays, and the calls
    /// inside its arguments are inlined in its place.
    #[test]
    fn test_from_body_keeps_a_call_with_a_column_argument() {
        assert_eq!(
            inline_option("SELECT owner_of(id) AS owner FROM items"),
            None
        );
        assert_eq!(
            inline_option("SELECT docs_over(id) AS l FROM items"),
            None,
            "a subquery inside a FROM-less body is a scope too"
        );
        assert_eq!(
            inline_option("SELECT docs_over(1) AS l FROM items"),
            Some(
                "SELECT ((SELECT COUNT(*) FROM docs WHERE 1 > (1)::int8))::int8 AS l FROM items"
                    .to_owned()
            )
        );
        assert_eq!(
            inline_option("SELECT id FROM items WHERE owner_of(double_it(n)) = 1"),
            Some("SELECT id FROM items WHERE OWNER_OF((((n)::int4 * 2))::int4) = 1".to_owned())
        );
        assert_eq!(
            inline_option("SELECT id FROM items WHERE owner_of($1) = 1"),
            Some(
                "SELECT id FROM items WHERE ((SELECT owner_id FROM docs WHERE id = ($1)::int8 LIMIT 1))::int8 = 1"
                    .to_owned()
            )
        );
    }

    /// A parameter used twice copies its argument twice, as PostgreSQL does,
    /// unless the argument holds a subquery, which PostgreSQL does not
    /// duplicate either.
    #[test]
    fn test_repeated_parameter_copies_its_argument_but_not_a_subquery() {
        assert_eq!(
            inline_option("SELECT square(n + 1) AS s FROM items"),
            Some("SELECT ((((n + 1))::int4 * ((n + 1))::int4))::int4 AS s FROM items".to_owned())
        );
        assert_eq!(
            inline_option("SELECT square((SELECT max(n) FROM items)) AS s"),
            None
        );
    }

    /// The definition a call records is the row's: a change to the body or
    /// to anything else `pg_proc` says about the function is another
    /// definition, and the writer refuses the stale inlining.
    #[test]
    fn test_definition_follows_the_row() {
        let row = stable(1, "public", "f", source("select 1", &[], "integer"));
        let same = parse(&row).expect("parse").definition;
        assert_eq!(parse(&row).expect("parse").definition, same);
        let other_body = FunctionRow {
            source: Some(source("select 2", &[], "integer")),
            ..row.clone()
        };
        assert_ne!(parse(&other_body).expect("parse").definition, same);
        let other_volatility = FunctionRow {
            volatility: FunctionVolatility::Immutable,
            ..row
        };
        assert_ne!(parse(&other_volatility).expect("parse").definition, same);
    }

    /// A body that calls another inlinable function is inlined through it,
    /// each call recorded; the nested call is bare and recorded so.
    #[test]
    fn test_nested_calls_inline_through_passes() {
        let (sql, calls) = inline("SELECT tenant_doc_count() AS c FROM items");
        assert_eq!(
            sql,
            "SELECT ((SELECT COUNT(*) FROM docs WHERE tenant = \
             (((current_setting('app.claims', true))::jsonb ->> 'tenant'))::int4 LIMIT 1))::int8 AS c FROM items"
        );
        assert_eq!(names(&calls), ["tenant_doc_count", "current_tenant"]);
    }

    /// A function calling itself never settles: an error naming it.
    #[test]
    fn test_recursion_is_an_error() {
        let tree = query_tree_parse("SELECT self_ref() AS s FROM items").expect("parse");
        let error = query_tree_functions_inline(&tree, &catalog()).expect_err("recursion");
        assert!(
            matches!(error, FunctionInlineError::TooDeep { ref function, depth } if function == "self_ref" && depth == INLINE_DEPTH),
            "{error}"
        );
    }

    /// Nothing to inline leaves the tree alone: a bare name two schemas
    /// answer, an aggregate call, a name the catalog lacks.
    #[test]
    fn test_ambiguous_and_unknown_calls_stay() {
        for sql in [
            "SELECT id FROM items WHERE pick() = n",
            "SELECT count(*) AS c FROM items",
            "SELECT id FROM items WHERE missing(n) = 1",
        ] {
            let tree = query_tree_parse(sql).expect("parse");
            assert!(
                query_tree_functions_inline(&tree, &catalog())
                    .expect("inline")
                    .is_none(),
                "{sql}"
            );
        }
        let (sql, calls) = inline("SELECT id FROM items WHERE n = a.pick()");
        assert_eq!(sql, "SELECT id FROM items WHERE n = (1)::int4");
        assert_eq!(calls[0].oid, FunctionOid::from_raw(20_008));
    }

    /// The eligibility rules, one refusal each.
    #[test]
    fn test_body_rules() {
        let ok = stable(
            1,
            "public",
            "f",
            source("select $1 + 1", &["integer"], "integer"),
        );
        assert!(parse(&ok).is_ok());
        let refused = |row: FunctionRow| parse(&row).expect_err("refused");
        assert_eq!(
            refused(FunctionRow {
                source: None,
                ..ok.clone()
            }),
            FunctionBodyError::NoSqlBody
        );
        assert_eq!(
            refused(FunctionRow {
                volatility: FunctionVolatility::Volatile,
                ..ok.clone()
            }),
            FunctionBodyError::Volatile
        );
        assert_eq!(
            refused(FunctionRow {
                security_definer: true,
                ..ok.clone()
            }),
            FunctionBodyError::SecurityDefiner
        );
        assert_eq!(
            refused(FunctionRow {
                returns_set: true,
                ..ok.clone()
            }),
            FunctionBodyError::ReturnsSet
        );
        assert_eq!(
            refused(FunctionRow {
                default_count: 1,
                ..ok.clone()
            }),
            FunctionBodyError::VariadicOrDefaulted
        );
        assert_eq!(
            refused(FunctionRow {
                kind: FunctionKind::Window,
                ..ok.clone()
            }),
            FunctionBodyError::NotPlainFunction
        );
        let with = |source: FunctionSource| FunctionRow {
            arg_count: u16::try_from(source.arg_types.len()).unwrap_or(0),
            source: Some(source),
            ..ok.clone()
        };
        assert_eq!(
            refused(with(FunctionSource {
                strict: true,
                ..source("select $1 + 1", &["integer"], "integer")
            })),
            FunctionBodyError::StrictWithArguments
        );
        assert!(
            parse(&with(FunctionSource {
                strict: true,
                ..source("select 1", &[], "integer")
            }))
            .is_ok(),
            "strict without arguments has no NULL to test"
        );
        assert_eq!(
            refused(with(FunctionSource {
                has_config: true,
                ..source("select 1", &[], "integer")
            })),
            FunctionBodyError::SetsConfiguration
        );
        assert_eq!(
            refused(with(source("select $1", &["anyelement"], "anyelement"))),
            FunctionBodyError::PolymorphicType {
                type_name: EcoString::from("anyelement")
            }
        );
        assert_eq!(
            refused(with(source("select 1; select 2", &[], "integer"))),
            FunctionBodyError::NotSingleSelect
        );
        assert_eq!(
            refused(with(source("insert into t values (1)", &[], "integer"))),
            FunctionBodyError::NotSingleSelect
        );
        assert_eq!(
            refused(with(source("select 1, 2", &[], "integer"))),
            FunctionBodyError::NotOneColumn
        );
        assert_eq!(
            refused(with(source("select n from t limit 2", &[], "integer"))),
            FunctionBodyError::LimitNotOne
        );
        assert!(parse(&with(source("select n from t limit 1", &[], "integer"))).is_ok());
        assert_eq!(
            refused(with(source("select $2", &["integer"], "integer"))),
            FunctionBodyError::UndeclaredParameter
        );
        assert_eq!(
            refused(with(source("select col + 1", &[], "integer"))),
            FunctionBodyError::ColumnWithoutFrom
        );
        let named = |text: &str| FunctionSource {
            arg_names: vec![EcoString::from("x")],
            ..source(text, &["integer"], "integer")
        };
        assert!(parse(&with(named("select x + 1"))).is_ok());
        assert_eq!(
            refused(with(named("select n from t where n = x"))),
            FunctionBodyError::NamedParameterWithFrom
        );
        let repeated = parse(&with(source(
            "select $1 * $1 + $2",
            &["integer", "integer"],
            "integer",
        )))
        .expect("parse");
        assert_eq!(repeated.repeated_parameters, [0]);
    }

    /// A named parameter in a FROM-less body is the argument.
    #[test]
    fn test_named_parameter_is_substituted() {
        let catalog = FunctionCatalog::from_rows(vec![function(
            1,
            "public",
            "twice",
            FunctionVolatility::Immutable,
            FunctionSource {
                arg_names: vec![EcoString::from("x")],
                ..source("SELECT x * 2", &["integer"], "integer")
            },
        )]);
        let tree = query_tree_parse("SELECT twice(n) AS t FROM items").expect("parse");
        let inlined = query_tree_functions_inline(&tree, &catalog)
            .expect("inline")
            .expect("inlined");
        assert_eq!(
            inlined.tree.sql(),
            "SELECT (((n)::int4 * 2))::int4 AS t FROM items"
        );
    }

    /// A type name is read as the converter spells a cast.
    #[test]
    fn test_type_names_canonicalise() {
        let target = |name: &str| cast_target_of(&EcoString::from(name));
        assert_eq!(target("integer"), Ok(CastTarget::Int4));
        assert_eq!(target("bigint"), Ok(CastTarget::Int8));
        assert_eq!(target("boolean"), Ok(CastTarget::Bool));
        assert_eq!(
            target("timestamp with time zone"),
            Ok(CastTarget::Other(EcoString::from("timestamptz")))
        );
        assert_eq!(
            target("character varying(20)"),
            Ok(CastTarget::Other(EcoString::from("varchar(20)")))
        );
        assert_eq!(
            target("uuid"),
            Ok(CastTarget::Other(EcoString::from("uuid")))
        );
        assert_eq!(
            target("anyarray"),
            Err(FunctionBodyError::PolymorphicType {
                type_name: EcoString::from("anyarray")
            })
        );
    }
}
