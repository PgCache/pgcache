//! Evaluating a session-term fragment (PGC-580): the fragment `term_fold`
//! recognises becomes one literal under the session's values, with the
//! semantics origin would apply — NULL propagation through `coalesce`,
//! `nullif` and the json access operators, a cast to `json`/`jsonb` parsing
//! its text, a text extraction rendering only what renders the same in json
//! and jsonb. Whatever origin would render itself is left unfolded: the
//! fragment degrades to its bare terms, substituted on their own as before
//! fragments, and the key carries their raw values. A value origin would
//! reject outright (text that is not json) is not degraded, since no form
//! of the read populates: it is unavailable, and the read forwards.

use ecow::EcoString;
use error_set::error_set;
use serde_json::Value as Json;

use super::session_terms::{FoldedTerm, SessionTermContext, SessionTermError, term_value};
use super::term_fold::bare_select_item;
use crate::query::ast::rope::{
    NodeKind, NodeRef, QueryTree, Remap, Replacement, Replacements, Scalar, ScalarKind,
    TreeBuilder, TypeCast, View, WhereSubquery,
};
use crate::query::ast::{JsonOp, LiteralValue, NodeId, ScalarOp};
use crate::query::cast::{CastTarget, cast_target_deparse, cast_target_rejects_text};

error_set! {
    TermFoldError := {
        Term(SessionTermError),
        #[display("the expression does not fold: {reason}")]
        Unfoldable {
            reason: &'static str,
        },
    }
}

/// A value under evaluation: a literal, or a json document a cast to `json`
/// or `jsonb` produced, which field access reads and a text extraction
/// turns back into a literal.
enum Folded {
    Literal(LiteralValue),
    Json(Json),
}

/// Every fragment of `tree` under `context`, in pre-order.
pub(crate) fn query_tree_term_fragments_fold(
    tree: &QueryTree,
    fragments: &[NodeId],
    context: &SessionTermContext,
) -> Vec<FoldedTerm> {
    let root = tree.root().node_ref();
    fragments
        .iter()
        .map(|id| {
            let position = root.at_position(*id);
            match position_term_fold(position, context) {
                Ok(value) => FoldedTerm::Value(value),
                Err(TermFoldError::Term(error)) => FoldedTerm::Unavailable(error),
                Err(TermFoldError::Unfoldable { .. }) => fragment_terms_fold(position, context),
            }
        })
        .collect()
}

/// The bare terms of the fragment at `position`, each its value.
fn fragment_terms_fold(position: NodeRef<'_>, context: &SessionTermContext) -> FoldedTerm {
    let mut values = Vec::new();
    for node in position.subtree() {
        if let NodeKind::Literal(LiteralValue::SessionTerm(term)) = node.kind() {
            match term_value(term, context) {
                Ok(value) => values.push(value),
                Err(error) => return FoldedTerm::Unavailable(error),
            }
        }
    }
    FoldedTerm::Terms(values)
}

/// The positions of the bare terms of the fragment at `position`, in
/// pre-order: what [`FoldedTerm::Terms`] pairs with.
fn fragment_term_positions(position: NodeRef<'_>) -> Vec<NodeId> {
    position
        .subtree()
        .filter(|node| matches!(node.kind(), NodeKind::Literal(LiteralValue::SessionTerm(_))))
        .map(|node| node.id())
        .collect()
}

/// `tree` with the fragments rooted at `fragments` replaced by `values`,
/// pairing in order: what the proxy folded, applied by the writer to the
/// fragments the proxy recorded. A degraded fragment keeps its shape and
/// takes the values at its bare terms; an unavailable one is the error.
pub(crate) fn query_tree_term_fragments_replace(
    tree: &QueryTree,
    fragments: &[NodeId],
    values: &[FoldedTerm],
) -> Result<QueryTree, TermFoldError> {
    query_tree_term_fragments_replace_remapped(tree, fragments, values).map(|(tree, _)| tree)
}

/// [`query_tree_term_fragments_replace`] with where the kept nodes landed:
/// a fragment of several nodes becomes one leaf, so a position recorded on
/// `tree` after it (the inliner's own-column positions) moves.
pub(crate) fn query_tree_term_fragments_replace_remapped(
    tree: &QueryTree,
    fragments: &[NodeId],
    values: &[FoldedTerm],
) -> Result<(QueryTree, Remap), TermFoldError> {
    if fragments.len() != values.len() {
        return Err(unfoldable(
            "the folded values do not pair with the fragments",
        ));
    }
    if fragments.is_empty() {
        let mut builder = TreeBuilder::over(tree);
        builder.keep(tree.root().id());
        return Ok(builder.finish());
    }
    let root = tree.root().node_ref();
    let mut replacements = Replacements::new();
    for (id, value) in fragments.iter().zip(values) {
        match value {
            FoldedTerm::Value(value) => {
                replacements.push(*id, Replacement::Leaf(NodeKind::Literal(value.clone())));
            }
            FoldedTerm::Terms(values) => {
                let positions = fragment_term_positions(root.at_position(*id));
                if positions.len() != values.len() {
                    return Err(unfoldable(
                        "the term values do not pair with the fragment's terms",
                    ));
                }
                for (position, value) in positions.into_iter().zip(values) {
                    replacements.push(
                        position,
                        Replacement::Leaf(NodeKind::Literal(value.clone())),
                    );
                }
            }
            FoldedTerm::Unavailable(error) => return Err(TermFoldError::Term(error.clone())),
        }
    }
    let mut builder = TreeBuilder::over(tree);
    builder.copy(root, &mut replacements);
    Ok(builder.finish())
}

/// The fragment rooted at `position` folded under `context`.
fn position_term_fold(
    position: NodeRef<'_>,
    context: &SessionTermContext,
) -> Result<LiteralValue, TermFoldError> {
    if matches!(position.kind(), NodeKind::WhereSubquery { .. }) {
        let subquery = WhereSubquery::from_ref(position);
        return match bare_select_item(subquery.query()) {
            Some(item) => scalar_term_fold(item, context),
            None => Err(unfoldable("a subquery with a FROM clause")),
        };
    }
    scalar_term_fold(Scalar::from_ref(position), context)
}

/// `expr`, a term fragment, folded to the literal it stands for under
/// `context`.
fn scalar_term_fold(
    expr: Scalar<'_>,
    context: &SessionTermContext,
) -> Result<LiteralValue, TermFoldError> {
    match fold(expr, context)? {
        Folded::Literal(value) => Ok(value),
        Folded::Json(_) => Err(unfoldable("a json value is not a literal")),
    }
}

fn fold(expr: Scalar<'_>, context: &SessionTermContext) -> Result<Folded, TermFoldError> {
    match expr.kind() {
        ScalarKind::Literal(literal) => match literal.value() {
            LiteralValue::SessionTerm(term) => Ok(Folded::Literal(term_value(term, context)?)),
            LiteralValue::Parameter(_) | LiteralValue::Array(..) => Err(unfoldable("a parameter")),
            value @ (LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::OrderedText(_)
            | LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Null
            | LiteralValue::NullWithCast(_)) => Ok(Folded::Literal(value.clone())),
        },
        ScalarKind::TypeCast(cast) => cast_fold(cast, context),
        ScalarKind::Function(function) => {
            let name = function.name();
            if name.eq_ignore_ascii_case("coalesce") {
                for arg in function.args() {
                    let value = fold(arg, context)?;
                    if !is_null(&value) {
                        return Ok(value);
                    }
                }
                Ok(Folded::Literal(LiteralValue::Null))
            } else if name.eq_ignore_ascii_case("nullif") {
                let mut args = function.args();
                let (Some(first), Some(second)) = (args.next(), args.next()) else {
                    return Err(unfoldable("nullif takes two arguments"));
                };
                let first = fold(first, context)?;
                let second = fold(second, context)?;
                if is_null(&first) {
                    return Ok(first);
                }
                // Only two plain strings compare as text; a cast operand
                // compares in its own type, which origin decides.
                match (text_of(&first), text_of(&second)) {
                    (Some(a), Some(b)) if a == b => Ok(Folded::Literal(LiteralValue::Null)),
                    (Some(_), Some(_)) => Ok(first),
                    _ => Err(unfoldable("nullif over values that are not plain text")),
                }
            } else {
                Err(unfoldable("a function call"))
            }
        }
        ScalarKind::Operator(operator) => {
            let ScalarOp::Json(op) = operator.op() else {
                return Err(unfoldable("an arithmetic operator"));
            };
            // The access operators are strict: a NULL document or key is NULL.
            let document = fold(operator.left(), context)?;
            if is_null(&document) {
                return Ok(Folded::Literal(LiteralValue::Null));
            }
            let Folded::Json(document) = document else {
                return Err(unfoldable("json access on a value that is not json"));
            };
            let key = fold(operator.right(), context)?;
            if is_null(&key) {
                return Ok(Folded::Literal(LiteralValue::Null));
            }
            json_access(op, &document, &key)
        }
        ScalarKind::Subquery(subquery) => match bare_select_item(subquery.query()) {
            Some(item) => fold(item, context),
            None => Err(unfoldable("a subquery with a FROM clause")),
        },
        ScalarKind::Column(_) => Err(unfoldable("a column")),
        ScalarKind::Case(_) => Err(unfoldable("a CASE expression")),
        ScalarKind::Array(_) => Err(unfoldable("an array")),
    }
}

fn cast_fold(cast: TypeCast<'_>, context: &SessionTermContext) -> Result<Folded, TermFoldError> {
    let target = cast.target();
    let inner = fold(cast.expr(), context)?;
    let to_json = matches!(
        target,
        CastTarget::Other(name) if name.as_str() == "json" || name.as_str() == "jsonb"
    );
    match inner {
        Folded::Json(document) if to_json => Ok(Folded::Json(document)),
        Folded::Json(_) => Err(unfoldable("a cast of a json value")),
        // A cast literal already has a type, and the outer cast converts
        // from that type (`'007'::int::text` is `'7'`): origin's to apply.
        Folded::Literal(LiteralValue::StringWithCast(..)) => {
            Err(unfoldable("a cast of a cast literal"))
        }
        Folded::Literal(LiteralValue::String(text)) => {
            if to_json {
                serde_json::from_str(&text).map(Folded::Json).map_err(|_| {
                    TermFoldError::Term(SessionTermError::ValueRejected {
                        reason: "a json document origin would reject",
                    })
                })
            } else if let Some(reason) = cast_target_rejects_text(target, &text) {
                Err(TermFoldError::Term(SessionTermError::ValueRejected {
                    reason,
                }))
            } else {
                Ok(Folded::Literal(LiteralValue::StringWithCast(
                    text,
                    cast_target_deparse(target).into(),
                )))
            }
        }
        Folded::Literal(LiteralValue::Null | LiteralValue::NullWithCast(_)) => Ok(Folded::Literal(
            LiteralValue::NullWithCast(cast_target_deparse(target).into()),
        )),
        Folded::Literal(
            LiteralValue::OrderedText(_)
            | LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Parameter(_)
            | LiteralValue::SessionTerm(_)
            | LiteralValue::Array(..),
        ) => Err(unfoldable("a cast of a non-text literal")),
    }
}

/// `document op key`: field access by text key or array index, path access
/// by a `{a,b}` text path, as json (`->`, `#>`) or as text (`->>`, `#>>`).
/// A missing field is SQL NULL, as on origin; a json null is a json value
/// to `->` and `#>` and SQL NULL to the text forms. A negative index counts
/// from the end on origin and is left to it.
fn json_access(op: JsonOp, document: &Json, key: &Folded) -> Result<Folded, TermFoldError> {
    let found = match op {
        JsonOp::Field | JsonOp::FieldText => match key {
            Folded::Literal(LiteralValue::String(name) | LiteralValue::StringWithCast(name, _)) => {
                document.get(name.as_str())
            }
            Folded::Literal(LiteralValue::Integer(index)) => {
                if *index < 0 {
                    return Err(unfoldable("a negative json index"));
                }
                usize::try_from(*index)
                    .ok()
                    .and_then(|index| document.get(index))
            }
            Folded::Literal(_) | Folded::Json(_) => {
                return Err(unfoldable("a json key that is not a literal"));
            }
        },
        JsonOp::Path | JsonOp::PathText => {
            let Folded::Literal(LiteralValue::String(path) | LiteralValue::StringWithCast(path, _)) =
                key
            else {
                return Err(unfoldable("a json path that is not a literal"));
            };
            let mut current = Some(document);
            for step in text_array_elements(path)? {
                current = match current {
                    Some(value @ Json::Array(_)) => {
                        if step.parse::<i64>().is_ok_and(|index| index < 0) {
                            return Err(unfoldable("a negative json index"));
                        }
                        step.parse::<usize>()
                            .ok()
                            .and_then(|index| value.get(index))
                    }
                    Some(value @ Json::Object(_)) => value.get(step),
                    Some(Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_)) | None => {
                        None
                    }
                };
            }
            current
        }
        JsonOp::Contains
        | JsonOp::ContainedBy
        | JsonOp::HasKey
        | JsonOp::HasAnyKey
        | JsonOp::HasAllKeys
        | JsonOp::PathExists
        | JsonOp::PathMatch
        | JsonOp::Concat
        | JsonOp::DeletePath => return Err(unfoldable("a json operator other than access")),
    };
    let Some(value) = found else {
        return Ok(Folded::Literal(LiteralValue::Null));
    };
    match op {
        JsonOp::Field | JsonOp::Path => Ok(Folded::Json(value.clone())),
        JsonOp::FieldText | JsonOp::PathText => match value {
            Json::Null => Ok(Folded::Literal(LiteralValue::Null)),
            Json::String(text) => Ok(Folded::Literal(LiteralValue::String(text.as_str().into()))),
            Json::Bool(flag) => Ok(Folded::Literal(LiteralValue::String(
                if *flag { "true" } else { "false" }.into(),
            ))),
            // An integer renders the same in json and jsonb; a decimal or
            // exponent form does not (jsonb normalises it to numeric).
            Json::Number(number) if number.is_i64() || number.is_u64() => Ok(Folded::Literal(
                LiteralValue::String(number.to_string().into()),
            )),
            Json::Number(_) | Json::Array(_) | Json::Object(_) => {
                Err(unfoldable("a json value whose text form origin renders"))
            }
        },
        JsonOp::Contains
        | JsonOp::ContainedBy
        | JsonOp::HasKey
        | JsonOp::HasAnyKey
        | JsonOp::HasAllKeys
        | JsonOp::PathExists
        | JsonOp::PathMatch
        | JsonOp::Concat
        | JsonOp::DeletePath => Err(unfoldable("a json operator other than access")),
    }
}

/// The elements of a `{a,b,c}` text array literal without quoting; anything
/// quoted or escaped is left to origin.
fn text_array_elements(path: &str) -> Result<Vec<&str>, TermFoldError> {
    let inner = path
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| unfoldable("a json path that is not an array literal"))?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    if inner.contains(['"', '\\']) {
        return Err(unfoldable("a quoted json path element"));
    }
    Ok(inner.split(',').map(str::trim).collect())
}

fn is_null(value: &Folded) -> bool {
    matches!(
        value,
        Folded::Literal(LiteralValue::Null | LiteralValue::NullWithCast(_))
    )
}

fn text_of(value: &Folded) -> Option<&EcoString> {
    match value {
        Folded::Literal(LiteralValue::String(text)) => Some(text),
        Folded::Literal(_) | Folded::Json(_) => None,
    }
}

fn unfoldable(reason: &'static str) -> TermFoldError {
    TermFoldError::Unfoldable { reason }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::query_tree_parse;
    use crate::query::ast::rope::deparse::query_deparse_with;
    use crate::query::ast::rope::{Body, Plain, SelectItem};
    use crate::query::transform::CustomSettingValue;
    use crate::query::transform::term_fold::query_tree_term_fragments;

    fn tree_of(expr: &str) -> QueryTree {
        query_tree_parse(&format!("SELECT {expr} FROM t")).expect("parse")
    }

    fn first_item(tree: &QueryTree) -> Scalar<'_> {
        let Body::Select(select) = tree.root().body() else {
            panic!("a SELECT");
        };
        let Some(SelectItem::Expr(item)) = select.items().and_then(|mut items| items.next()) else {
            panic!("one item");
        };
        item.expr()
    }

    fn plain(tree: &QueryTree) -> String {
        let mut buf = String::new();
        query_deparse_with(tree.root(), &mut Plain, &mut buf);
        buf
    }

    fn context(user: Option<&str>, settings: &[(&str, CustomSettingValue)]) -> SessionTermContext {
        SessionTermContext {
            current_user: user.map(EcoString::from),
            session_user: user.map(EcoString::from),
            settings: settings
                .iter()
                .map(|(name, value)| (EcoString::from(*name), value.clone()))
                .collect(),
        }
    }

    fn known(value: &str) -> CustomSettingValue {
        CustomSettingValue::Known(value.into())
    }

    fn folded(expr: &str, context: &SessionTermContext) -> Result<LiteralValue, TermFoldError> {
        let tree = tree_of(expr);
        scalar_term_fold(first_item(&tree), context)
    }

    fn fold_ok(expr: &str, context: &SessionTermContext) -> LiteralValue {
        folded(expr, context).expect("the fragment folds")
    }

    fn text(value: &str) -> LiteralValue {
        LiteralValue::String(value.into())
    }

    fn text_as(value: &str, cast: &str) -> LiteralValue {
        LiteralValue::StringWithCast(value.into(), cast.into())
    }

    const SUB: &str = "11111111-1111-1111-1111-111111111111";
    const SUB2: &str = "22222222-2222-2222-2222-222222222222";
    const CLAIMS: &str = r#"{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated","aud":["a","b"],"n":7,"x":1.5,"nil":null}"#;

    #[test]
    fn test_term_and_cast_fold_to_the_sessions_value() {
        let ctx = context(Some("alice"), &[("app.tenant", known("7"))]);
        assert_eq!(fold_ok("current_setting('app.tenant')", &ctx), text("7"));
        assert_eq!(
            fold_ok("current_setting('app.tenant')::int", &ctx),
            text_as("7", "int4")
        );
        assert_eq!(fold_ok("current_user", &ctx), text("alice"));
        assert_eq!(
            fold_ok("current_user::text", &ctx),
            text_as("alice", "text")
        );
        assert_eq!(
            fold_ok("(select current_setting('app.tenant'))::int", &ctx),
            text_as("7", "int4")
        );
    }

    #[test]
    fn test_missing_ok_and_coalesce_follow_null_semantics() {
        let ctx = context(
            Some("alice"),
            &[
                ("app.a", CustomSettingValue::Unset),
                ("app.b", known("")),
                ("app.c", known("c")),
            ],
        );
        assert_eq!(
            fold_ok("current_setting('app.a', true)", &ctx),
            LiteralValue::Null
        );
        assert_eq!(
            fold_ok("current_setting('app.a', true)::uuid", &ctx),
            LiteralValue::NullWithCast("uuid".into())
        );
        assert_eq!(
            fold_ok(
                "coalesce(current_setting('app.a', true), current_setting('app.c', true))",
                &ctx
            ),
            text("c")
        );
        assert_eq!(
            fold_ok("nullif(current_setting('app.b', true), '')", &ctx),
            LiteralValue::Null
        );
        assert_eq!(
            fold_ok("nullif(current_setting('app.c', true), '')", &ctx),
            text("c")
        );
        assert_eq!(
            fold_ok("nullif(current_setting('app.a', true), '')", &ctx),
            LiteralValue::Null
        );
    }

    #[test]
    fn test_unset_or_unknown_terms_are_term_errors() {
        let ctx = context(None, &[("app.a", CustomSettingValue::Unset)]);
        assert!(matches!(
            folded("current_setting('app.a')", &ctx),
            Err(TermFoldError::Term(SessionTermError::SettingUnset { .. }))
        ));
        assert!(matches!(
            folded("current_setting('app.z')", &ctx),
            Err(TermFoldError::Term(SessionTermError::SettingUnknown { .. }))
        ));
        assert!(matches!(
            folded("current_user", &ctx),
            Err(TermFoldError::Term(SessionTermError::UserUnknown))
        ));
    }

    #[test]
    fn test_json_access_extracts_a_claim() {
        let ctx = context(None, &[("request.jwt.claims", known(CLAIMS))]);
        let claims = "current_setting('request.jwt.claims', true)::jsonb";
        assert_eq!(fold_ok(&format!("{claims} ->> 'sub'"), &ctx), text(SUB));
        assert_eq!(
            fold_ok(&format!("({claims} ->> 'sub')::uuid"), &ctx),
            text_as(SUB, "uuid")
        );
        assert_eq!(fold_ok(&format!("{claims} ->> 'n'"), &ctx), text("7"));
        assert_eq!(
            fold_ok(&format!("{claims} -> 'aud' ->> 1"), &ctx),
            text("b")
        );
        assert_eq!(
            fold_ok(&format!("{claims} #>> '{{aud,0}}'"), &ctx),
            text("a")
        );
        assert_eq!(
            fold_ok(&format!("{claims} ->> 'missing'"), &ctx),
            LiteralValue::Null
        );
        assert_eq!(
            fold_ok(&format!("{claims} ->> 'nil'"), &ctx),
            LiteralValue::Null
        );
        assert_eq!(
            fold_ok(
                "current_setting('request.jwt.claims', true)::json ->> 'role'",
                &ctx
            ),
            text("authenticated")
        );
    }

    /// Supabase's `auth.uid()` body, as PGC-579 will inline it.
    #[test]
    fn test_supabase_uid_body_folds_to_the_claim() {
        let body = "nullif(coalesce(current_setting('request.jwt.claim.sub', true), \
                    (current_setting('request.jwt.claims', true)::jsonb ->> 'sub')), '')::uuid";
        let by_claims = context(
            None,
            &[
                ("request.jwt.claim.sub", CustomSettingValue::Unset),
                ("request.jwt.claims", known(CLAIMS)),
            ],
        );
        assert_eq!(fold_ok(body, &by_claims), text_as(SUB, "uuid"));
        let by_claim = context(
            None,
            &[
                ("request.jwt.claim.sub", known(SUB2)),
                ("request.jwt.claims", CustomSettingValue::Unset),
            ],
        );
        assert_eq!(fold_ok(body, &by_claim), text_as(SUB2, "uuid"));
        let no_token = context(
            None,
            &[
                ("request.jwt.claim.sub", CustomSettingValue::Unset),
                ("request.jwt.claims", CustomSettingValue::Unset),
            ],
        );
        assert_eq!(
            fold_ok(body, &no_token),
            LiteralValue::NullWithCast("uuid".into())
        );
    }

    /// What origin would render itself is left to origin: a decimal number
    /// as text, an object as text, a json value as the result.
    #[test]
    fn test_values_origin_renders_are_unfoldable() {
        let ctx = context(None, &[("request.jwt.claims", known(CLAIMS))]);
        let claims = "current_setting('request.jwt.claims', true)::jsonb";
        for expr in [
            format!("{claims} ->> 'x'"),
            format!("{claims} ->> 'aud'"),
            format!("{claims} -> 'sub'"),
            format!("{claims}::text"),
        ] {
            assert!(
                matches!(folded(&expr, &ctx), Err(TermFoldError::Unfoldable { .. })),
                "{expr}"
            );
        }
    }

    /// The writer's replacement pairs the folded values with the fragments
    /// by pre-order position, the FROM-less wrapper in a predicate included.
    #[test]
    fn test_fragments_fold_and_replace_in_order() {
        let tree = query_tree_parse(
            "SELECT id, coalesce(current_setting('app.a', true), 'none') AS a FROM t \
             WHERE owner = current_user AND tenant = current_setting('app.tenant')::int \
             AND note = lower(current_setting('app.note'))",
        )
        .expect("parse");
        let fragments = query_tree_term_fragments(&tree);
        let ctx = context(
            Some("alice"),
            &[
                ("app.a", CustomSettingValue::Unset),
                ("app.tenant", known("7")),
                ("app.note", known("hi")),
            ],
        );
        let values = query_tree_term_fragments_fold(&tree, &fragments, &ctx);
        assert_eq!(
            values,
            [
                text("none"),
                text("alice"),
                text_as("7", "int4"),
                text("hi")
            ]
            .map(FoldedTerm::Value)
        );
        let replaced =
            query_tree_term_fragments_replace(&tree, &fragments, &values).expect("replace");
        assert_eq!(
            plain(&replaced),
            "SELECT id, 'none' AS a FROM t WHERE owner = 'alice' AND tenant = '7'::int4 \
             AND note = LOWER('hi')"
        );
        assert!(query_tree_term_fragments(&replaced).is_empty());
        assert!(matches!(
            query_tree_term_fragments_replace(&tree, &fragments, &values[..3]),
            Err(TermFoldError::Unfoldable { .. })
        ));

        let wrapped = query_tree_parse(
            "SELECT id FROM t WHERE owner = (SELECT current_setting('app.who')) AND id > 1",
        )
        .expect("parse");
        let fragments = query_tree_term_fragments(&wrapped);
        let ctx = context(None, &[("app.who", known("ann"))]);
        let values = query_tree_term_fragments_fold(&wrapped, &fragments, &ctx);
        let replaced =
            query_tree_term_fragments_replace(&wrapped, &fragments, &values).expect("replace");
        assert_eq!(
            plain(&replaced),
            "SELECT id FROM t WHERE owner = 'ann' AND id > 1"
        );
    }

    /// A fragment whose shape is origin's to evaluate (a typed `nullif`, a
    /// json value compared whole) degrades to its bare terms: their values
    /// key the read and are substituted on their own, as before fragments.
    /// A term the session cannot supply is carried as the reason, which the
    /// replace refuses.
    #[test]
    fn test_unfoldable_fragment_degrades_to_its_terms() {
        let tree = query_tree_parse(
            "SELECT id FROM t WHERE tenant = nullif(current_setting('app.tenant')::int, 0) \
             AND claims = current_setting('app.claims')::jsonb \
             AND who = current_setting('app.who', true)",
        )
        .expect("parse");
        let fragments = query_tree_term_fragments(&tree);
        assert_eq!(fragments.len(), 3);
        let ctx = context(
            None,
            &[
                ("app.tenant", known("7")),
                ("app.claims", known(r#"{"a":1}"#)),
                ("app.who", CustomSettingValue::Unset),
            ],
        );
        let values = query_tree_term_fragments_fold(&tree, &fragments, &ctx);
        assert_eq!(
            values,
            [
                FoldedTerm::Terms(vec![text("7")]),
                FoldedTerm::Terms(vec![text(r#"{"a":1}"#)]),
                FoldedTerm::Value(LiteralValue::Null),
            ]
        );
        let replaced =
            query_tree_term_fragments_replace(&tree, &fragments, &values).expect("replace");
        assert_eq!(
            plain(&replaced),
            "SELECT id FROM t WHERE tenant = NULLIF(('7')::int4, 0) \
             AND claims = ('{\"a\":1}')::jsonb AND who = NULL"
        );

        let unknown = context(None, &[("app.claims", known("{}"))]);
        let values = query_tree_term_fragments_fold(&tree, &fragments, &unknown);
        assert!(matches!(
            values[0],
            FoldedTerm::Unavailable(SessionTermError::SettingUnknown { .. })
        ));
        assert!(matches!(
            query_tree_term_fragments_replace(&tree, &fragments, &values),
            Err(TermFoldError::Term(SessionTermError::SettingUnknown { .. }))
        ));
    }

    /// Text that is not json fails the cast on origin whatever form the
    /// read takes, so it is not degraded to its terms: the fragment is
    /// unavailable and the read forwards.
    #[test]
    fn test_a_value_origin_rejects_is_unavailable() {
        let tree = query_tree_parse(
            "SELECT id FROM t WHERE owner = current_setting('app.claims')::jsonb ->> 'sub'",
        )
        .expect("parse");
        let fragments = query_tree_term_fragments(&tree);
        let ctx = context(None, &[("app.claims", known(""))]);
        let values = query_tree_term_fragments_fold(&tree, &fragments, &ctx);
        assert_eq!(
            values,
            [FoldedTerm::Unavailable(SessionTermError::ValueRejected {
                reason: "a json document origin would reject",
            })]
        );
        assert!(matches!(
            query_tree_term_fragments_replace(&tree, &fragments, &values),
            Err(TermFoldError::Term(SessionTermError::ValueRejected { .. }))
        ));
    }

    /// Text the cast's type rejects (`abc` as an integer, a `sub` claim that
    /// is no uuid) fails on origin whatever form the read takes, like text
    /// that is not json: the fragment is unavailable and the read forwards.
    /// A type the fold has no rule for is cast blind, as before (PGC-596).
    #[test]
    fn test_text_the_cast_rejects_is_unavailable() {
        let rejected = |expr: &str, ctx: &SessionTermContext| {
            matches!(
                folded(expr, ctx),
                Err(TermFoldError::Term(SessionTermError::ValueRejected { .. }))
            )
        };
        let ctx = context(None, &[("app.a", known("abc"))]);
        assert!(rejected("current_setting('app.a')::int", &ctx));
        assert!(rejected("current_setting('app.a')::bigint", &ctx));
        assert!(rejected("current_setting('app.a')::numeric", &ctx));
        assert!(rejected("current_setting('app.a')::bool", &ctx));
        assert!(rejected("current_setting('app.a')::uuid", &ctx));
        assert!(rejected(
            "coalesce(nullif(current_setting('app.a', true), ''), 'x')::uuid",
            &ctx
        ));
        assert_eq!(
            fold_ok("current_setting('app.a')::timestamptz", &ctx),
            LiteralValue::StringWithCast("abc".into(), "timestamptz".into()),
            "a type without a rule is cast blind"
        );
        let ctx = context(None, &[("app.a", known(" 42 "))]);
        assert_eq!(
            fold_ok("current_setting('app.a')::int", &ctx),
            LiteralValue::StringWithCast(" 42 ".into(), "int4".into()),
            "the value is kept as written, origin trims it"
        );
    }

    /// A cast over a cast literal converts between types (`'007'::int::text`
    /// is `'7'`), and `nullif` over a cast operand compares in that type:
    /// both are origin's to evaluate.
    #[test]
    fn test_cast_chains_and_typed_nullif_are_unfoldable() {
        let ctx = context(None, &[("app.a", known("007"))]);
        for expr in [
            "current_setting('app.a')::int::text",
            "nullif(current_setting('app.a')::int, '7')",
            "nullif(current_setting('app.a'), '7'::int)",
            "coalesce(current_setting('app.a')::int, '7'::int)::text",
        ] {
            assert!(
                matches!(folded(expr, &ctx), Err(TermFoldError::Unfoldable { .. })),
                "{expr}"
            );
        }
        assert_eq!(
            fold_ok("nullif(current_setting('app.a'), '007')::int", &ctx),
            LiteralValue::NullWithCast("int4".into())
        );
    }

    /// `->` and `#>` on a json null yield a json null, not SQL NULL: a
    /// `coalesce` over it keeps it, and a text extraction from it is NULL.
    /// A negative index counts from the end on origin and is left to it.
    #[test]
    fn test_json_null_stays_json_and_negative_indexes_are_unfoldable() {
        let ctx = context(None, &[("request.jwt.claims", known(CLAIMS))]);
        let claims = "current_setting('request.jwt.claims', true)::jsonb";
        assert_eq!(
            fold_ok(
                &format!("coalesce({claims} -> 'nil', {claims} -> 'aud') ->> 0"),
                &ctx
            ),
            LiteralValue::Null
        );
        assert_eq!(
            fold_ok(
                &format!("coalesce({claims} -> 'missing', {claims} -> 'aud') ->> 0"),
                &ctx
            ),
            text("a")
        );
        assert_eq!(
            fold_ok(&format!("{claims} #> '{{nil}}' ->> 'x'"), &ctx),
            LiteralValue::Null
        );
        for expr in [
            format!("{claims} -> 'nil'"),
            format!("{claims} -> 'aud' ->> -1"),
            format!("{claims} #>> '{{aud,-1}}'"),
        ] {
            assert!(
                matches!(folded(&expr, &ctx), Err(TermFoldError::Unfoldable { .. })),
                "{expr}"
            );
        }
    }
}
