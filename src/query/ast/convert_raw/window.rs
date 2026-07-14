//! Window specifications: `frameOptions` decoding, `OVER (...)` / named-window
//! resolution, and frame bounds.

use std::collections::HashMap;

use ecow::EcoString;
use smallvec::SmallVec;

use pg_query::pg_nodes as pg;

use super::super::raw::{NodePtr, cast, cstr, list_nodes, node_tag};
use super::super::*;
use super::{null_order_map, order_dir_map, scalar_expr_convert};

// Window `frameOptions` bitmask from PostgreSQL `parsenodes.h`. libpg_query
// exposes these only as C `#define`s, which bindgen does not emit, so they are
// mirrored here.
const FRAMEOPTION_NONDEFAULT: i32 = 0x00001;
const FRAMEOPTION_RANGE: i32 = 0x00002;
const FRAMEOPTION_ROWS: i32 = 0x00004;
const FRAMEOPTION_GROUPS: i32 = 0x00008;
const FRAMEOPTION_START_UNBOUNDED_PRECEDING: i32 = 0x00020;
const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: i32 = 0x00100;
const FRAMEOPTION_START_CURRENT_ROW: i32 = 0x00200;
const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;
const FRAMEOPTION_START_OFFSET_PRECEDING: i32 = 0x00800;
const FRAMEOPTION_END_OFFSET_PRECEDING: i32 = 0x01000;
const FRAMEOPTION_START_OFFSET_FOLLOWING: i32 = 0x02000;
const FRAMEOPTION_END_OFFSET_FOLLOWING: i32 = 0x04000;
const FRAMEOPTION_EXCLUDE_CURRENT_ROW: i32 = 0x08000;
const FRAMEOPTION_EXCLUDE_GROUP: i32 = 0x10000;
const FRAMEOPTION_EXCLUDE_TIES: i32 = 0x20000;

/// Convert a function's `OVER` clause. A bare `OVER w` (`name` set, no inline
/// clauses) becomes a deferred reference resolved later against the SELECT's
/// `WINDOW` clause (PGC-280); an inline `OVER (...)` is converted directly.
pub(super) unsafe fn window_def_convert(
    win_def: *const pg::WindowDef,
) -> Result<WindowSpec, AstError> {
    unsafe {
        let name = cstr((*win_def).name);
        if !name.is_empty() {
            return Ok(WindowSpec {
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
                ref_name: Some(EcoString::from(name)),
            });
        }
        window_spec_from_clauses(win_def)
    }
}

/// Convert the inline PARTITION BY / ORDER BY / frame clauses of a `WindowDef`,
/// ignoring its `name` (used both for `OVER (...)` and for the definitions in a
/// `WINDOW` clause). `OVER (w ...)` frame inheritance (`refname`) is not
/// supported and forwards.
pub(super) unsafe fn window_spec_from_clauses(
    win_def: *const pg::WindowDef,
) -> Result<WindowSpec, AstError> {
    unsafe {
        if !cstr((*win_def).refname).is_empty() {
            return Err(AstError::UnsupportedFeature {
                feature: "window definition inheriting another window".to_owned(),
            });
        }
        let partition_by = list_nodes((*win_def).partitionClause)
            .map(|n| scalar_expr_convert(n))
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = window_order_by_convert((*win_def).orderClause)?.into_vec();
        let frame = window_frame_convert(win_def)?;
        Ok(WindowSpec {
            partition_by,
            order_by,
            frame,
            ref_name: None,
        })
    }
}

/// Build the `name → WindowSpec` map from a SELECT's `WINDOW` clause, so that
/// `OVER w` references can be resolved to their definitions (PGC-280).
pub(super) unsafe fn window_clause_extract(
    window_clause: *const pg::List,
) -> Result<HashMap<EcoString, WindowSpec>, AstError> {
    unsafe {
        let mut defs = HashMap::new();
        for node in list_nodes(window_clause) {
            if node_tag(node) != pg::NodeTag_T_WindowDef {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("WINDOW clause node type: {:?}", node_tag(node)),
                });
            }
            let win_def = cast::<pg::WindowDef>(node);
            let name = cstr((*win_def).name);
            if name.is_empty() {
                return Err(AstError::UnsupportedFeature {
                    feature: "unnamed WINDOW clause entry".to_owned(),
                });
            }
            defs.insert(EcoString::from(name), window_spec_from_clauses(win_def)?);
        }
        Ok(defs)
    }
}

/// Replace `OVER w` references in the SELECT list with their definitions
/// (PGC-280). An unresolved reference left behind is caught by
/// `window_refs_assert_resolved` and forwards the whole query.
pub(super) fn select_columns_window_refs_resolve(
    columns: &mut SelectColumns,
    defs: &HashMap<EcoString, WindowSpec>,
) -> Result<(), AstError> {
    if let SelectColumns::Columns(cols) = columns {
        for col in cols {
            if let SelectColumn::Expr { expr, .. } = col {
                scalar_expr_window_refs_resolve(expr, defs)?;
            }
        }
    }
    Ok(())
}

pub(super) fn scalar_expr_window_refs_resolve(
    expr: &mut ScalarExpr,
    defs: &HashMap<EcoString, WindowSpec>,
) -> Result<(), AstError> {
    match expr {
        ScalarExpr::Function(func) => {
            if let Some(over) = &mut func.over
                && let Some(name) = over.ref_name.take()
            {
                *over = defs
                    .get(&name)
                    .cloned()
                    .ok_or_else(|| AstError::UnsupportedFeature {
                        feature: format!("reference to undefined window {name}"),
                    })?;
            }
            for arg in &mut func.args {
                scalar_expr_window_refs_resolve(arg, defs)?;
            }
        }
        ScalarExpr::Arithmetic(arith) => {
            scalar_expr_window_refs_resolve(&mut arith.left, defs)?;
            scalar_expr_window_refs_resolve(&mut arith.right, defs)?;
        }
        ScalarExpr::Case(case) => {
            if let Some(arg) = &mut case.arg {
                scalar_expr_window_refs_resolve(arg, defs)?;
            }
            for when in &mut case.whens {
                scalar_expr_window_refs_resolve(&mut when.result, defs)?;
            }
            if let Some(default) = &mut case.default {
                scalar_expr_window_refs_resolve(default, defs)?;
            }
        }
        ScalarExpr::Array(elems) => {
            for e in elems {
                scalar_expr_window_refs_resolve(e, defs)?;
            }
        }
        ScalarExpr::TypeCast { expr, .. } => {
            scalar_expr_window_refs_resolve(expr, defs)?;
        }
        ScalarExpr::Column(_) | ScalarExpr::Literal(_) | ScalarExpr::Subquery(_) => {}
    }
    Ok(())
}

/// Fail loud on any window reference the SELECT-list resolution did not reach
/// (e.g. `OVER w` in ORDER BY). Returning an error forwards the query, so an
/// unresolved reference never silently deparses to `OVER ()` (PGC-280).
pub(super) fn window_refs_assert_resolved(query: &QueryExpr) -> Result<(), AstError> {
    if query.nodes::<WindowSpec>().any(|w| w.ref_name.is_some()) {
        return Err(AstError::UnsupportedFeature {
            feature: "window reference outside the SELECT list".to_owned(),
        });
    }
    Ok(())
}

/// Decode the frame clause from a `WindowDef`. Returns `None` for the SQL
/// default frame (`NONDEFAULT` bit clear). Dropping a non-default frame is a
/// silent wrong result (PGC-279), so every explicit frame must round-trip.
pub(super) unsafe fn window_frame_convert(
    win_def: *const pg::WindowDef,
) -> Result<Option<WindowFrame>, AstError> {
    unsafe {
        let opts = (*win_def).frameOptions;
        if opts & FRAMEOPTION_NONDEFAULT == 0 {
            return Ok(None);
        }
        let mode = if opts & FRAMEOPTION_RANGE != 0 {
            FrameMode::Range
        } else if opts & FRAMEOPTION_ROWS != 0 {
            FrameMode::Rows
        } else if opts & FRAMEOPTION_GROUPS != 0 {
            FrameMode::Groups
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("window frame mode (frameOptions={opts:#x})"),
            });
        };
        let start = frame_bound_convert(
            opts,
            (*win_def).startOffset as NodePtr,
            FRAMEOPTION_START_UNBOUNDED_PRECEDING,
            FRAMEOPTION_START_CURRENT_ROW,
            FRAMEOPTION_START_OFFSET_PRECEDING,
            FRAMEOPTION_START_OFFSET_FOLLOWING,
            // START_UNBOUNDED_FOLLOWING is disallowed by PostgreSQL as a start.
            0,
            "start",
        )?;
        let end = frame_bound_convert(
            opts,
            (*win_def).endOffset as NodePtr,
            // END_UNBOUNDED_PRECEDING is disallowed by PostgreSQL as an end.
            0,
            FRAMEOPTION_END_CURRENT_ROW,
            FRAMEOPTION_END_OFFSET_PRECEDING,
            FRAMEOPTION_END_OFFSET_FOLLOWING,
            FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
            "end",
        )?;
        let exclusion = if opts & FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
            FrameExclusion::CurrentRow
        } else if opts & FRAMEOPTION_EXCLUDE_GROUP != 0 {
            FrameExclusion::Group
        } else if opts & FRAMEOPTION_EXCLUDE_TIES != 0 {
            FrameExclusion::Ties
        } else {
            FrameExclusion::NoOthers
        };
        Ok(Some(WindowFrame {
            mode,
            start,
            end,
            exclusion,
        }))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn frame_bound_convert(
    opts: i32,
    offset: NodePtr,
    unbounded_preceding: i32,
    current_row: i32,
    offset_preceding: i32,
    offset_following: i32,
    unbounded_following: i32,
    which: &str,
) -> Result<FrameBound, AstError> {
    unsafe {
        if unbounded_preceding != 0 && opts & unbounded_preceding != 0 {
            Ok(FrameBound::UnboundedPreceding)
        } else if unbounded_following != 0 && opts & unbounded_following != 0 {
            Ok(FrameBound::UnboundedFollowing)
        } else if opts & current_row != 0 {
            Ok(FrameBound::CurrentRow)
        } else if opts & offset_preceding != 0 {
            Ok(FrameBound::OffsetPreceding(Box::new(scalar_expr_convert(
                offset,
            )?)))
        } else if opts & offset_following != 0 {
            Ok(FrameBound::OffsetFollowing(Box::new(scalar_expr_convert(
                offset,
            )?)))
        } else {
            Err(AstError::UnsupportedFeature {
                feature: format!("window frame {which} bound (frameOptions={opts:#x})"),
            })
        }
    }
}

pub(super) unsafe fn window_order_by_convert(
    order_clause: *const pg::List,
) -> Result<SmallVec<[OrderByClause; 1]>, AstError> {
    unsafe {
        let mut order_by = SmallVec::new();
        for sort_node in list_nodes(order_clause) {
            if node_tag(sort_node) != pg::NodeTag_T_SortBy {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("ORDER BY node type: {:?}", node_tag(sort_node)),
                });
            }
            order_by.push(sort_by_to_order_clause(cast::<pg::SortBy>(sort_node))?);
        }
        Ok(order_by)
    }
}

pub(super) unsafe fn sort_by_to_order_clause(
    sort_by: *const pg::SortBy,
) -> Result<OrderByClause, AstError> {
    unsafe {
        let expr_node = (*sort_by).node as NodePtr;
        if expr_node.is_null() {
            return Err(AstError::UnsupportedFeature {
                feature: "ORDER BY without expression".to_owned(),
            });
        }
        let expr = scalar_expr_convert(expr_node)?;
        let direction = order_dir_map((*sort_by).sortby_dir)?;
        let null_order = null_order_map((*sort_by).sortby_nulls)?;
        Ok(OrderByClause {
            expr,
            direction,
            null_order,
        })
    }
}
