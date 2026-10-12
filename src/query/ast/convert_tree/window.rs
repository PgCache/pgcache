//! Window specifications: `frameOptions` decoding, `OVER (...)` and
//! named-window resolution, frame bounds and ORDER BY lists.

use std::collections::HashMap;

use ecow::EcoString;
use pg_query::pg_nodes as pg;

use super::scalar::scalar_emit;
use crate::query::ast::convert_raw::{null_order_map, order_dir_map};
use crate::query::ast::raw::{NodePtr, cast, cstr, list_len, list_nodes, node_tag, node_tag_name};
use crate::query::ast::rope::{FrameBoundKind, NodeKind, TreeBuilder};
use crate::query::ast::{AstError, FrameExclusion, FrameMode};

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

/// A SELECT's `WINDOW` clause: name → raw definition, so `OVER w` in its
/// select list emits the definition (PGC-280).
pub(super) type WindowDefs = HashMap<EcoString, *const pg::WindowDef>;

pub(super) unsafe fn window_defs_extract(
    window_clause: *const pg::List,
) -> Result<WindowDefs, AstError> {
    unsafe {
        let mut defs = HashMap::new();
        for node in list_nodes(window_clause) {
            if node_tag(node) != pg::NodeTag_T_WindowDef {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in WINDOW clause", node_tag_name(node_tag(node))),
                });
            }
            let win_def = cast::<pg::WindowDef>(node);
            let name = cstr((*win_def).name);
            if name.is_empty() {
                return Err(AstError::UnsupportedFeature {
                    feature: "unnamed WINDOW clause entry".to_owned(),
                });
            }
            // The owned converter validates every definition up front.
            window_spec_check(win_def)?;
            defs.insert(EcoString::from(name), win_def);
        }
        Ok(defs)
    }
}

/// A function's `OVER` clause: a bare `OVER w` emits the named definition
/// (only in the select list that holds the `WINDOW` clause, as the owned
/// converter resolved it); an inline `OVER (...)` is emitted directly.
pub(super) unsafe fn window_emit(
    builder: &mut TreeBuilder<'_>,
    win_def: *const pg::WindowDef,
    windows: Option<&WindowDefs>,
) -> Result<(), AstError> {
    unsafe {
        let name = cstr((*win_def).name);
        if name.is_empty() {
            return window_spec_emit(builder, win_def);
        }
        let Some(windows) = windows else {
            return Err(AstError::UnsupportedFeature {
                feature: "window reference outside the SELECT list".to_owned(),
            });
        };
        let Some(definition) = windows.get(name) else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("reference to undefined window {name}"),
            });
        };
        window_spec_emit(builder, *definition)
    }
}

/// The checks the inline clauses of a `WindowDef` pass before emission.
unsafe fn window_spec_check(win_def: *const pg::WindowDef) -> Result<(), AstError> {
    unsafe {
        if !cstr((*win_def).refname).is_empty() {
            return Err(AstError::UnsupportedFeature {
                feature: "window definition inheriting another window".to_owned(),
            });
        }
        Ok(())
    }
}

/// The frame of a `WindowDef`, decoded: `None` for the SQL default frame.
struct Frame {
    mode: FrameMode,
    start: (FrameBoundKind, NodePtr),
    end: (FrameBoundKind, NodePtr),
    exclusion: FrameExclusion,
}

/// The inline PARTITION BY / ORDER BY / frame clauses of a `WindowDef`,
/// ignoring its `name`. `OVER (w ...)` frame inheritance (`refname`) is not
/// supported and forwards.
unsafe fn window_spec_emit(
    builder: &mut TreeBuilder<'_>,
    win_def: *const pg::WindowDef,
) -> Result<(), AstError> {
    unsafe {
        window_spec_check(win_def)?;
        let frame = frame_decode(win_def)?;
        let open = builder.open(NodeKind::Window {
            partition_count: list_len((*win_def).partitionClause),
            order_count: list_len((*win_def).orderClause),
            has_frame: frame.is_some(),
            ref_name: None,
        });
        for expr in list_nodes((*win_def).partitionClause) {
            scalar_emit(builder, expr, None)?;
        }
        order_by_emit(builder, (*win_def).orderClause, None)?;
        if let Some(frame) = &frame {
            frame_emit(builder, frame)?;
        }
        builder.close(open);
        Ok(())
    }
}

unsafe fn frame_emit(builder: &mut TreeBuilder<'_>, frame: &Frame) -> Result<(), AstError> {
    unsafe {
        let open = builder.open(NodeKind::Frame {
            mode: frame.mode,
            start: frame.start.0,
            end: frame.end.0,
            exclusion: frame.exclusion,
        });
        for (kind, offset) in [frame.start, frame.end] {
            if kind.has_offset() {
                scalar_emit(builder, offset, None)?;
            }
        }
        builder.close(open);
        Ok(())
    }
}

/// Decode the frame clause from a `WindowDef`. `None` for the SQL default
/// frame (`NONDEFAULT` bit clear). Dropping a non-default frame is a silent
/// wrong result (PGC-279), so every explicit frame must round-trip.
unsafe fn frame_decode(win_def: *const pg::WindowDef) -> Result<Option<Frame>, AstError> {
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
        let start = frame_bound_decode(opts, &START_BOUND, "start")?;
        let end = frame_bound_decode(opts, &END_BOUND, "end")?;
        let exclusion = if opts & FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
            FrameExclusion::CurrentRow
        } else if opts & FRAMEOPTION_EXCLUDE_GROUP != 0 {
            FrameExclusion::Group
        } else if opts & FRAMEOPTION_EXCLUDE_TIES != 0 {
            FrameExclusion::Ties
        } else {
            FrameExclusion::NoOthers
        };
        Ok(Some(Frame {
            mode,
            start: (start, (*win_def).startOffset as NodePtr),
            end: (end, (*win_def).endOffset as NodePtr),
            exclusion,
        }))
    }
}

/// The `frameOptions` bits of one frame bound; a bit of zero is a form
/// PostgreSQL disallows for that bound.
struct BoundFlags {
    unbounded_preceding: i32,
    current_row: i32,
    offset_preceding: i32,
    offset_following: i32,
    unbounded_following: i32,
}

// START_UNBOUNDED_FOLLOWING is disallowed by PostgreSQL as a start.
const START_BOUND: BoundFlags = BoundFlags {
    unbounded_preceding: FRAMEOPTION_START_UNBOUNDED_PRECEDING,
    current_row: FRAMEOPTION_START_CURRENT_ROW,
    offset_preceding: FRAMEOPTION_START_OFFSET_PRECEDING,
    offset_following: FRAMEOPTION_START_OFFSET_FOLLOWING,
    unbounded_following: 0,
};

// END_UNBOUNDED_PRECEDING is disallowed by PostgreSQL as an end.
const END_BOUND: BoundFlags = BoundFlags {
    unbounded_preceding: 0,
    current_row: FRAMEOPTION_END_CURRENT_ROW,
    offset_preceding: FRAMEOPTION_END_OFFSET_PRECEDING,
    offset_following: FRAMEOPTION_END_OFFSET_FOLLOWING,
    unbounded_following: FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
};

fn frame_bound_decode(
    opts: i32,
    flags: &BoundFlags,
    which: &str,
) -> Result<FrameBoundKind, AstError> {
    let set = |flag: i32| flag != 0 && opts & flag != 0;
    if set(flags.unbounded_preceding) {
        Ok(FrameBoundKind::UnboundedPreceding)
    } else if set(flags.unbounded_following) {
        Ok(FrameBoundKind::UnboundedFollowing)
    } else if set(flags.current_row) {
        Ok(FrameBoundKind::CurrentRow)
    } else if set(flags.offset_preceding) {
        Ok(FrameBoundKind::OffsetPreceding)
    } else if set(flags.offset_following) {
        Ok(FrameBoundKind::OffsetFollowing)
    } else {
        Err(AstError::UnsupportedFeature {
            feature: format!("window frame {which} bound (frameOptions={opts:#x})"),
        })
    }
}

/// The `OrderBy` nodes of a sort list: a query's ORDER BY, a window's, an
/// aggregate's.
pub(super) unsafe fn order_by_emit(
    builder: &mut TreeBuilder<'_>,
    order_clause: *const pg::List,
    windows: Option<&WindowDefs>,
) -> Result<(), AstError> {
    unsafe {
        for sort_node in list_nodes(order_clause) {
            if node_tag(sort_node) != pg::NodeTag_T_SortBy {
                return Err(AstError::UnsupportedFeature {
                    feature: format!("{} in ORDER BY", node_tag_name(node_tag(sort_node))),
                });
            }
            let sort_by = cast::<pg::SortBy>(sort_node);
            let expr_node = (*sort_by).node as NodePtr;
            if expr_node.is_null() {
                return Err(AstError::UnsupportedFeature {
                    feature: "ORDER BY without expression".to_owned(),
                });
            }
            let open = builder.open(NodeKind::OrderBy {
                direction: order_dir_map((*sort_by).sortby_dir)?,
                null_order: null_order_map((*sort_by).sortby_nulls)?,
            });
            scalar_emit(builder, expr_node, windows)?;
            builder.close(open);
        }
        Ok(())
    }
}
