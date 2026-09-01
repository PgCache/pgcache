//! The pure serve-time decision (PGC-392): given a snapshot of a
//! fingerprint's cache entry and the request, decide whether to serve from
//! cache, forward, coalesce on an in-flight population, or ask the writer to
//! admit. Dispatch applies the decision under its write guard by re-deciding
//! from the guarded state; pgcache-fit replays the same function offline.
//!
//! Memo and MV selection are serve *backends* for a [`ServeDecision::Hit`],
//! not part of this decision. Coalescing has no offline analogue (population
//! completes instantly without a time axis), so fit never observes `Loading`.

use crate::settings::CachePolicy;

pub use super::messages::AdmitAction;
use super::query::limit_is_sufficient;
use super::types::{CachedQueryState, CachedQueryView};

/// The decision-relevant slice of a cache entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntrySnapshot {
    pub state: CachedQueryState,
    /// Rows cached for this fingerprint (`None` = all rows).
    pub max_limit: Option<u64>,
}

impl EntrySnapshot {
    /// Population (or limit bump) finished: the entry serves `max_limit` rows.
    pub fn population_complete(self, max_limit: Option<u64>) -> Self {
        EntrySnapshot {
            state: CachedQueryState::Ready,
            max_limit,
        }
    }

    /// CDC invalidated the entry; the next request fast-readmits it.
    pub fn invalidate(self) -> Self {
        EntrySnapshot {
            state: CachedQueryState::Invalidated,
            ..self
        }
    }
}

impl From<&CachedQueryView> for EntrySnapshot {
    fn from(view: &CachedQueryView) -> Self {
        EntrySnapshot {
            state: view.state,
            max_limit: view.max_limit,
        }
    }
}

/// Request and configuration inputs to the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionInput {
    /// Rows the request needs (LIMIT + OFFSET; `None` = all rows).
    pub rows_needed: Option<u64>,
    pub admission_threshold: u32,
    pub cache_policy: CachePolicy,
    /// Memory-pressure throttle: no new registrations and no coalesce waits
    /// on populations that will be skipped.
    pub throttled: bool,
    /// Credit stamped on a Pending entry at insert and on each re-hit; a GC
    /// decay budget only, so offline replay passes 0.
    pub pending_credit: u32,
}

/// Why a request is forwarded to origin without touching cache state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardReason {
    /// Memory pressure: neither register nor wait on a skipped population.
    Throttled,
    /// New-registration rate limit exhausted.
    RegistrationRateLimited,
}

/// A state write the decision requires. `expected` is the state the decision
/// was made from (`None` = no entry); dispatch re-decides under the write
/// guard rather than trusting the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    pub expected: Option<CachedQueryState>,
    pub new: CachedQueryState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeDecision {
    /// Ready with a sufficient cached window: serve from cache.
    Hit,
    /// Ready but the cached window is too small: forward, and claim the
    /// Ready→Loading transition so exactly one dispatch requests the bump.
    LimitBump {
        transition: Transition,
    },
    /// Population in flight: wait for it and serve from cache when it lands.
    Coalesce,
    Forward(ForwardReason),
    /// Ask the writer for a subsumption check; `Admit` also registers and
    /// populates when not subsumed, `CheckOnly` leaves the entry Pending.
    Register {
        transition: Transition,
        action: AdmitAction,
    },
}

impl ServeDecision {
    /// The state write this decision needs, if any.
    pub fn transition(&self) -> Option<Transition> {
        match self {
            ServeDecision::LimitBump { transition }
            | ServeDecision::Register { transition, .. } => Some(*transition),
            ServeDecision::Hit | ServeDecision::Coalesce | ServeDecision::Forward(_) => None,
        }
    }
}

/// Decide how to serve a request against `entry`. `registration_budget` is
/// consulted only for a cold fingerprint that would register; the runtime
/// backs it with the new-registration token bucket (taking a token is the
/// act of registering), offline replay with `|| true`.
pub fn serve_decide(
    entry: Option<&EntrySnapshot>,
    input: &DecisionInput,
    registration_budget: impl FnOnce() -> bool,
) -> ServeDecision {
    let Some(entry) = entry else {
        if input.throttled {
            return ServeDecision::Forward(ForwardReason::Throttled);
        }
        if !registration_budget() {
            return ServeDecision::Forward(ForwardReason::RegistrationRateLimited);
        }
        let immediate_admit =
            input.cache_policy == CachePolicy::Fifo || input.admission_threshold <= 1;
        let (new, action) = if immediate_admit {
            (CachedQueryState::Loading, AdmitAction::Admit)
        } else {
            (
                CachedQueryState::Pending {
                    hit_count: 1,
                    credit: input.pending_credit,
                },
                AdmitAction::CheckOnly,
            )
        };
        return ServeDecision::Register {
            transition: Transition {
                expected: None,
                new,
            },
            action,
        };
    };

    let from = Some(entry.state);
    match entry.state {
        CachedQueryState::Ready if limit_is_sufficient(entry.max_limit, input.rows_needed) => {
            ServeDecision::Hit
        }
        CachedQueryState::Ready => ServeDecision::LimitBump {
            transition: Transition {
                expected: from,
                new: CachedQueryState::Loading,
            },
        },
        CachedQueryState::Loading if input.throttled => {
            ServeDecision::Forward(ForwardReason::Throttled)
        }
        CachedQueryState::Loading => ServeDecision::Coalesce,
        CachedQueryState::Pending { hit_count, .. } => {
            let hit_count = hit_count + 1;
            let (new, action) = if hit_count >= input.admission_threshold {
                (CachedQueryState::Loading, AdmitAction::Admit)
            } else {
                (
                    CachedQueryState::Pending {
                        hit_count,
                        credit: input.pending_credit,
                    },
                    AdmitAction::CheckOnly,
                )
            };
            ServeDecision::Register {
                transition: Transition {
                    expected: from,
                    new,
                },
                action,
            }
        }
        // Fast readmission: skip the admission gate.
        CachedQueryState::Invalidated => ServeDecision::Register {
            transition: Transition {
                expected: from,
                new: CachedQueryState::Loading,
            },
            action: AdmitAction::Admit,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> DecisionInput {
        DecisionInput {
            rows_needed: None,
            admission_threshold: 1,
            cache_policy: CachePolicy::Clock,
            throttled: false,
            pending_credit: 7,
        }
    }

    fn ready(max_limit: Option<u64>) -> EntrySnapshot {
        EntrySnapshot {
            state: CachedQueryState::Ready,
            max_limit,
        }
    }

    fn state(state: CachedQueryState) -> EntrySnapshot {
        EntrySnapshot {
            state,
            max_limit: None,
        }
    }

    fn decide(entry: Option<&EntrySnapshot>, input: &DecisionInput) -> ServeDecision {
        serve_decide(entry, input, || true)
    }

    #[test]
    fn test_ready_sufficient_hits() {
        assert_eq!(decide(Some(&ready(None)), &input()), ServeDecision::Hit);
        let limited = DecisionInput {
            rows_needed: Some(10),
            ..input()
        };
        assert_eq!(decide(Some(&ready(Some(10))), &limited), ServeDecision::Hit);
        assert_eq!(decide(Some(&ready(None)), &limited), ServeDecision::Hit);
    }

    #[test]
    fn test_ready_insufficient_bumps_to_loading() {
        let limited = DecisionInput {
            rows_needed: Some(20),
            ..input()
        };
        let decision = decide(Some(&ready(Some(10))), &limited);
        assert_eq!(
            decision,
            ServeDecision::LimitBump {
                transition: Transition {
                    expected: Some(CachedQueryState::Ready),
                    new: CachedQueryState::Loading,
                },
            }
        );
        // An unlimited request against a limited window also bumps.
        assert!(matches!(
            decide(Some(&ready(Some(10))), &input()),
            ServeDecision::LimitBump { .. }
        ));
    }

    #[test]
    fn test_loading_coalesces_unless_throttled() {
        let loading = state(CachedQueryState::Loading);
        assert_eq!(decide(Some(&loading), &input()), ServeDecision::Coalesce);
        let throttled = DecisionInput {
            throttled: true,
            ..input()
        };
        assert_eq!(
            decide(Some(&loading), &throttled),
            ServeDecision::Forward(ForwardReason::Throttled)
        );
    }

    #[test]
    fn test_cold_admits_immediately_at_threshold_one() {
        assert_eq!(
            decide(None, &input()),
            ServeDecision::Register {
                transition: Transition {
                    expected: None,
                    new: CachedQueryState::Loading,
                },
                action: AdmitAction::Admit,
            }
        );
    }

    #[test]
    fn test_cold_fifo_admits_regardless_of_threshold() {
        let fifo = DecisionInput {
            admission_threshold: 5,
            cache_policy: CachePolicy::Fifo,
            ..input()
        };
        assert!(matches!(
            decide(None, &fifo),
            ServeDecision::Register {
                action: AdmitAction::Admit,
                ..
            }
        ));
    }

    #[test]
    fn test_cold_above_threshold_enters_pending_with_credit() {
        let gated = DecisionInput {
            admission_threshold: 3,
            ..input()
        };
        assert_eq!(
            decide(None, &gated),
            ServeDecision::Register {
                transition: Transition {
                    expected: None,
                    new: CachedQueryState::Pending {
                        hit_count: 1,
                        credit: 7,
                    },
                },
                action: AdmitAction::CheckOnly,
            }
        );
    }

    #[test]
    fn test_pending_counts_up_then_admits() {
        let gated = DecisionInput {
            admission_threshold: 3,
            ..input()
        };
        let pending = state(CachedQueryState::Pending {
            hit_count: 1,
            credit: 0,
        });
        assert_eq!(
            decide(Some(&pending), &gated),
            ServeDecision::Register {
                transition: Transition {
                    expected: Some(pending.state),
                    new: CachedQueryState::Pending {
                        hit_count: 2,
                        credit: 7,
                    },
                },
                action: AdmitAction::CheckOnly,
            }
        );
        let at_threshold = state(CachedQueryState::Pending {
            hit_count: 2,
            credit: 7,
        });
        assert_eq!(
            decide(Some(&at_threshold), &gated),
            ServeDecision::Register {
                transition: Transition {
                    expected: Some(at_threshold.state),
                    new: CachedQueryState::Loading,
                },
                action: AdmitAction::Admit,
            }
        );
    }

    #[test]
    fn test_invalidated_readmits_without_gate() {
        let gated = DecisionInput {
            admission_threshold: 3,
            ..input()
        };
        assert_eq!(
            decide(Some(&state(CachedQueryState::Invalidated)), &gated),
            ServeDecision::Register {
                transition: Transition {
                    expected: Some(CachedQueryState::Invalidated),
                    new: CachedQueryState::Loading,
                },
                action: AdmitAction::Admit,
            }
        );
    }

    #[test]
    fn test_cold_pressure_forwards_without_consuming_budget() {
        let throttled = DecisionInput {
            throttled: true,
            ..input()
        };
        let mut budget_consulted = false;
        let decision = serve_decide(None, &throttled, || {
            budget_consulted = true;
            true
        });
        assert_eq!(decision, ServeDecision::Forward(ForwardReason::Throttled));
        assert!(!budget_consulted);
        assert_eq!(
            serve_decide(None, &input(), || false),
            ServeDecision::Forward(ForwardReason::RegistrationRateLimited)
        );
    }

    #[test]
    fn test_budget_not_consulted_for_existing_entries() {
        for entry in [
            ready(None),
            state(CachedQueryState::Loading),
            state(CachedQueryState::Invalidated),
            state(CachedQueryState::Pending {
                hit_count: 1,
                credit: 0,
            }),
        ] {
            let mut consulted = false;
            let _ = serve_decide(Some(&entry), &input(), || {
                consulted = true;
                false
            });
            assert!(!consulted, "{:?}", entry.state);
        }
    }

    #[test]
    fn test_lifecycle_transitions() {
        let loading = state(CachedQueryState::Loading);
        let ready = loading.population_complete(Some(5));
        assert_eq!(ready.state, CachedQueryState::Ready);
        assert_eq!(ready.max_limit, Some(5));
        let invalidated = ready.invalidate();
        assert_eq!(invalidated.state, CachedQueryState::Invalidated);
        assert_eq!(invalidated.max_limit, Some(5));
    }
}
