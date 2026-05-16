use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::SetOnce;

use crate::error::ActorStopReason;

/// Why actor state is absent at terminal shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ActorStateAbsence {
    /// The actor state existed and `Drop` completed.
    Dropped,
    /// Startup failed before actor state was allocated.
    NeverAllocated,
    /// Actor state was explicitly ejected to a caller.
    Ejected,
}

/// Terminal reason published after the actor's shutdown sequence finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ActorTerminalReason {
    /// Startup failed before actor state was allocated.
    StartupFailed,
    /// The actor stopped normally.
    Stopped,
    /// The actor stopped for supervisor restart.
    SupervisorRestart,
    /// The actor was killed.
    Killed,
    /// The actor panicked or received a panic stop reason.
    Panicked,
    /// A linked actor died.
    LinkDied,
    /// The actor stopped because its cleanup hook failed.
    CleanupFailed,
    /// The actor stopped because a remote peer disconnected.
    #[cfg(feature = "remote")]
    PeerDisconnected,
}

impl ActorTerminalReason {
    pub(crate) fn from_stop_reason(reason: &ActorStopReason) -> Self {
        match reason {
            ActorStopReason::Normal => Self::Stopped,
            ActorStopReason::SupervisorRestart => Self::SupervisorRestart,
            ActorStopReason::Killed => Self::Killed,
            ActorStopReason::Panicked(_) => Self::Panicked,
            ActorStopReason::LinkDied { .. } => Self::LinkDied,
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => Self::PeerDisconnected,
        }
    }

    /// Returns whether the terminal reason represents a normal non-error stop.
    pub fn is_normal(self) -> bool {
        matches!(self, Self::Stopped | Self::SupervisorRestart)
    }
}

/// Terminal outcome observed after the framework has completed shutdown.
///
/// This is the public lifecycle contract. Intermediate teardown steps are an
/// implementation detail; callers wait for one terminal outcome and inspect the
/// state-absence and terminal-reason fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorTerminalOutcome {
    /// Why actor state is absent.
    pub state: ActorStateAbsence,
    /// Why the actor reached terminal shutdown.
    pub reason: ActorTerminalReason,
}

impl ActorTerminalOutcome {
    pub(crate) fn dropped(reason: ActorStopReason) -> Self {
        Self {
            state: ActorStateAbsence::Dropped,
            reason: ActorTerminalReason::from_stop_reason(&reason),
        }
    }

    pub(crate) fn cleanup_failed() -> Self {
        Self {
            state: ActorStateAbsence::Dropped,
            reason: ActorTerminalReason::CleanupFailed,
        }
    }

    pub(crate) fn startup_failed() -> Self {
        Self {
            state: ActorStateAbsence::NeverAllocated,
            reason: ActorTerminalReason::StartupFailed,
        }
    }

    #[cfg(feature = "remote")]
    pub(crate) fn peer_disconnected() -> Self {
        Self {
            state: ActorStateAbsence::Dropped,
            reason: ActorTerminalReason::PeerDisconnected,
        }
    }
}

/// Shared terminal lifecycle cell.
#[derive(Debug, Clone)]
pub(crate) struct ActorLifecycle {
    outcome: Arc<SetOnce<ActorTerminalOutcome>>,
}

impl ActorLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            outcome: Arc::new(SetOnce::new()),
        }
    }

    pub(crate) fn set_terminal_outcome(&self, outcome: ActorTerminalOutcome) {
        self.outcome
            .set(outcome)
            .expect("nothing else should set the terminal outcome");
    }

    pub(crate) async fn wait_for_shutdown(&self) -> ActorTerminalOutcome {
        *self.outcome.wait().await
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.outcome.initialized()
    }
}
