//! A multi-producer, single-consumer queue for sending messages and signals between actors.
//!
//! An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

use std::{
    any::Any,
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use dyn_clone::DynClone;
use futures::{FutureExt, future::BoxFuture};
use tokio::{
    sync::mpsc::{self, error::TryRecvError},
    time,
};

use crate::{
    Actor,
    actor::{ActorId, ActorRef, ActorTerminalOutcome},
    error::{ActorStopReason, SendError},
    links::{BoxMailboxReceiver, Link},
    message::BoxMessage,
    reply::BoxReplySender,
};

/// Creates a bounded mailbox for communicating between actors with backpressure.
///
/// _See tokio's [`mpsc::channel`] docs for more info._
///
/// [`mpsc::channel`]: tokio::sync::mpsc::channel
pub fn bounded<A: Actor>(buffer: usize) -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::channel(buffer);
    #[cfg(feature = "hotpath")]
    let (tx, rx) = hotpath::channel!((tx, rx), label = A::name());
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let admission_open = Arc::new(AtomicBool::new(true));
    let message_generation = Arc::new(AtomicU64::new(0));
    (
        MailboxSender {
            inner: MailboxSenderInner::Bounded {
                messages: tx,
                control: control_tx,
            },
            admission_open: admission_open.clone(),
            message_generation: message_generation.clone(),
            #[cfg(feature = "metrics")]
            messages_sent: metrics::counter!("kameo_messages_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_sent: metrics::counter!("kameo_lifecycle_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_sent: metrics::counter!("kameo_link_died_sent", "actor_name" => A::name()),
        },
        MailboxReceiver {
            inner: MailboxReceiverInner::Bounded {
                messages: rx,
                control: control_rx,
            },
            message_generation,
            #[cfg(feature = "metrics")]
            messages_received: metrics::counter!("kameo_messages_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_received: metrics::counter!("kameo_lifecycle_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_received: metrics::counter!("kameo_link_died_received", "actor_name" => A::name()),
        },
    )
}

/// Creates an unbounded mailbox for communicating between actors without backpressure.
///
/// See tokio's [`mpsc::unbounded_channel`] docs for more info.
///
/// [`mpsc::unbounded_channel`]: tokio::sync::mpsc::unbounded_channel
pub fn unbounded<A: Actor>() -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::unbounded_channel();
    #[cfg(feature = "hotpath")]
    let (tx, rx) = hotpath::channel!((tx, rx), label = A::name());
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let admission_open = Arc::new(AtomicBool::new(true));
    let message_generation = Arc::new(AtomicU64::new(0));
    (
        MailboxSender {
            inner: MailboxSenderInner::Unbounded {
                messages: tx,
                control: control_tx,
            },
            admission_open: admission_open.clone(),
            message_generation: message_generation.clone(),
            #[cfg(feature = "metrics")]
            messages_sent: metrics::counter!("kameo_messages_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_sent: metrics::counter!("kameo_lifecycle_sent", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_sent: metrics::counter!("kameo_link_died_sent", "actor_name" => A::name()),
        },
        MailboxReceiver {
            inner: MailboxReceiverInner::Unbounded {
                messages: rx,
                control: control_rx,
            },
            message_generation,
            #[cfg(feature = "metrics")]
            messages_received: metrics::counter!("kameo_messages_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            lifecycle_signals_received: metrics::counter!("kameo_lifecycle_received", "actor_name" => A::name()),
            #[cfg(feature = "metrics")]
            link_died_signals_received: metrics::counter!("kameo_link_died_received", "actor_name" => A::name()),
        },
    )
}

/// Sends messages and signals to the associated `MailboxReceiver`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub struct MailboxSender<A: Actor> {
    inner: MailboxSenderInner<A>,
    admission_open: Arc<AtomicBool>,
    message_generation: Arc<AtomicU64>,
    #[cfg(feature = "metrics")]
    messages_sent: metrics::Counter,
    #[cfg(feature = "metrics")]
    lifecycle_signals_sent: metrics::Counter,
    #[cfg(feature = "metrics")]
    link_died_signals_sent: metrics::Counter,
}

enum MailboxSenderInner<A: Actor> {
    /// Bounded mailbox sender.
    Bounded {
        messages: mpsc::Sender<QueuedMessage<A>>,
        control: mpsc::UnboundedSender<Signal<A>>,
    },
    /// Unbounded mailbox sender.
    Unbounded {
        messages: mpsc::UnboundedSender<QueuedMessage<A>>,
        control: mpsc::UnboundedSender<Signal<A>>,
    },
}

struct QueuedMessage<A: Actor> {
    generation: u64,
    signal: Signal<A>,
}

#[cfg(feature = "metrics")]
enum SignalKind {
    Message,
    Lifecycle,
    LinkDied,
}

#[cfg(feature = "metrics")]
impl SignalKind {
    #[inline]
    fn apply_metric<A: Actor>(self, tx: &MailboxSender<A>) {
        match self {
            SignalKind::Message => tx.messages_sent.increment(1),
            SignalKind::Lifecycle => tx.lifecycle_signals_sent.increment(1),
            SignalKind::LinkDied => tx.link_died_signals_sent.increment(1),
        }
    }
}

#[cfg(feature = "metrics")]
impl<A: Actor> From<&Signal<A>> for SignalKind {
    #[inline]
    fn from(signal: &Signal<A>) -> Self {
        match signal {
            Signal::Message { .. } => SignalKind::Message,
            Signal::StartupFinished | Signal::Stop | Signal::SupervisorRestart => {
                SignalKind::Lifecycle
            }
            Signal::LinkDied { .. } => SignalKind::LinkDied,
        }
    }
}

impl<A: Actor> MailboxSender<A> {
    pub(crate) fn open_message_admission(&self) {
        self.admission_open.store(true, Ordering::Release);
    }

    pub(crate) fn is_accepting_messages(&self) -> bool {
        self.admission_open.load(Ordering::Acquire)
    }

    fn is_message_signal(signal: &Signal<A>) -> bool {
        matches!(signal, Signal::Message { .. })
    }

    fn current_message_generation(&self) -> u64 {
        self.message_generation.load(Ordering::Acquire)
    }

    fn accepts_message_generation(&self, generation: u64) -> bool {
        self.is_accepting_messages() && self.current_message_generation() == generation
    }

    fn queued_message(&self, generation: u64, signal: Signal<A>) -> QueuedMessage<A> {
        QueuedMessage { generation, signal }
    }

    /// Sends a value, waiting until there is capacity.
    ///
    /// See tokio's [`mpsc::Sender::send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::send`]: tokio::sync::mpsc::Sender::send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    pub async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendError(signal));
                    }
                    let permit = match messages.reserve().await {
                        Ok(permit) => permit,
                        Err(_) => return Err(mpsc::error::SendError(signal)),
                    };
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendError(signal));
                    }
                    permit.send(self.queued_message(generation, signal));
                    Ok(())
                } else {
                    control
                        .send(signal)
                        .map_err(|err| mpsc::error::SendError(err.0))
                }
            }
            MailboxSenderInner::Unbounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendError(signal));
                    }
                    messages
                        .send(self.queued_message(generation, signal))
                        .map_err(|err| mpsc::error::SendError(err.0.signal))
                } else {
                    control.send(signal)
                }
            }
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Attempts to immediately send a message on this `Sender`.
    /// Unbounded mailboxes will always have capacity.
    ///
    /// See tokio's [`mpsc::Sender::try_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::try_send`]: tokio::sync::mpsc::Sender::try_send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, signal: Signal<A>) -> Result<(), mpsc::error::TrySendError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::TrySendError::Closed(signal));
                    }
                    messages
                        .try_send(self.queued_message(generation, signal))
                        .map_err(|err| match err {
                            mpsc::error::TrySendError::Full(queued) => {
                                mpsc::error::TrySendError::Full(queued.signal)
                            }
                            mpsc::error::TrySendError::Closed(queued) => {
                                mpsc::error::TrySendError::Closed(queued.signal)
                            }
                        })
                } else {
                    control
                        .send(signal)
                        .map_err(|err| mpsc::error::TrySendError::Closed(err.0))
                }
            }
            MailboxSenderInner::Unbounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::TrySendError::Closed(signal));
                    }
                    messages
                        .send(self.queued_message(generation, signal))
                        .map_err(|err| mpsc::error::TrySendError::Closed(err.0.signal))
                } else {
                    control
                        .send(signal)
                        .map_err(|err| mpsc::error::TrySendError::Closed(err.0))
                }
            }
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Sends a value, waiting until there is capacity, but only for a limited time.
    /// Unbounded mailboxes will never need to wait for capacity.
    ///
    /// See tokio's [`mpsc::Sender::try_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::try_send`]: tokio::sync::mpsc::Sender::try_send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    pub async fn send_timeout(
        &self,
        signal: Signal<A>,
        timeout: Duration,
    ) -> Result<(), mpsc::error::SendTimeoutError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendTimeoutError::Closed(signal));
                    }
                    match time::timeout(timeout, messages.reserve()).await {
                        Err(_) => Err(mpsc::error::SendTimeoutError::Timeout(signal)),
                        Ok(Err(_)) => Err(mpsc::error::SendTimeoutError::Closed(signal)),
                        Ok(Ok(permit)) => {
                            if !self.accepts_message_generation(generation) {
                                Err(mpsc::error::SendTimeoutError::Closed(signal))
                            } else {
                                permit.send(self.queued_message(generation, signal));
                                Ok(())
                            }
                        }
                    }
                } else {
                    control
                        .send(signal)
                        .map_err(|err| mpsc::error::SendTimeoutError::Closed(err.0))
                }
            }
            MailboxSenderInner::Unbounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendTimeoutError::Closed(signal));
                    }
                    messages
                        .send(self.queued_message(generation, signal))
                        .map_err(|err| mpsc::error::SendTimeoutError::Closed(err.0.signal))
                } else {
                    control
                        .send(signal)
                        .map_err(|err| mpsc::error::SendTimeoutError::Closed(err.0))
                }
            }
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Blocking send to call outside of asynchronous contexts.
    /// Unbounded mailboxes will never block due to unbounded capacity.
    ///
    /// See tokio's [`mpsc::Sender::blocking_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::blocking_send`]: tokio::sync::mpsc::Sender::blocking_send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    #[allow(clippy::result_large_err)]
    pub fn blocking_send(
        &self,
        signal: Signal<A>,
    ) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        #[cfg(feature = "metrics")]
        let signal_kind = SignalKind::from(&signal);

        let res = match &self.inner {
            MailboxSenderInner::Bounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendError(signal));
                    }
                    messages
                        .blocking_send(self.queued_message(generation, signal))
                        .map_err(|err| mpsc::error::SendError(err.0.signal))
                } else {
                    control
                        .send(signal)
                        .map_err(|err| mpsc::error::SendError(err.0))
                }
            }
            MailboxSenderInner::Unbounded { messages, control } => {
                if Self::is_message_signal(&signal) {
                    let generation = self.current_message_generation();
                    if !self.accepts_message_generation(generation) {
                        return Err(mpsc::error::SendError(signal));
                    }
                    messages
                        .send(self.queued_message(generation, signal))
                        .map_err(|err| mpsc::error::SendError(err.0.signal))
                } else {
                    control.send(signal)
                }
            }
        };

        #[cfg(feature = "metrics")]
        if res.is_ok() {
            signal_kind.apply_metric(self);
        }

        res
    }

    /// Completes when the ordinary message lane receiver has dropped.
    ///
    /// Lifecycle/control signals use a separate control lane. This method reports the
    /// user-message lane because public mailbox senders are primarily an ordinary message
    /// surface.
    ///
    /// See tokio's [`mpsc::Sender::closed`] and [`mpsc::UnboundedSender::closed`] docs for more info.
    ///
    /// [`mpsc::Sender::closed`]: tokio::sync::mpsc::Sender::closed
    /// [`mpsc::UnboundedSender::closed`]: tokio::sync::mpsc::UnboundedSender::closed
    pub async fn closed(&self) {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => messages.closed().await,
            MailboxSenderInner::Unbounded { messages, .. } => messages.closed().await,
        }
    }

    /// Checks if the ordinary message lane has been closed. This happens when the
    /// [`MailboxReceiver`] is dropped, or when the [`MailboxReceiver::close`] method is
    /// called.
    ///
    /// Lifecycle/control signals use a separate control lane. This method reports the
    /// user-message lane because public mailbox senders are primarily an ordinary message
    /// surface.
    ///
    /// See tokio's [`mpsc::Sender::is_closed`] and [`mpsc::UnboundedSender::is_closed`] docs for more info.
    ///
    /// [`mpsc::Sender::is_closed`]: tokio::sync::mpsc::Sender::is_closed
    /// [`mpsc::UnboundedSender::is_closed`]: tokio::sync::mpsc::UnboundedSender::is_closed
    pub fn is_closed(&self) -> bool {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => messages.is_closed(),
            MailboxSenderInner::Unbounded { messages, .. } => messages.is_closed(),
        }
    }

    /// Returns `true` if senders belong to the same channel.
    ///
    /// See tokio's [`mpsc::Sender::same_channel`] and [`mpsc::UnboundedSender::same_channel`] docs for more info.
    ///
    /// [`mpsc::Sender::same_channel`]: tokio::sync::mpsc::Sender::same_channel
    /// [`mpsc::UnboundedSender::same_channel`]: tokio::sync::mpsc::UnboundedSender::same_channel
    pub fn same_channel(&self, other: &MailboxSender<A>) -> bool {
        match (&self.inner, &other.inner) {
            (
                MailboxSenderInner::Bounded { messages: a, .. },
                MailboxSenderInner::Bounded { messages: b, .. },
            ) => a.same_channel(b),
            (MailboxSenderInner::Bounded { .. }, MailboxSenderInner::Unbounded { .. }) => false,
            (MailboxSenderInner::Unbounded { .. }, MailboxSenderInner::Bounded { .. }) => false,
            (
                MailboxSenderInner::Unbounded { messages: a, .. },
                MailboxSenderInner::Unbounded { messages: b, .. },
            ) => a.same_channel(b),
        }
    }

    /// Returns the current capacity of the ordinary message lane, if bounded.
    /// Unbounded ordinary message lanes return `None`.
    ///
    /// Lifecycle/control signals use a separate unbounded control lane, so this value does
    /// not describe control-signal capacity.
    ///
    /// See tokio's [`mpsc::Sender::capacity`] docs for more info.
    ///
    /// [`mpsc::Sender::capacity`]: tokio::sync::mpsc::Sender::capacity
    pub fn capacity(&self) -> Option<usize> {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => Some(messages.capacity()),
            MailboxSenderInner::Unbounded { .. } => None,
        }
    }

    /// Converts the `MailboxSender` to a [`WeakMailboxSender`] that does not count
    /// towards RAII semantics, i.e. if all `Sender` instances of the
    /// channel were dropped and only `WeakMailboxSender` instances remain,
    /// the channel is closed.
    ///
    /// See tokio's [`mpsc::Sender::downgrade`] and [`mpsc::UnboundedSender::downgrade`] docs for more info.
    ///
    /// [`mpsc::Sender::downgrade`]: tokio::sync::mpsc::Sender::downgrade
    /// [`mpsc::UnboundedSender::downgrade`]: tokio::sync::mpsc::UnboundedSender::downgrade
    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, control } => WeakMailboxSender {
                inner: WeakMailboxSenderInner::Bounded {
                    messages: messages.downgrade(),
                    control: control.downgrade(),
                },
                admission_open: self.admission_open.clone(),
                message_generation: self.message_generation.clone(),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
            MailboxSenderInner::Unbounded { messages, control } => WeakMailboxSender {
                inner: WeakMailboxSenderInner::Unbounded {
                    messages: messages.downgrade(),
                    control: control.downgrade(),
                },
                admission_open: self.admission_open.clone(),
                message_generation: self.message_generation.clone(),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
        }
    }

    /// Returns the number of strong handles to the ordinary message lane.
    ///
    /// Lifecycle/control signals use a separate control lane. This method reports the
    /// user-message lane because public mailbox senders are primarily an ordinary message
    /// surface.
    ///
    /// See tokio's [`mpsc::Sender::strong_count`] and [`mpsc::UnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::Sender::strong_count`]: tokio::sync::mpsc::Sender::strong_count
    /// [`mpsc::UnboundedSender::strong_count`]: tokio::sync::mpsc::UnboundedSender::strong_count
    pub fn strong_count(&self) -> usize {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => messages.strong_count(),
            MailboxSenderInner::Unbounded { messages, .. } => messages.strong_count(),
        }
    }

    /// Returns the number of weak handles to the ordinary message lane.
    ///
    /// Lifecycle/control signals use a separate control lane. This method reports the
    /// user-message lane because public mailbox senders are primarily an ordinary message
    /// surface.
    ///
    /// See tokio's [`mpsc::Sender::weak_count`] and [`mpsc::UnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::Sender::weak_count`]: tokio::sync::mpsc::Sender::weak_count
    /// [`mpsc::UnboundedSender::weak_count`]: tokio::sync::mpsc::UnboundedSender::weak_count
    pub fn weak_count(&self) -> usize {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => messages.weak_count(),
            MailboxSenderInner::Unbounded { messages, .. } => messages.weak_count(),
        }
    }
}

impl<A: Actor> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, control } => MailboxSender {
                inner: MailboxSenderInner::Bounded {
                    messages: messages.clone(),
                    control: control.clone(),
                },
                admission_open: self.admission_open.clone(),
                message_generation: self.message_generation.clone(),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
            MailboxSenderInner::Unbounded { messages, control } => MailboxSender {
                inner: MailboxSenderInner::Unbounded {
                    messages: messages.clone(),
                    control: control.clone(),
                },
                admission_open: self.admission_open.clone(),
                message_generation: self.message_generation.clone(),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => {
                f.debug_tuple("Bounded").field(messages).finish()
            }
            MailboxSenderInner::Unbounded { messages, .. } => {
                f.debug_tuple("Unbounded").field(messages).finish()
            }
        }
    }
}

/// A mailbox sender that does not prevent the channel from being closed.
///
/// See tokio's [`mpsc::WeakSender`] and [`mpsc::WeakUnboundedSender`] docs for more info.
///
/// [`mpsc::WeakSender`]: tokio::sync::mpsc::WeakSender
/// [`mpsc::WeakUnboundedSender`]: tokio::sync::mpsc::WeakUnboundedSender
pub struct WeakMailboxSender<A: Actor> {
    inner: WeakMailboxSenderInner<A>,
    admission_open: Arc<AtomicBool>,
    message_generation: Arc<AtomicU64>,
    #[cfg(feature = "metrics")]
    messages_sent: metrics::Counter,
    #[cfg(feature = "metrics")]
    lifecycle_signals_sent: metrics::Counter,
    #[cfg(feature = "metrics")]
    link_died_signals_sent: metrics::Counter,
}

enum WeakMailboxSenderInner<A: Actor> {
    /// Bounded weak mailbox sender.
    Bounded {
        messages: mpsc::WeakSender<QueuedMessage<A>>,
        control: mpsc::WeakUnboundedSender<Signal<A>>,
    },
    /// Unbounded weak mailbox sender.
    Unbounded {
        messages: mpsc::WeakUnboundedSender<QueuedMessage<A>>,
        control: mpsc::WeakUnboundedSender<Signal<A>>,
    },
}

impl<A: Actor> WeakMailboxSender<A> {
    pub(crate) fn stop_message_admission(&self) {
        self.admission_open.store(false, Ordering::Release);
        self.message_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn is_accepting_messages(&self) -> bool {
        self.admission_open.load(Ordering::Acquire)
    }

    /// Tries to convert a `WeakMailboxSender` into a [`MailboxSender`]. This will return `Some`
    /// if there are other `MailboxSender` instances alive and the channel wasn't
    /// previously dropped, otherwise `None` is returned.
    ///
    /// See tokio's [`mpsc::WeakSender::upgrade`] and [`mpsc::WeakUnboundedSender::upgrade`] docs for more info.
    ///
    /// [`mpsc::WeakSender::upgrade`]: tokio::sync::mpsc::WeakSender::upgrade
    /// [`mpsc::WeakUnboundedSender::upgrade`]: tokio::sync::mpsc::WeakUnboundedSender::upgrade
    pub fn upgrade(&self) -> Option<MailboxSender<A>> {
        match &self.inner {
            WeakMailboxSenderInner::Bounded { messages, control } => {
                let messages = messages.upgrade()?;
                let control = control.upgrade()?;
                Some(MailboxSender {
                    inner: MailboxSenderInner::Bounded { messages, control },
                    admission_open: self.admission_open.clone(),
                    message_generation: self.message_generation.clone(),
                    #[cfg(feature = "metrics")]
                    messages_sent: self.messages_sent.clone(),
                    #[cfg(feature = "metrics")]
                    lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                    #[cfg(feature = "metrics")]
                    link_died_signals_sent: self.link_died_signals_sent.clone(),
                })
            }
            WeakMailboxSenderInner::Unbounded { messages, control } => {
                let messages = messages.upgrade()?;
                let control = control.upgrade()?;
                Some(MailboxSender {
                    inner: MailboxSenderInner::Unbounded { messages, control },
                    admission_open: self.admission_open.clone(),
                    message_generation: self.message_generation.clone(),
                    #[cfg(feature = "metrics")]
                    messages_sent: self.messages_sent.clone(),
                    #[cfg(feature = "metrics")]
                    lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                    #[cfg(feature = "metrics")]
                    link_died_signals_sent: self.link_died_signals_sent.clone(),
                })
            }
        }
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::strong_count`] and [`mpsc::WeakUnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::strong_count`]: tokio::sync::mpsc::WeakSender::strong_count
    /// [`mpsc::WeakUnboundedSender::strong_count`]: tokio::sync::mpsc::WeakUnboundedSender::strong_count
    pub fn strong_count(&self) -> usize {
        match &self.inner {
            WeakMailboxSenderInner::Bounded { messages, .. } => messages.strong_count(),
            WeakMailboxSenderInner::Unbounded { messages, .. } => messages.strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::weak_count`] and [`mpsc::WeakUnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::weak_count`]: tokio::sync::mpsc::WeakSender::weak_count
    /// [`mpsc::WeakUnboundedSender::weak_count`]: tokio::sync::mpsc::WeakUnboundedSender::weak_count
    pub fn weak_count(&self) -> usize {
        match &self.inner {
            WeakMailboxSenderInner::Bounded { messages, .. } => messages.weak_count(),
            WeakMailboxSenderInner::Unbounded { messages, .. } => messages.weak_count(),
        }
    }
}

impl<A: Actor> Clone for WeakMailboxSender<A> {
    fn clone(&self) -> Self {
        match &self.inner {
            WeakMailboxSenderInner::Bounded { messages, control } => WeakMailboxSender {
                inner: WeakMailboxSenderInner::Bounded {
                    messages: messages.clone(),
                    control: control.clone(),
                },
                admission_open: self.admission_open.clone(),
                message_generation: self.message_generation.clone(),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
            WeakMailboxSenderInner::Unbounded { messages, control } => WeakMailboxSender {
                inner: WeakMailboxSenderInner::Unbounded {
                    messages: messages.clone(),
                    control: control.clone(),
                },
                admission_open: self.admission_open.clone(),
                message_generation: self.message_generation.clone(),
                #[cfg(feature = "metrics")]
                messages_sent: self.messages_sent.clone(),
                #[cfg(feature = "metrics")]
                lifecycle_signals_sent: self.lifecycle_signals_sent.clone(),
                #[cfg(feature = "metrics")]
                link_died_signals_sent: self.link_died_signals_sent.clone(),
            },
        }
    }
}

impl<A: Actor> fmt::Debug for WeakMailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            WeakMailboxSenderInner::Bounded { messages, .. } => {
                f.debug_tuple("Bounded").field(messages).finish()
            }
            WeakMailboxSenderInner::Unbounded { messages, .. } => {
                f.debug_tuple("Unbounded").field(messages).finish()
            }
        }
    }
}

/// Receives values from the associated `MailboxSender`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub struct MailboxReceiver<A: Actor> {
    inner: MailboxReceiverInner<A>,
    message_generation: Arc<AtomicU64>,
    #[cfg(feature = "metrics")]
    messages_received: metrics::Counter,
    #[cfg(feature = "metrics")]
    lifecycle_signals_received: metrics::Counter,
    #[cfg(feature = "metrics")]
    link_died_signals_received: metrics::Counter,
}

enum MailboxReceiverInner<A: Actor> {
    /// Bounded mailbox receiver.
    Bounded {
        messages: mpsc::Receiver<QueuedMessage<A>>,
        control: mpsc::UnboundedReceiver<Signal<A>>,
    },
    /// Unbounded mailbox receiver.
    Unbounded {
        messages: mpsc::UnboundedReceiver<QueuedMessage<A>>,
        control: mpsc::UnboundedReceiver<Signal<A>>,
    },
}

impl<A: Actor> MailboxReceiver<A> {
    fn record_received_signal(&self, signal: &Signal<A>) {
        #[cfg(not(feature = "metrics"))]
        let _ = signal;

        #[cfg(feature = "metrics")]
        match signal {
            Signal::Message { .. } => self.messages_received.increment(1),
            Signal::StartupFinished | Signal::Stop | Signal::SupervisorRestart => {
                self.lifecycle_signals_received.increment(1)
            }
            Signal::LinkDied { .. } => self.link_died_signals_received.increment(1),
        }
    }

    fn accept_queued_message(
        message_generation: &AtomicU64,
        queued: QueuedMessage<A>,
    ) -> Option<Signal<A>> {
        if queued.generation == message_generation.load(Ordering::Acquire) {
            Some(queued.signal)
        } else {
            None
        }
    }

    /// Receives the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::recv`] and [`mpsc::UnboundedReceiver::recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv`]: tokio::sync::mpsc::Receiver::recv
    /// [`mpsc::UnboundedReceiver::recv`]: tokio::sync::mpsc::UnboundedReceiver::recv
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        let message_generation = self.message_generation.clone();
        loop {
            let signal = match &mut self.inner {
                MailboxReceiverInner::Bounded { messages, control } => {
                    tokio::select! {
                        biased;
                        signal = control.recv() => match signal {
                            Some(signal) => Some(signal),
                            None => messages.recv().await.and_then(|queued| {
                                Self::accept_queued_message(&message_generation, queued)
                            }),
                        },
                        queued = messages.recv() => queued.and_then(|queued| {
                            Self::accept_queued_message(&message_generation, queued)
                        }),
                    }
                }
                MailboxReceiverInner::Unbounded { messages, control } => {
                    tokio::select! {
                        biased;
                        signal = control.recv() => match signal {
                            Some(signal) => Some(signal),
                            None => messages.recv().await.and_then(|queued| {
                                Self::accept_queued_message(&message_generation, queued)
                            }),
                        },
                        queued = messages.recv() => queued.and_then(|queued| {
                            Self::accept_queued_message(&message_generation, queued)
                        }),
                    }
                }
            };

            if let Some(signal_reference) = &signal {
                self.record_received_signal(signal_reference);
                return signal;
            }

            if self.is_closed() && self.is_empty() {
                return None;
            }
        }
    }

    /// Receives the next values for this receiver and extends `buffer`.
    ///
    /// See tokio's [`mpsc::Receiver::recv_many`] and [`mpsc::UnboundedReceiver::recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv_many`]: tokio::sync::mpsc::Receiver::recv_many
    /// [`mpsc::UnboundedReceiver::recv_many`]: tokio::sync::mpsc::UnboundedReceiver::recv_many
    pub async fn recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }

        let Some(signal) = self.recv().await else {
            return 0;
        };
        buffer.push(signal);
        let mut count = 1;

        while count < limit {
            match self.try_recv() {
                Ok(signal) => {
                    buffer.push(signal);
                    count += 1;
                }
                Err(_) => break,
            }
        }

        count
    }

    /// Tries to receive the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::try_recv`] and [`mpsc::UnboundedReceiver::try_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::try_recv`]: tokio::sync::mpsc::Receiver::try_recv
    /// [`mpsc::UnboundedReceiver::try_recv`]: tokio::sync::mpsc::UnboundedReceiver::try_recv
    pub fn try_recv(&mut self) -> Result<Signal<A>, TryRecvError> {
        let message_generation = self.message_generation.clone();
        let res = loop {
            let res = match &mut self.inner {
                MailboxReceiverInner::Bounded { messages, control } => match control.try_recv() {
                    Ok(signal) => Ok(signal),
                    Err(TryRecvError::Disconnected) | Err(TryRecvError::Empty) => {
                        messages.try_recv().and_then(|queued| {
                            Self::accept_queued_message(&message_generation, queued)
                                .ok_or(TryRecvError::Empty)
                        })
                    }
                },
                MailboxReceiverInner::Unbounded { messages, control } => match control.try_recv() {
                    Ok(signal) => Ok(signal),
                    Err(TryRecvError::Disconnected) | Err(TryRecvError::Empty) => {
                        messages.try_recv().and_then(|queued| {
                            Self::accept_queued_message(&message_generation, queued)
                                .ok_or(TryRecvError::Empty)
                        })
                    }
                },
            };

            if !matches!(res, Err(TryRecvError::Empty)) || self.is_empty() {
                break res;
            }
        };

        if let Ok(signal) = &res {
            self.record_received_signal(signal);
        }

        res
    }

    /// Blocking receive to call outside of asynchronous contexts.
    ///
    /// This mirrors [`Self::recv`]: lifecycle/control signals are observed on the control
    /// lane and ordinary messages are observed on the message lane. Like Tokio's blocking
    /// channel receive methods, this must not be called from inside an asynchronous runtime.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv`] and [`mpsc::UnboundedReceiver::blocking_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv`]: tokio::sync::mpsc::Receiver::blocking_recv
    /// [`mpsc::UnboundedReceiver::blocking_recv`]: tokio::sync::mpsc::UnboundedReceiver::blocking_recv
    pub fn blocking_recv(&mut self) -> Option<Signal<A>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime can drive blocking mailbox receive");
        runtime.block_on(self.recv())
    }

    /// Variant of [`Self::recv_many`] for blocking contexts.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv_many`] and [`mpsc::UnboundedReceiver::blocking_recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv_many`]: tokio::sync::mpsc::Receiver::blocking_recv_many
    /// [`mpsc::UnboundedReceiver::blocking_recv_many`]: tokio::sync::mpsc::UnboundedReceiver::blocking_recv_many
    pub fn blocking_recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }

        let Some(signal) = self.blocking_recv() else {
            return 0;
        };
        buffer.push(signal);
        let mut count = 1;

        while count < limit {
            match self.try_recv() {
                Ok(signal) => {
                    buffer.push(signal);
                    count += 1;
                }
                Err(_) => break,
            }
        }

        count
    }

    /// Closes the receiving half of a channel, without dropping it.
    ///
    /// See tokio's [`mpsc::Receiver::close`] and [`mpsc::UnboundedReceiver::close`] docs for more info.
    ///
    /// [`mpsc::Receiver::close`]: tokio::sync::mpsc::Receiver::close
    /// [`mpsc::UnboundedReceiver::close`]: tokio::sync::mpsc::UnboundedReceiver::close
    pub fn close(&mut self) {
        match &mut self.inner {
            MailboxReceiverInner::Bounded { messages, control } => {
                messages.close();
                control.close();
            }
            MailboxReceiverInner::Unbounded { messages, control } => {
                messages.close();
                control.close();
            }
        }
    }

    /// Checks if a channel is closed.
    ///
    /// See tokio's [`mpsc::Receiver::is_closed`] and [`mpsc::UnboundedReceiver::is_closed`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_closed`]: tokio::sync::mpsc::Receiver::is_closed
    /// [`mpsc::UnboundedReceiver::is_closed`]: tokio::sync::mpsc::UnboundedReceiver::is_closed
    pub fn is_closed(&self) -> bool {
        match &self.inner {
            MailboxReceiverInner::Bounded { messages, control } => {
                messages.is_closed() && control.is_closed()
            }
            MailboxReceiverInner::Unbounded { messages, control } => {
                messages.is_closed() && control.is_closed()
            }
        }
    }

    /// Checks if a channel is empty.
    ///
    /// See tokio's [`mpsc::Receiver::is_empty`] and [`mpsc::UnboundedReceiver::is_empty`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_empty`]: tokio::sync::mpsc::Receiver::is_empty
    /// [`mpsc::UnboundedReceiver::is_empty`]: tokio::sync::mpsc::UnboundedReceiver::is_empty
    pub fn is_empty(&self) -> bool {
        match &self.inner {
            MailboxReceiverInner::Bounded { messages, control } => {
                messages.is_empty() && control.is_empty()
            }
            MailboxReceiverInner::Unbounded { messages, control } => {
                messages.is_empty() && control.is_empty()
            }
        }
    }

    /// Returns the number of messages in the channel.
    ///
    /// See tokio's [`mpsc::Receiver::len`] and [`mpsc::UnboundedReceiver::len`] docs for more info.
    ///
    /// [`mpsc::Receiver::len`]: tokio::sync::mpsc::Receiver::len
    /// [`mpsc::UnboundedReceiver::len`]: tokio::sync::mpsc::UnboundedReceiver::len
    pub fn len(&self) -> usize {
        match &self.inner {
            MailboxReceiverInner::Bounded { messages, control } => messages.len() + control.len(),
            MailboxReceiverInner::Unbounded { messages, control } => messages.len() + control.len(),
        }
    }

    /// Polls to receive the next message on this channel.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv`] and [`mpsc::UnboundedReceiver::poll_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv`]: tokio::sync::mpsc::Receiver::poll_recv
    /// [`mpsc::UnboundedReceiver::poll_recv`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Signal<A>>> {
        let message_generation = self.message_generation.clone();
        let poll = match &mut self.inner {
            MailboxReceiverInner::Bounded { messages, control } => match control.poll_recv(cx) {
                Poll::Ready(Some(signal)) => Poll::Ready(Some(signal)),
                Poll::Ready(None) | Poll::Pending => match messages.poll_recv(cx) {
                    Poll::Ready(Some(queued)) => {
                        match Self::accept_queued_message(&message_generation, queued) {
                            Some(signal) => Poll::Ready(Some(signal)),
                            None => {
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                        }
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                },
            },
            MailboxReceiverInner::Unbounded { messages, control } => match control.poll_recv(cx) {
                Poll::Ready(Some(signal)) => Poll::Ready(Some(signal)),
                Poll::Ready(None) | Poll::Pending => match messages.poll_recv(cx) {
                    Poll::Ready(Some(queued)) => {
                        match Self::accept_queued_message(&message_generation, queued) {
                            Some(signal) => Poll::Ready(Some(signal)),
                            None => {
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                        }
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                },
            },
        };

        if let Poll::Ready(Some(signal)) = &poll {
            self.record_received_signal(signal);
        }

        poll
    }

    /// Polls to receive multiple messages on this channel, extending the provided buffer.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv_many`] and [`mpsc::UnboundedReceiver::poll_recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv_many`]: tokio::sync::mpsc::Receiver::poll_recv_many
    /// [`mpsc::UnboundedReceiver::poll_recv_many`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv_many
    pub fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut Vec<Signal<A>>,
        limit: usize,
    ) -> Poll<usize> {
        if limit == 0 {
            return Poll::Ready(0);
        }

        match self.poll_recv(cx) {
            Poll::Ready(Some(signal)) => {
                buffer.push(signal);
                let mut count = 1;
                while count < limit {
                    match self.try_recv() {
                        Ok(signal) => {
                            buffer.push(signal);
                            count += 1;
                        }
                        Err(_) => break,
                    }
                }
                Poll::Ready(count)
            }
            Poll::Ready(None) => Poll::Ready(0),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_strong_count`] and [`mpsc::UnboundedReceiver::sender_strong_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_strong_count`]: tokio::sync::mpsc::Receiver::sender_strong_count
    /// [`mpsc::UnboundedReceiver::sender_strong_count`]: tokio::sync::mpsc::UnboundedReceiver::sender_strong_count
    pub fn sender_strong_count(&self) -> usize {
        match &self.inner {
            MailboxReceiverInner::Bounded { messages, .. } => messages.sender_strong_count(),
            MailboxReceiverInner::Unbounded { messages, .. } => messages.sender_strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_weak_count`] and [`mpsc::UnboundedReceiver::sender_weak_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_weak_count`]: tokio::sync::mpsc::Receiver::sender_weak_count
    /// [`mpsc::UnboundedReceiver::sender_weak_count`]: tokio::sync::mpsc::UnboundedReceiver::sender_weak_count
    pub fn sender_weak_count(&self) -> usize {
        match &self.inner {
            MailboxReceiverInner::Bounded { messages, .. } => messages.sender_weak_count(),
            MailboxReceiverInner::Unbounded { messages, .. } => messages.sender_weak_count(),
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            MailboxReceiverInner::Bounded { messages, .. } => {
                f.debug_tuple("Bounded").field(messages).finish()
            }
            MailboxReceiverInner::Unbounded { messages, .. } => {
                f.debug_tuple("Unbounded").field(messages).finish()
            }
        }
    }
}

/// A signal which can be sent to an actors mailbox.
#[allow(missing_debug_implementations)]
pub enum Signal<A: Actor> {
    /// The actor has finished starting up.
    StartupFinished,
    /// A message.
    Message {
        /// The boxed message.
        message: BoxMessage<A>,
        /// The actor ref, to keep the actor from stopping due to RAII semantics.
        actor_ref: ActorRef<A>,
        /// The reply sender.
        reply: Option<BoxReplySender>,
        /// If the message sent from within the actor's tokio task/thread
        sent_within_actor: bool,
        /// The message name.
        message_name: &'static str,
        /// The span that was active when the message was sent, for cross-actor span propagation.
        #[cfg(feature = "tracing")]
        caller_span: tracing::Span,
    },
    /// A linked actor has died.
    LinkDied {
        /// The dead actor's ID.
        id: ActorId,
        /// The reason the actor stopped.
        reason: ActorStopReason,
        /// The terminal outcome published after the actor completed shutdown.
        outcome: ActorTerminalOutcome,
        /// The mailbox receiver. `Some` when sent to a supervising parent, `None` for sibling links.
        mailbox_rx: Option<Box<dyn Any + Send>>,
        /// The dead actor's own peer links, passed along the supervised path so the supervisor can
        /// notify them when it decides not to restart. `None` on the unsupervised (sibling) path.
        dead_actor_sibblings: Option<HashMap<ActorId, Link>>,
    },
    /// Signals the actor to stop.
    Stop,
    /// Signals the actor to restart.
    SupervisorRestart,
}

impl<A: Actor> Signal<A> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message { message, .. } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send + Sync {
    fn signal_startup_finished(&self) -> Result<(), SendError>;
    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
        outcome: ActorTerminalOutcome,
        mailbox_rx: Option<BoxMailboxReceiver>,
        dead_actor_sibblings: Option<HashMap<ActorId, Link>>,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
    fn closed(&self) -> BoxFuture<'_, ()>;
}

impl<A> SignalMailbox for MailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        self.try_send(Signal::StartupFinished)
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => SendError::MailboxFull(()),
                mpsc::error::TrySendError::Closed(_) => SendError::ActorNotRunning(()),
            })
    }

    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
        outcome: ActorTerminalOutcome,
        mailbox_rx: Option<Box<dyn Any + Send>>,
        dead_actor_sibblings: Option<HashMap<ActorId, Link>>,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.send(Signal::LinkDied {
                id,
                reason,
                outcome,
                mailbox_rx,
                dead_actor_sibblings,
            })
            .await
            .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.send(Signal::Stop)
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        match &self.inner {
            MailboxSenderInner::Bounded { messages, .. } => messages.closed().boxed(),
            MailboxSenderInner::Unbounded { messages, .. } => messages.closed().boxed(),
        }
    }
}

impl<A> SignalMailbox for WeakMailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self.upgrade() {
            Some(tx) => tx.signal_startup_finished(),
            None => Err(SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
        outcome: ActorTerminalOutcome,
        mailbox_rx: Option<Box<dyn Any + Send>>,
        dead_actor_sibblings: Option<HashMap<ActorId, Link>>,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => {
                    tx.signal_link_died(id, reason, outcome, mailbox_rx, dead_actor_sibblings)
                        .await
                }
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        match &self.inner {
            WeakMailboxSenderInner::Bounded { messages, .. } => async move {
                if let Some(tx) = messages.upgrade() {
                    tx.closed().await;
                }
            }
            .boxed(),
            WeakMailboxSenderInner::Unbounded { messages, .. } => async move {
                if let Some(tx) = messages.upgrade() {
                    tx.closed().await;
                }
            }
            .boxed(),
        }
    }
}

dyn_clone::clone_trait_object!(SignalMailbox);
