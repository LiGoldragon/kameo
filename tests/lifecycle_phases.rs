use std::{
    error, fmt,
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kameo::{
    actor::{
        Actor, ActorId, ActorRef, ActorStateAbsence, ActorTerminalOutcome, ActorTerminalReason,
        Spawn, WeakActorRef,
    },
    error::{ActorStopReason, Infallible},
    message::{Context, Message},
    supervision::{RestartPolicy, SupervisionStrategy},
};
use tokio::sync::oneshot;

struct ResourceActor {
    listener: TcpListener,
    stop_delay: Duration,
    drop_delay: Duration,
    lifecycle: LifecycleWitness,
}

struct LifecycleWitness {
    stop_sender: Option<oneshot::Sender<()>>,
    drop_sender: Option<oneshot::Sender<()>>,
}

impl ResourceActor {
    fn bind(stop_delay: Duration) -> ResourceActorFixture {
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).expect("test fixture binds a TCP listener");
        let address = listener
            .local_addr()
            .expect("bound listener has a local address");
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (drop_sender, drop_receiver) = oneshot::channel();

        ResourceActorFixture {
            actor: Self {
                listener,
                stop_delay,
                drop_delay: Duration::from_millis(300),
                lifecycle: LifecycleWitness {
                    stop_sender: Some(stop_sender),
                    drop_sender: Some(drop_sender),
                },
            },
            probe: SocketProbe { address },
            stop_receiver,
            drop_receiver,
        }
    }
}

impl Drop for ResourceActor {
    fn drop(&mut self) {
        let _listener_address = self.listener.local_addr();
        std::thread::sleep(self.drop_delay);
        if let Some(sender) = self.lifecycle.drop_sender.take() {
            let _ = sender.send(());
        }
    }
}

impl Actor for ResourceActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        tokio::time::sleep(self.stop_delay).await;
        if let Some(sender) = self.lifecycle.stop_sender.take() {
            let _ = sender.send(());
        }
        Ok(())
    }
}

struct ResourceActorFixture {
    actor: ResourceActor,
    probe: SocketProbe,
    stop_receiver: oneshot::Receiver<()>,
    drop_receiver: oneshot::Receiver<()>,
}

impl ResourceActorFixture {
    fn into_parts(
        self,
    ) -> (
        ResourceActor,
        SocketProbe,
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
    ) {
        (
            self.actor,
            self.probe,
            self.stop_receiver,
            self.drop_receiver,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct SocketProbe {
    address: SocketAddr,
}

impl SocketProbe {
    fn can_rebind(&self) -> bool {
        TcpListener::bind(self.address).is_ok()
    }
}

#[derive(Debug, Clone)]
struct LifecycleTestError(&'static str);

impl fmt::Display for LifecycleTestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl error::Error for LifecycleTestError {}

struct StartupFailureActor;

impl Actor for StartupFailureActor {
    type Args = ();
    type Error = LifecycleTestError;

    async fn on_start(
        _state: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Err(LifecycleTestError("startup failed"))
    }
}

struct StopFailureActor;

impl Actor for StopFailureActor {
    type Args = Self;
    type Error = LifecycleTestError;

    async fn on_start(
        state: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Err(LifecycleTestError("cleanup failed"))
    }
}

struct RestartSupervisor;

impl Actor for RestartSupervisor {
    type Args = Self;
    type Error = Infallible;

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForOne
    }

    async fn on_start(
        state: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

#[derive(Clone)]
struct RestartResourceArguments {
    address: SocketAddr,
    witness: Arc<RestartResourceWitness>,
}

struct RestartResourceWitness {
    start_count: AtomicUsize,
    drop_count: AtomicUsize,
    second_start_saw_previous_drop: AtomicBool,
    second_start_sender: Mutex<Option<oneshot::Sender<bool>>>,
}

impl RestartResourceWitness {
    fn new(second_start_sender: oneshot::Sender<bool>) -> Self {
        Self {
            start_count: AtomicUsize::new(0),
            drop_count: AtomicUsize::new(0),
            second_start_saw_previous_drop: AtomicBool::new(false),
            second_start_sender: Mutex::new(Some(second_start_sender)),
        }
    }

    fn record_start(&self) {
        let previous_start_count = self.start_count.fetch_add(1, Ordering::SeqCst);
        if previous_start_count == 1 {
            let saw_previous_drop = self.drop_count.load(Ordering::SeqCst) > 0;
            self.second_start_saw_previous_drop
                .store(saw_previous_drop, Ordering::SeqCst);
            if let Some(sender) = self
                .second_start_sender
                .lock()
                .expect("second-start sender lock is not poisoned")
                .take()
            {
                let _ = sender.send(saw_previous_drop);
            }
        }
    }

    fn record_drop(&self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

struct RestartResourceActor {
    listener: TcpListener,
    witness: Arc<RestartResourceWitness>,
}

impl Drop for RestartResourceActor {
    fn drop(&mut self) {
        let _listener_address = self.listener.local_addr();
        std::thread::sleep(Duration::from_millis(200));
        self.witness.record_drop();
    }
}

impl Actor for RestartResourceActor {
    type Args = RestartResourceArguments;
    type Error = LifecycleTestError;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let listener = TcpListener::bind(arguments.address)
            .map_err(|_| LifecycleTestError("restart resource bind failed"))?;
        arguments.witness.record_start();
        Ok(Self {
            listener,
            witness: arguments.witness,
        })
    }
}

struct StopActor;

impl Message<StopActor> for RestartResourceActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: StopActor,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        context.stop();
    }
}

struct AdmissionActor {
    cleanup_started_sender: Option<oneshot::Sender<()>>,
    cleanup_release_receiver: Option<oneshot::Receiver<()>>,
}

impl Actor for AdmissionActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        if let Some(sender) = self.cleanup_started_sender.take() {
            let _ = sender.send(());
        }
        if let Some(receiver) = self.cleanup_release_receiver.take() {
            let _ = receiver.await;
        }
        Ok(())
    }
}

struct LinkOutcomeObserver {
    outcome_sender: Mutex<Option<oneshot::Sender<ActorTerminalOutcome>>>,
}

impl LinkOutcomeObserver {
    fn new(outcome_sender: oneshot::Sender<ActorTerminalOutcome>) -> Self {
        Self {
            outcome_sender: Mutex::new(Some(outcome_sender)),
        }
    }
}

impl Actor for LinkOutcomeObserver {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_link_died(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _id: ActorId,
        outcome: ActorTerminalOutcome,
        _reason: ActorStopReason,
    ) -> Result<std::ops::ControlFlow<ActorStopReason>, Self::Error> {
        if let Some(sender) = self
            .outcome_sender
            .lock()
            .expect("link outcome sender lock is not poisoned")
            .take()
        {
            let _ = sender.send(outcome);
        }
        Ok(std::ops::ControlFlow::Continue(()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdmissionProbe;

impl Message<AdmissionProbe> for AdmissionActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: AdmissionProbe,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
    }
}

struct ActorScenario {
    stop_delay: Duration,
}

impl ActorScenario {
    fn delayed_stop() -> Self {
        Self {
            stop_delay: Duration::from_millis(150),
        }
    }

    fn resource_actor(
        &self,
    ) -> (
        ResourceActor,
        SocketProbe,
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
    ) {
        ResourceActor::bind(self.stop_delay).into_parts()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_shutdown_returns_after_cleanup_drop_and_notifications() {
    let (actor, probe, mut stop_receiver, mut drop_receiver) =
        ActorScenario::delayed_stop().resource_actor();
    let actor_reference = ResourceActor::spawn_in_thread(actor);
    let weak_actor_reference = actor_reference.downgrade();

    actor_reference.wait_for_startup().await;
    assert!(
        !probe.can_rebind(),
        "the actor should hold the listener while running"
    );

    actor_reference
        .stop_gracefully()
        .await
        .expect("actor accepts graceful stop");

    let strong_outcome = actor_reference.wait_for_shutdown().await;
    let weak_outcome = weak_actor_reference.wait_for_shutdown().await;
    assert_eq!(strong_outcome, weak_outcome);
    assert_eq!(strong_outcome.state, ActorStateAbsence::Dropped);
    assert_eq!(strong_outcome.reason, ActorTerminalReason::Stopped);

    tokio::time::timeout(Duration::from_millis(20), &mut stop_receiver)
        .await
        .expect("on_stop completed before wait_for_shutdown returned")
        .expect("on_stop witness sender remains alive");
    tokio::time::timeout(Duration::from_millis(20), &mut drop_receiver)
        .await
        .expect("actor dropped before wait_for_shutdown returned")
        .expect("drop witness sender remains alive");
    assert!(
        probe.can_rebind(),
        "wait_for_shutdown means the actor's resource is gone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_admission_stops_before_cleanup_finishes() {
    let (cleanup_started_sender, cleanup_started_receiver) = oneshot::channel();
    let (cleanup_release_sender, cleanup_release_receiver) = oneshot::channel();
    let actor_reference = AdmissionActor::spawn(AdmissionActor {
        cleanup_started_sender: Some(cleanup_started_sender),
        cleanup_release_receiver: Some(cleanup_release_receiver),
    });

    actor_reference.wait_for_startup().await;
    assert!(
        actor_reference.is_accepting_messages(),
        "actor accepts messages after startup"
    );
    actor_reference
        .stop_gracefully()
        .await
        .expect("actor accepts graceful stop");
    cleanup_started_receiver
        .await
        .expect("cleanup start witness sender remains alive");

    assert!(
        !actor_reference.is_accepting_messages(),
        "shutdown closes ordinary message admission before cleanup finishes"
    );
    assert!(
        !actor_reference.is_terminated(),
        "closing message admission is not the same as terminal shutdown"
    );
    assert_eq!(
        actor_reference.tell(AdmissionProbe).send().await,
        Err(kameo::error::SendError::ActorNotRunning(AdmissionProbe)),
        "message admission closes before on_stop finishes"
    );

    cleanup_release_sender
        .send(())
        .expect("cleanup release receiver remains alive");
    let outcome = actor_reference.wait_for_shutdown().await;
    assert_eq!(outcome.state, ActorStateAbsence::Dropped);
    assert_eq!(outcome.reason, ActorTerminalReason::Stopped);
    assert!(
        actor_reference.is_terminated(),
        "actor is terminal after wait_for_shutdown returns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn link_signal_delivers_terminal_outcome_to_actor_hook() {
    let (outcome_sender, outcome_receiver) = oneshot::channel();
    let observer = LinkOutcomeObserver::spawn(LinkOutcomeObserver::new(outcome_sender));
    let (actor, _probe, _stop_receiver, _drop_receiver) =
        ActorScenario::delayed_stop().resource_actor();
    let linked_actor = ResourceActor::spawn(actor);

    linked_actor.link(&observer).await;
    linked_actor
        .stop_gracefully()
        .await
        .expect("linked actor accepts graceful stop");

    let linked_actor_outcome = linked_actor.wait_for_shutdown().await;
    let observed_outcome = tokio::time::timeout(Duration::from_secs(1), outcome_receiver)
        .await
        .expect("observer receives link terminal outcome")
        .expect("link outcome sender remains alive");

    assert_eq!(observed_outcome, linked_actor_outcome);
    assert_eq!(observed_outcome.state, ActorStateAbsence::Dropped);
    assert_eq!(observed_outcome.reason, ActorTerminalReason::Stopped);

    observer
        .stop_gracefully()
        .await
        .expect("observer accepts graceful stop");
    observer.wait_for_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_failure_returns_never_allocated_outcome() {
    let actor_reference = StartupFailureActor::spawn(());

    assert!(
        actor_reference.wait_for_startup_result().await.is_err(),
        "startup failure is visible through startup result"
    );
    let outcome = actor_reference.wait_for_shutdown().await;
    assert_eq!(outcome.state, ActorStateAbsence::NeverAllocated);
    assert_eq!(outcome.reason, ActorTerminalReason::StartupFailed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_error_returns_cleanup_failed_outcome() {
    let actor_reference = StopFailureActor::spawn(StopFailureActor);

    actor_reference.wait_for_startup().await;
    actor_reference
        .stop_gracefully()
        .await
        .expect("actor accepts graceful stop");

    let outcome = actor_reference.wait_for_shutdown().await;
    assert_eq!(outcome.state, ActorStateAbsence::Dropped);
    assert_eq!(outcome.reason, ActorTerminalReason::CleanupFailed);
    assert!(
        actor_reference.wait_for_shutdown_result().await.is_err(),
        "cleanup failure remains visible through the compatibility result API"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_restart_waits_for_terminal_outcome_before_replacement_start() {
    let address_probe =
        TcpListener::bind(("127.0.0.1", 0)).expect("test fixture reserves an address");
    let address = address_probe
        .local_addr()
        .expect("address probe has a local address");
    drop(address_probe);

    let (second_start_sender, second_start_receiver) = oneshot::channel();
    let witness = Arc::new(RestartResourceWitness::new(second_start_sender));
    let supervisor = RestartSupervisor::spawn(RestartSupervisor);
    let child = RestartResourceActor::supervise(
        &supervisor,
        RestartResourceArguments {
            address,
            witness: witness.clone(),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .spawn()
    .await;

    child.wait_for_startup().await;
    child
        .tell(StopActor)
        .send()
        .await
        .expect("child accepts stop request");

    let second_start_saw_previous_drop =
        tokio::time::timeout(Duration::from_secs(2), second_start_receiver)
            .await
            .expect("supervisor restarts the child")
            .expect("second start witness sender remains alive");
    assert!(
        second_start_saw_previous_drop,
        "supervisor must not restart a replacement before old child state drops"
    );
    assert_eq!(witness.drop_count.load(Ordering::SeqCst), 1);

    supervisor
        .stop_gracefully()
        .await
        .expect("supervisor accepts graceful stop");
    supervisor.wait_for_shutdown().await;
}
