/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.exec;

import static org.lealone.hansql.exec.context.FailureUtils.EXIT_CODE_HEAP_OOM;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.hansql.common.DeferredException;
import org.lealone.hansql.common.EventProcessor;
import org.lealone.hansql.common.exceptions.UserException;
import org.lealone.hansql.exec.context.FailureUtils;
import org.lealone.hansql.exec.exception.OutOfMemoryException;
import org.lealone.hansql.exec.ops.ExecutorFragmentContext;
import org.lealone.hansql.exec.ops.FragmentContext;
import org.lealone.hansql.exec.physical.base.FragmentRoot;
import org.lealone.hansql.exec.physical.impl.ImplCreator;
import org.lealone.hansql.exec.physical.impl.RootExec;
import org.lealone.hansql.exec.proto.BitControl.PlanFragment;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.lealone.hansql.exec.proto.ExecProtos.FragmentHandle;
import org.lealone.hansql.exec.proto.UserBitShared.FragmentState;
import org.lealone.hansql.exec.proto.helper.QueryIdHelper;
import org.lealone.hansql.exec.session.UserClientConnection;
import org.lealone.hansql.exec.testing.ControlsInjector;
import org.lealone.hansql.exec.testing.ControlsInjectorFactory;
import org.lealone.sql.query.Select;

/**
 * <h2>Overview</h2>
 * <p>
 *   Responsible for running a single fragment on a single Drillbit. Listens/responds to status request and cancellation messages.
 * </p>
 * <h2>Theory of Operation</h2>
 * <p>
 *  The {@link FragmentExecutor} runs a fragment's {@link RootExec} in the {@link FragmentExecutor#run()} method in a single thread. While a fragment is running
 *  it may be subject to termination requests. The {@link FragmentExecutor} is reponsible for gracefully handling termination requests for the {@link RootExec}. There
 *  are two types of termination messages:
 *  <ol>
 *    <li><b>Cancellation Request:</b> This signals that the fragment and therefore the {@link RootExec} need to terminate immediately.</li>
 *    <li><b>Receiver Finished:</b> This signals that a downstream receiver no longer needs anymore data. A fragment may recieve multiple receiver finished requests
 *    (one for each downstream receiver). The {@link RootExec} will only terminate once it has recieved {@link FragmentExecutor.EventType#RECEIVER_FINISHED} messages
 *    for all downstream receivers.</li>
 *  </ol>
 * </p>
 * <p>
 *   The {@link FragmentExecutor} processes termination requests appropriately for the {@link RootExec}. A <b>Cancellation Request</b> is signalled when
 *   {@link FragmentExecutor#cancel()} is called. A <b>Receiver Finished</b> event is signalled when {@link FragmentExecutor#receivingFragmentFinished(FragmentHandle)} is
 *   called. The way in which these signals are handled is the following:
 * </p>
 * <h3>Cancellation Request</h3>
 * <p>
 *   There are two ways in which a cancellation request can be handled when {@link FragmentExecutor#cancel()} is called.
 *   <ol>
 *     <li>The Cancellation Request is recieved before the {@link RootExec} for the fragment is even started. In this case we can cleanup resources allocated for the fragment
 *     and never start a {@link RootExec}</li>
 *     <li>The Cancellation Request is recieve after the {@link RootExec} for the fragment is started. In this the cancellation request is sent to the
 *     {@link FragmentEventProcessor}. If this is not the first cancellation request it is ignored. If this is the first cancellation request the {@link RootExec} for this
 *     fragment is terminated by interrupting it. Then the {@link FragmentExecutor#run()} thread proceeds to cleanup resources normally</li>
 *   </ol>
 * </p>
 * <h3>Receiver Finished</h3>
 * <p>
 *  When {@link FragmentExecutor#receivingFragmentFinished(FragmentHandle)} is called, the message is passed to the {@link FragmentEventProcessor} if we
 *  did not already recieve a Cancellation request. Then the finished message is queued in {@link FragmentExecutor#receiverFinishedQueue}. The {@link FragmentExecutor#run()} polls
 *  {@link FragmentExecutor#receiverFinishedQueue} and singlas the {@link RootExec} with {@link RootExec#receivingFragmentFinished(FragmentHandle)} appropriately.
 * </p>
 * <h2>Possible Design Flaws / Poorly Defined Behavoir</h2>
 * <p>
 *   There are still a few aspects of the {@link FragmentExecutor} design that are not clear.
 *   <ol>
 *     <li>If we get a <b>Receiver Finished</b> message for one downstream receiver, will we eventually get one from every downstream receiver?</li>
 *     <li>What happens when we process a <b>Receiver Finished</b> message for some (but not all) downstream receivers and then we cancel the fragment?</li>
 *     <li>What happens when we process a <b>Receiver Finished</b> message for some (but not all) downstream receivers and then we run out of data from the upstream?</li>
 *   </ol>
 * </p>
 */
class FragmentExecutor {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutor.class);
    private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(FragmentExecutor.class);

    private final String fragmentName;
    private final ExecutorFragmentContext fragmentContext;
    private final DeferredException deferredException = new DeferredException();
    private final PlanFragment fragment;
    private final FragmentRoot rootOperator;

    private volatile RootExec root;
    private final AtomicReference<FragmentState> fragmentState = new AtomicReference<>(
            FragmentState.AWAITING_ALLOCATION);
    /**
     * Holds all of the messages sent by downstream receivers that have finished. The {@link FragmentExecutor#run()} thread reads from this queue and passes the
     * finished messages to the fragment's {@link RootExec} via the {@link RootExec#receivingFragmentFinished(FragmentHandle)} method.
     */
    private final Queue<FragmentHandle> receiverFinishedQueue = new ConcurrentLinkedQueue<>();
    private final FragmentEventProcessor eventProcessor = new FragmentEventProcessor();

    // Thread that is currently executing the Fragment. Value is null if the fragment hasn't started running or finished
    private final AtomicReference<Thread> myThreadRef = new AtomicReference<>(null);
    private final UserClientConnection clientConnection;

    /**
     * Create a FragmentExecutor where we already have a root operator in memory.
     *
     * @param context
     * @param fragment
     * @param statusReporter
     * @param rootOperator
     */
    FragmentExecutor(ExecutorFragmentContext context, PlanFragment fragment, FragmentRoot rootOperator,
            UserClientConnection clientConnection) {
        this.fragmentContext = context;
        this.fragment = fragment;
        this.rootOperator = rootOperator;
        this.fragmentName = QueryIdHelper.getQueryIdentifier(context.getHandle());
        this.clientConnection = clientConnection;
        context.setExecutorState(new ExecutorStateImpl());
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("FragmentExecutor [fragmentContext=");
        builder.append(fragmentContext);
        builder.append(", fragmentState=");
        builder.append(fragmentState);
        builder.append("]");
        return builder.toString();
    }

    /**
     * <p>
     * Cancel the execution of this fragment is in an appropriate state. Messages come from external.
     * </p>
     * <p>
     * <b>Note:</b> This will be called from threads <b>Other</b> than the one running this runnable(),
     * so we need to be careful about the state transitions that can result.
     * </p>
     */
    public void cancel() {
        boolean thisIsOnlyThread = myThreadRef.compareAndSet(null, Thread.currentThread());

        if (thisIsOnlyThread) {
            eventProcessor.cancelAndFinish();
            eventProcessor.start(); // start immediately as we are the first thread accessing this fragment
        } else {
            eventProcessor.cancel();
        }
    }

    private void cleanup(FragmentState state) {
        if (root != null && fragmentState.get() == FragmentState.FAILED) {
            root.dumpBatches();
        }

        closeOutResources();
        updateState(state);
    }

    private Runnable yieldableFragment;
    private Select select;

    public void setSelect(Select select) {
        this.select = select;
    }

    public void run() {
        yieldableFragment.run();
    }

    public void execute(boolean isStarting) {
        final Thread myThread = Thread.currentThread();

        if (!myThreadRef.compareAndSet(null, myThread)) {
            return;
        }

        // final String originalThreadName = myThread.getName();
        final FragmentHandle fragmentHandle = fragmentContext.getHandle();
        // final String newThreadName = QueryIdHelper.getExecutorThreadName(fragmentHandle);

        try {

            // myThread.setName(newThreadName);

            // if we didn't get the root operator when the executor was created, create it now.
            final FragmentRoot rootOperator = this.rootOperator != null ? this.rootOperator
                    : fragmentContext.getPlanReader().readFragmentRoot(fragment.getFragmentJson());

            root = ImplCreator.getExec(fragmentContext, rootOperator);
            if (root == null) {
                return;
            }

            updateState(FragmentState.RUNNING);

            eventProcessor.start();
            injector.injectPause(fragmentContext.getExecutionControls(), "fragment-running", logger);

            final DrillbitEndpoint endpoint = fragmentContext.getEndpoint();
            logger.debug("Starting fragment {}:{} on {}:{}", fragmentHandle.getMajorFragmentId(),
                    fragmentHandle.getMinorFragmentId(), endpoint.getAddress(), endpoint.getUserPort());

            injector.injectChecked(fragmentContext.getExecutionControls(), "fragment-execution", IOException.class);

            yieldableFragment = new Runnable() {
                @Override
                public void run() {
                    while (shouldContinue()) {
                        // Fragment is not cancelled

                        for (FragmentHandle fragmentHandle2; (fragmentHandle2 = receiverFinishedQueue
                                .poll()) != null;) {
                            // See if we have any finished requests. If so execute them.
                            root.receivingFragmentFinished(fragmentHandle2);
                        }

                        if (!root.next()) {
                            // Fragment has processed all of its data
                            break;
                        }

                        if (select != null && select.setCurrentRowNumber(clientConnection.getRowCount()))
                            return;
                    }
                    clientConnection.sendResult(null);
                    eventProcessor.terminate();
                    cleanup(FragmentState.FINISHED);
                }
            };
            if (!isStarting)
                yieldableFragment.run();
        } catch (OutOfMemoryError | OutOfMemoryException e) {
            if (FailureUtils.isDirectMemoryOOM(e)) {
                fail(UserException.memoryError(e).build(logger));
            } else {
                // we have a heap out of memory error. The JVM is unstable, exit.
                FailureUtils.unrecoverableFailure(e, "Unable to handle out of memory condition in FragmentExecutor.",
                        EXIT_CODE_HEAP_OOM);
            }
        } catch (Throwable t) {
            fail(t);
        } finally {
            // // Don't process any more termination requests, we are done.
            // eventProcessor.terminate();
            // // Clear the interrupt flag if it is set.
            // Thread.interrupted();
            //
            // // here we could be in FAILED, RUNNING, or CANCELLATION_REQUESTED
            // // FAILED state will be because of any Exception in execution loop root.next()
            // // CANCELLATION_REQUESTED because of a CANCEL request received by Foreman.
            // // ELSE will be in FINISHED state.
            // cleanup(FragmentState.FINISHED);
            //
            // // myThread.setName(originalThreadName);
        }
    }

    /**
     * Utility method to check where we are in a no terminal state.
     *
     * @return Whether or not execution should continue.
     */
    private boolean shouldContinue() {
        return !isCompleted() && FragmentState.CANCELLATION_REQUESTED != fragmentState.get();
    }

    /**
     * Returns true if the fragment is in a terminal state
     *
     * @return Whether this state is in a terminal state.
     */
    public boolean isCompleted() {
        return isTerminal(fragmentState.get());
    }

    private void closeOutResources() {

        // first close the operators and release all memory.
        try {
            // Say executor was cancelled before setup. Now when executor actually runs, root is not initialized, but
            // this
            // method is called in finally. So root can be null.
            if (root != null) {
                root.close();
            }
        } catch (final Exception e) {
            fail(e);
        }

        // then close the fragment context.
        fragmentContext.close();

    }

    private void warnStateChange(FragmentState current, FragmentState target) {
        logger.warn(fragmentName + ": Ignoring unexpected state transition {} --> {}", current.name(), target.name());
    }

    private void errorStateChange(FragmentState current, FragmentState target) {
    }

    private synchronized boolean updateState(FragmentState target) {
        final FragmentState current = fragmentState.get();
        logger.info(fragmentName + ": State change requested {} --> {}", current, target);
        switch (target) {
        case CANCELLATION_REQUESTED:
            switch (current) {
            case SENDING:
            case AWAITING_ALLOCATION:
            case RUNNING:
                fragmentState.set(target);
                return true;

            default:
                warnStateChange(current, target);
                return false;
            }

        case FINISHED:
            if (current == FragmentState.CANCELLATION_REQUESTED) {
                target = FragmentState.CANCELLED;
            } else if (current == FragmentState.FAILED) {
                target = FragmentState.FAILED;
            }
            // fall-through
        case FAILED:
            if (!isTerminal(current)) {
                fragmentState.set(target);
                // don't notify reporter until we finalize this terminal state.
                return true;
            } else if (current == FragmentState.FAILED) {
                // no warn since we can call fail multiple times.
                return false;
            } else if (current == FragmentState.CANCELLED && target == FragmentState.FAILED) {
                fragmentState.set(FragmentState.FAILED);
                return true;
            } else {
                warnStateChange(current, target);
                return false;
            }

        case RUNNING:
            if (current == FragmentState.AWAITING_ALLOCATION) {
                fragmentState.set(target);
                return true;
            } else {
                errorStateChange(current, target);
            }

            // these should never be requested.
        case CANCELLED:
        case SENDING:
        case AWAITING_ALLOCATION:
        default:
            errorStateChange(current, target);
        }

        // errorStateChange() throw should mean this is never executed
        throw new IllegalStateException();
    }

    private boolean isTerminal(final FragmentState state) {
        return state == FragmentState.CANCELLED || state == FragmentState.FAILED || state == FragmentState.FINISHED;
    }

    /**
     * Capture an exception and add store it. Update state to failed status (if not already there). Does not immediately
     * report status back to Foreman. Only the original thread can return status to the Foreman.
     *
     * @param excep
     *          The failure that occurred.
     */
    private void fail(final Throwable excep) {
        deferredException.addThrowable(excep);
        updateState(FragmentState.FAILED);
    }

    public ExecutorFragmentContext getContext() {
        return fragmentContext;
    }

    private class ExecutorStateImpl implements FragmentContext.ExecutorState {
        @Override
        public boolean shouldContinue() {
            return FragmentExecutor.this.shouldContinue();
        }

        @Override
        public void fail(final Throwable t) {
            FragmentExecutor.this.fail(t);
        }

        @Override
        public boolean isFailed() {
            return fragmentState.get() == FragmentState.FAILED;
        }

        @Override
        public Throwable getFailureCause() {
            return deferredException.getException();
        }
    }

    private enum EventType {
        CANCEL,
        CANCEL_AND_FINISH,
        RECEIVER_FINISHED
    }

    private class FragmentEvent {
        private final EventType type;
        private final FragmentHandle handle;

        FragmentEvent(EventType type, FragmentHandle handle) {
            this.type = type;
            this.handle = handle;
        }
    }

    /**
     * Implementation of EventProcessor to handle fragment cancellation and early terminations
     * without relying on a latch, thus avoiding to block the rpc control thread.<br>
     * This is especially important as fragments can take longer to start
     */
    private class FragmentEventProcessor extends EventProcessor<FragmentEvent> {
        private AtomicBoolean terminate = new AtomicBoolean(false);

        void cancel() {
            sendEvent(new FragmentEvent(EventType.CANCEL, null));
        }

        void cancelAndFinish() {
            sendEvent(new FragmentEvent(EventType.CANCEL_AND_FINISH, null));
        }

        // void receiverFinished(FragmentHandle handle) {
        // sendEvent(new FragmentEvent(EventType.RECEIVER_FINISHED, handle));
        // }

        /**
         * Tell the {@link FragmentEventProcessor} not to process anymore events. This keeps stray cancellation requests
         * from being processed after the root has finished running and interrupts in the root thread have been cleared.
         */
        public void terminate() {
            terminate.set(true);
        }

        @Override
        protected void processEvent(FragmentEvent event) {
            if (event.type.equals(EventType.RECEIVER_FINISHED)) {
                // Finish request
                if (terminate.get()) {
                    // We have already recieved a cancellation or we have terminated the event processor. Do not process
                    // anymore finish requests.
                    return;
                }
            } else {
                // Cancel request
                if (!terminate.compareAndSet(false, true)) {
                    // We have already received a cancellation or we have terminated the event processor. Do not process
                    // anymore cancellation requests.
                    // This prevents the root thread from being interrupted at an inappropriate time.
                    return;
                }
            }

            switch (event.type) {
            case CANCEL:
                // We set the cancel requested flag but the actual cancellation is managed by the run() loop, if called.
                updateState(FragmentState.CANCELLATION_REQUESTED);
                // The root was started so we have to interrupt it in case it is performing a blocking operation.
                killThread();
                break;
            case CANCEL_AND_FINISH:
                // In this case the root was never started so we do not have to interrupt the thread.
                updateState(FragmentState.CANCELLATION_REQUESTED);
                // The FragmentExecutor#run() loop will not execute in this case so we have to cleanup resources here
                cleanup(FragmentState.FINISHED);
                break;
            case RECEIVER_FINISHED:
                assert event.handle != null : "RECEIVER_FINISHED event must have a handle";
                if (root != null) {
                    logger.info("Applying request for early sender termination for {} -> {}.",
                            QueryIdHelper.getQueryIdentifier(getContext().getHandle()),
                            QueryIdHelper.getFragmentId(event.handle));

                    receiverFinishedQueue.add(event.handle);
                } else {
                    logger.warn(
                            "Dropping request for early fragment termination for path {} -> {} as no root exec exists.",
                            QueryIdHelper.getFragmentId(getContext().getHandle()),
                            QueryIdHelper.getFragmentId(event.handle));
                }
                // Note we do not terminate the event processor in this case since we can recieve multiple
                // RECEIVER_FINISHED
                // events. One for each downstream receiver.
                break;
            }
        }

        /*
         * Interrupt the thread so that it exits from any blocking operation it could be executing currently. We
         * synchronize here to ensure we don't accidentally create a race condition where we interrupt the close out
         * procedure of the main thread.
        */
        private void killThread() {
            // myThreadRef must contain a non-null reference at this point
            final Thread myThread = myThreadRef.get();
            logger.debug("Interrupting fragment thread {}", myThread.getName());
            myThread.interrupt();
        }
    }
}
