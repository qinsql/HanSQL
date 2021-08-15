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
package org.lealone.hansql.exec.physical.impl.unorderedreceiver;

import java.io.IOException;
import java.util.Iterator;

import org.lealone.hansql.common.expression.SchemaPath;
import org.lealone.hansql.exec.exception.OutOfMemoryException;
import org.lealone.hansql.exec.exception.SchemaChangeException;
import org.lealone.hansql.exec.ops.ExchangeFragmentContext;
import org.lealone.hansql.exec.ops.FragmentContext;
import org.lealone.hansql.exec.ops.MetricDef;
import org.lealone.hansql.exec.ops.OperatorContext;
import org.lealone.hansql.exec.ops.OperatorStats;
import org.lealone.hansql.exec.physical.MinorFragmentEndpoint;
import org.lealone.hansql.exec.physical.config.UnorderedReceiver;
import org.lealone.hansql.exec.proto.BitControl.FinishedReceiver;
import org.lealone.hansql.exec.proto.ExecProtos.FragmentHandle;
import org.lealone.hansql.exec.proto.UserBitShared.RecordBatchDef;
import org.lealone.hansql.exec.record.BatchSchema;
import org.lealone.hansql.exec.record.CloseableRecordBatch;
import org.lealone.hansql.exec.record.RawFragmentBatch;
import org.lealone.hansql.exec.record.RawFragmentBatchProvider;
import org.lealone.hansql.exec.record.RecordBatchLoader;
import org.lealone.hansql.exec.record.TypedFieldId;
import org.lealone.hansql.exec.record.VectorContainer;
import org.lealone.hansql.exec.record.VectorWrapper;
import org.lealone.hansql.exec.record.WritableBatch;
import org.lealone.hansql.exec.record.selection.SelectionVector2;
import org.lealone.hansql.exec.record.selection.SelectionVector4;
import org.lealone.hansql.exec.testing.ControlsInjector;
import org.lealone.hansql.exec.testing.ControlsInjectorFactory;

public class UnorderedReceiverBatch implements CloseableRecordBatch {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnorderedReceiverBatch.class);
    private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(UnorderedReceiverBatch.class);

    private final RecordBatchLoader batchLoader;
    private final RawFragmentBatchProvider fragProvider;
    private final ExchangeFragmentContext context;
    private BatchSchema schema;
    private final OperatorStats stats;
    private boolean first = true;
    private final UnorderedReceiver config;
    private final OperatorContext oContext;
    // Represents last outcome of next(). If an Exception is thrown
    // during the method's execution a value IterOutcome.STOP will be assigned.
    private IterOutcome lastOutcome;

    public enum Metric implements MetricDef {
        BYTES_RECEIVED,
        NUM_SENDERS;

        @Override
        public int metricId() {
            return ordinal();
        }
    }

    public UnorderedReceiverBatch(final ExchangeFragmentContext context, final RawFragmentBatchProvider fragProvider,
            final UnorderedReceiver config) throws OutOfMemoryException {
        this.fragProvider = fragProvider;
        this.context = context;
        // In normal case, batchLoader does not require an allocator. However, in case of splitAndTransfer of a value
        // vector,
        // we may need an allocator for the new offset vector. Therefore, here we pass the context's allocator to
        // batchLoader.
        oContext = context.newOperatorContext(config);
        this.batchLoader = new RecordBatchLoader(oContext.getAllocator());

        this.stats = oContext.getStats();
        this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
        this.config = config;

        // // Register this operator's buffer allocator so that incoming buffers are owned by this allocator
        // context.getBuffers().getCollector(config.getOppositeMajorFragmentId()).setAllocator(oContext.getAllocator());
    }

    @Override
    public FragmentContext getContext() {
        return context;
    }

    @Override
    public BatchSchema getSchema() {
        return schema;
    }

    @Override
    public int getRecordCount() {
        return batchLoader.getRecordCount();
    }

    @Override
    public void kill(final boolean sendUpstream) {
        if (sendUpstream) {
            informSenders();
        }
        fragProvider.kill(context);
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
        return batchLoader.iterator();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypedFieldId getValueVectorId(final SchemaPath path) {
        return batchLoader.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(final Class<?> clazz, final int... ids) {
        return batchLoader.getValueAccessorById(clazz, ids);
    }

    private RawFragmentBatch getNextBatch() throws IOException {
        try {
            injector.injectInterruptiblePause(context.getExecutionControls(), "waiting-for-data", logger);
            return fragProvider.getNext();
        } catch (final InterruptedException e) {
            // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of
            // the
            // interruption and respond to it if it wants to.
            Thread.currentThread().interrupt();

            return null;
        }
    }

    @Override
    public IterOutcome next() {
        batchLoader.resetRecordCount();
        stats.startProcessing();
        try {
            RawFragmentBatch batch;
            try {
                stats.startWait();
                batch = getNextBatch();

                // skip over empty batches. we do this since these are basically control messages.
                while (batch != null && batch.getHeader().getDef().getRecordCount() == 0
                        && (!first || batch.getHeader().getDef().getFieldCount() == 0)) {
                    batch = getNextBatch();
                }
            } finally {
                stats.stopWait();
            }

            first = false;

            if (batch == null) {
                lastOutcome = IterOutcome.NONE;
                batchLoader.zero();
                if (!context.getExecutorState().shouldContinue()) {
                    lastOutcome = IterOutcome.STOP;
                }
                return lastOutcome;
            }

            if (context.getAllocator().isOverLimit()) {
                lastOutcome = IterOutcome.OUT_OF_MEMORY;
                return lastOutcome;
            }

            final RecordBatchDef rbd = batch.getHeader().getDef();
            final boolean schemaChanged = batchLoader.load(rbd, batch.getBody());
            // TODO: Clean: DRILL-2933: That load(...) no longer throws
            // SchemaChangeException, so check/clean catch clause below.
            stats.addLongStat(Metric.BYTES_RECEIVED, batch.getByteCount());

            batch.release();
            if (schemaChanged) {
                this.schema = batchLoader.getSchema();
                stats.batchReceived(0, rbd.getRecordCount(), true);
                lastOutcome = IterOutcome.OK_NEW_SCHEMA;
            } else {
                stats.batchReceived(0, rbd.getRecordCount(), false);
                lastOutcome = IterOutcome.OK;
            }
            return lastOutcome;
        } catch (SchemaChangeException | IOException ex) {
            context.getExecutorState().fail(ex);
            lastOutcome = IterOutcome.STOP;
            return lastOutcome;
        } catch (Exception e) {
            lastOutcome = IterOutcome.STOP;
            throw e;
        } finally {
            stats.stopProcessing();
        }
    }

    @Override
    public WritableBatch getWritableBatch() {
        return batchLoader.getWritableBatch();
    }

    @Override
    public void close() {
        batchLoader.clear();
    }

    @Override
    public VectorContainer getOutgoingContainer() {
        throw new UnsupportedOperationException(String.format(
                " You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
    }

    @Override
    public VectorContainer getContainer() {
        return batchLoader.getContainer();
    }

    private void informSenders() {
        logger.info("Informing senders of request to terminate sending.");
        final FragmentHandle handlePrototype = FragmentHandle.newBuilder()
                .setMajorFragmentId(config.getOppositeMajorFragmentId()).setQueryId(context.getHandle().getQueryId())
                .build();
        for (final MinorFragmentEndpoint providingEndpoint : config.getProvidingEndpoints()) {
            final FragmentHandle sender = FragmentHandle.newBuilder(handlePrototype)
                    .setMinorFragmentId(providingEndpoint.getId()).build();
            final FinishedReceiver finishedReceiver = FinishedReceiver.newBuilder().setReceiver(context.getHandle())
                    .setSender(sender).build();
            // context.getController()
            // .getTunnel(providingEndpoint.getEndpoint())
            // .informReceiverFinished(new OutcomeListener(), finishedReceiver);
        }
    }

    @Override
    public void dump() {
        logger.error("UnorderedReceiverBatch[batchLoader={}, schema={}]", batchLoader, schema);
    }

    @Override
    public boolean hasFailed() {
        return lastOutcome == IterOutcome.STOP;
    }
}
