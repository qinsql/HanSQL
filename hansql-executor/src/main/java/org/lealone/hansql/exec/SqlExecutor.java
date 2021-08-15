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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.common.exceptions.ExecutionSetupException;
import org.lealone.hansql.common.exceptions.UserException;
import org.lealone.hansql.common.logical.LogicalPlan;
import org.lealone.hansql.common.logical.PlanProperties.Generator.ResultMode;
import org.lealone.hansql.exec.context.DrillbitContext;
import org.lealone.hansql.exec.exception.OptimizerException;
import org.lealone.hansql.exec.ops.FragmentContextImpl;
import org.lealone.hansql.exec.ops.QueryContext;
import org.lealone.hansql.exec.opt.BasicOptimizer;
import org.lealone.hansql.exec.physical.PhysicalPlan;
import org.lealone.hansql.exec.physical.base.FragmentRoot;
import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.planner.SqlPlanner;
import org.lealone.hansql.exec.planner.fragment.DefaultQueryParallelizer;
import org.lealone.hansql.exec.planner.fragment.Fragment;
import org.lealone.hansql.exec.planner.fragment.MakeFragmentsVisitor;
import org.lealone.hansql.exec.proto.BitControl.PlanFragment;
import org.lealone.hansql.exec.proto.ExecProtos.FragmentHandle;
import org.lealone.hansql.exec.proto.ExecProtos.ServerPreparedStatementState;
import org.lealone.hansql.exec.proto.UserBitShared.QueryId;
import org.lealone.hansql.exec.proto.UserProtos.PreparedStatementHandle;
import org.lealone.hansql.exec.proto.UserProtos.RunQuery;
import org.lealone.hansql.exec.proto.helper.QueryIdHelper;
import org.lealone.hansql.exec.session.UserClientConnection;
import org.lealone.hansql.exec.testing.ControlsInjector;
import org.lealone.hansql.exec.testing.ControlsInjectorFactory;
import org.lealone.hansql.exec.util.Pointer;
import org.lealone.hansql.exec.work.QueryWorkUnit;
import org.lealone.hansql.exec.work.exception.SqlExecutorException;
import org.lealone.hansql.exec.work.exception.SqlExecutorSetupException;
import org.lealone.hansql.exec.work.filter.RuntimeFilterRouter;

import com.google.protobuf.InvalidProtocolBufferException;

public class SqlExecutor implements Runnable {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlExecutor.class);
    private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(SqlExecutor.class);

    private final QueryId queryId;
    private final String queryIdString;
    private final RunQuery queryRequest;
    private final QueryContext queryContext;
    private final DrillbitContext drillbitContext;
    private final UserClientConnection clientConnection; // used to send responses

    private RuntimeFilterRouter runtimeFilterRouter;
    private boolean enableRuntimeFilter;

    /**
     * Constructor. Sets up the SqlExecutor, but does not initiate any execution.
     *
     * @param executor executor
     * @param drillbitContext drillbit context
     * @param connection connection
     * @param queryId the id for the query
     * @param queryRequest the query to execute
     */
    public SqlExecutor(Executor executor, DrillbitContext drillbitContext, UserClientConnection connection,
            QueryId queryId, RunQuery queryRequest) {
        this.queryId = queryId;
        this.queryIdString = QueryIdHelper.getQueryId(queryId);
        this.queryRequest = queryRequest;
        this.queryContext = new QueryContext(connection.getSession(), drillbitContext, queryId);
        this.drillbitContext = drillbitContext;
        this.clientConnection = connection;

        // Apply AutoLimit on resultSet (Usually received via REST APIs)
        int autoLimit = queryRequest.getAutolimitRowcount();
        if (autoLimit > 0) {
            connection.getSession().getOptions().setLocalOption(ExecConstants.QUERY_MAX_ROWS, autoLimit);
        }

        this.enableRuntimeFilter = queryContext.getOptions()
                .getOption(ExecConstants.HASHJOIN_ENABLE_RUNTIME_FILTER_KEY).bool_val;
    }

    /**
     * Get the QueryContext created for the query.
     *
     * @return the QueryContext
     */
    public QueryContext getQueryContext() {
        return queryContext;
    }

    /**
     * Called by execution pool to do query setup, and kick off remote execution.
     *
     * <p>Note that completion of this function is not the end of the Foreman's role
     * in the query's lifecycle.
     */
    @Override
    public void run() {
        // rename the thread we're using for debugging purposes
        Thread currentThread = Thread.currentThread();
        String originalName = currentThread.getName();
        currentThread.setName(queryIdString + ":foreman");

        try {
            injector.injectChecked(queryContext.getExecutionControls(), "run-try-beginning",
                    SqlExecutorException.class);

            // convert a run query request into action
            switch (queryRequest.getType()) {
            case SQL:
                String sql = queryRequest.getPlan();
                // log query id, user name and query text before starting any real work. Also, put
                // them together such that it is easy to search based on query id
                logger.info("Query text for query with id {} issued by {}: {}", queryIdString,
                        queryContext.getQueryUserName(), sql);
                runSQL(sql);
                break;
            case LOGICAL:
                parseAndRunLogicalPlan(queryRequest.getPlan());
                break;
            case PHYSICAL:
                parseAndRunPhysicalPlan(queryRequest.getPlan());
                break;
            case EXECUTION:
                runFragment(queryRequest.getFragmentsList());
                break;
            case PREPARED_STATEMENT:
                runPreparedStatement(queryRequest.getPreparedStatementHandle());
                break;
            default:
                throw new IllegalStateException();
            }
            injector.injectChecked(queryContext.getExecutionControls(), "run-try-end", SqlExecutorException.class);
        } catch (Exception ex) {
        } finally {
            // restore the thread's original name
            currentThread.setName(originalName);
        }
    }

    private void runSQL(String sql) throws ExecutionSetupException {
        Pointer<String> textPlan = new Pointer<>();
        PhysicalPlan plan = SqlPlanner.getPlan(queryContext, sql, textPlan);
        runPhysicalPlan(plan, textPlan);
    }

    private void runPhysicalPlan(PhysicalPlan plan, Pointer<String> textPlan) throws ExecutionSetupException {
        validatePlan(plan);

        QueryWorkUnit work = getQueryWorkUnit(plan);
        if (enableRuntimeFilter) {
            runtimeFilterRouter = new RuntimeFilterRouter(work, drillbitContext);
            runtimeFilterRouter.collectRuntimeFilterParallelAndControlInfo();
        }
        work.applyPlan(drillbitContext.getPlanReader());
        logWorkUnit(work);

        executeQuery(work.getFragments(), work.getRootFragment(), work.getRootOperator());
    }

    private static void validatePlan(PhysicalPlan plan) throws SqlExecutorSetupException {
        if (plan.getProperties().resultMode != ResultMode.EXEC) {
            throw new SqlExecutorSetupException(String.format(
                    "Failure running plan. You requested a result mode of %s and a physical plan can only be output as EXEC",
                    plan.getProperties().resultMode));
        }
    }

    private QueryWorkUnit getQueryWorkUnit(PhysicalPlan plan) throws ExecutionSetupException {
        PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();
        Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);
        return new DefaultQueryParallelizer(plan.getProperties().hasResourcePlan, getQueryContext()).generateWorkUnit(
                queryContext.getOptions().getOptionList(), queryContext.getCurrentEndpoint(), queryId,
                queryContext.getOnlineEndpoints(), rootFragment, clientConnection.getSession(),
                queryContext.getQueryContextInfo());
    }

    private void executeQuery(List<PlanFragment> planFragments, PlanFragment rootPlanFragment,
            FragmentRoot rootOperator) throws ExecutionSetupException {
        FragmentContextImpl rootContext = new FragmentContextImpl(drillbitContext, rootPlanFragment, queryContext,
                clientConnection, drillbitContext.getFunctionImplementationRegistry());
        FragmentExecutor fragmentExecutor = new FragmentExecutor(rootContext, rootPlanFragment, rootOperator,
                clientConnection);
        fragmentExecutor.execute();
    }

    private void parseAndRunLogicalPlan(String json) throws ExecutionSetupException {
        LogicalPlan logicalPlan;
        try {
            logicalPlan = drillbitContext.getPlanReader().readLogicalPlan(json);
        } catch (IOException e) {
            throw new SqlExecutorException("Failure parsing logical plan.", e);
        }

        if (logicalPlan.getProperties().resultMode == ResultMode.LOGICAL) {
            throw new SqlExecutorException(
                    "Failure running plan. You requested a result mode of LOGICAL and submitted a logical plan. "
                            + "In this case you're output mode must be PHYSICAL or EXEC.");
        }

        log(logicalPlan);

        PhysicalPlan physicalPlan = convert(logicalPlan);

        if (logicalPlan.getProperties().resultMode == ResultMode.PHYSICAL) {
            returnPhysical(physicalPlan);
            return;
        }

        log(physicalPlan);
        runPhysicalPlan(physicalPlan);
    }

    private void log(LogicalPlan plan) {
        if (logger.isDebugEnabled()) {
            logger.debug("Logical {}", plan.unparse(queryContext.getLpPersistence()));
        }
    }

    private void log(PhysicalPlan plan) {
        if (logger.isDebugEnabled()) {
            try {
                String planText = queryContext.getLpPersistence().getMapper().writeValueAsString(plan);
                logger.debug("Physical {}", planText);
            } catch (IOException e) {
                logger.warn("Error while attempting to log physical plan.", e);
            }
        }
    }

    private void returnPhysical(PhysicalPlan plan) throws ExecutionSetupException {
        String jsonPlan = plan.unparse(queryContext.getLpPersistence().getMapper().writer());
        runPhysicalPlan(SqlPlanner.createDirectPlan(queryContext, new PhysicalFromLogicalExplain(jsonPlan)));
    }

    public static class PhysicalFromLogicalExplain {
        public final String json;

        public PhysicalFromLogicalExplain(String json) {
            this.json = json;
        }
    }

    private void parseAndRunPhysicalPlan(String json) throws ExecutionSetupException {
        try {
            PhysicalPlan plan = drillbitContext.getPlanReader().readPhysicalPlan(json);
            runPhysicalPlan(plan);
        } catch (IOException e) {
            throw new SqlExecutorSetupException("Failure while parsing physical plan.", e);
        }
    }

    private void runPhysicalPlan(PhysicalPlan plan) throws ExecutionSetupException {
        runPhysicalPlan(plan, null);
    }

    /**
     * This is a helper method to run query based on the list of PlanFragment that were planned
     * at some point of time
     * @param fragmentsList fragment list
     * @throws ExecutionSetupException
     */
    private void runFragment(List<PlanFragment> fragmentsList) throws ExecutionSetupException {
        // need to set QueryId, MinorFragment for incoming Fragments
        PlanFragment rootFragment = null;
        boolean isFirst = true;
        List<PlanFragment> planFragments = Lists.newArrayList();
        for (PlanFragment myFragment : fragmentsList) {
            FragmentHandle handle = myFragment.getHandle();
            // though we have new field in the FragmentHandle - parentQueryId
            // it can not be used until every piece of code that creates handle is using it, as otherwise
            // comparisons on that handle fail that causes fragment runtime failure
            FragmentHandle newFragmentHandle = FragmentHandle.newBuilder()
                    .setMajorFragmentId(handle.getMajorFragmentId()).setMinorFragmentId(handle.getMinorFragmentId())
                    .setQueryId(queryId).build();
            PlanFragment newFragment = PlanFragment.newBuilder(myFragment).setHandle(newFragmentHandle).build();
            if (isFirst) {
                rootFragment = newFragment;
                isFirst = false;
            } else {
                planFragments.add(newFragment);
            }
        }

        assert rootFragment != null;

        FragmentRoot rootOperator;
        try {
            rootOperator = drillbitContext.getPlanReader().readFragmentRoot(rootFragment.getFragmentJson());
        } catch (IOException e) {
            throw new ExecutionSetupException(
                    String.format("Unable to parse FragmentRoot from fragment: %s", rootFragment.getFragmentJson()));
        }

        executeQuery(planFragments, rootFragment, rootOperator);
    }

    /**
     * Helper method to execute the query in prepared statement. Current implementation takes the query from opaque
     * object of the <code>preparedStatement</code> and submits as a new query.
     *
     * @param preparedStatementHandle prepared statement handle
     * @throws ExecutionSetupException
     */
    private void runPreparedStatement(PreparedStatementHandle preparedStatementHandle) throws ExecutionSetupException {
        ServerPreparedStatementState serverState;

        try {
            serverState = ServerPreparedStatementState.PARSER.parseFrom(preparedStatementHandle.getServerInfo());
        } catch (InvalidProtocolBufferException ex) {
            throw UserException.parseError(ex)
                    .message("Failed to parse the prepared statement handle. "
                            + "Make sure the handle is same as one returned from create prepared statement call.")
                    .build(logger);
        }

        String queryText = serverState.getSqlQuery();
        logger.info("Prepared statement query for QueryId {} : {}", queryId, queryText);
        runSQL(queryText);
    }

    private void logWorkUnit(QueryWorkUnit queryWorkUnit) {
        if (!logger.isTraceEnabled()) {
            return;
        }
        logger.trace(String.format("PlanFragments for query %s \n%s", queryId, queryWorkUnit.stringifyFragments()));
    }

    private PhysicalPlan convert(LogicalPlan plan) throws OptimizerException {
        if (logger.isDebugEnabled()) {
            logger.debug("Converting logical plan {}.", plan.toJsonStringSafe(queryContext.getLpPersistence()));
        }
        return new BasicOptimizer(queryContext, clientConnection)
                .optimize(new BasicOptimizer.BasicOptimizationContext(queryContext), plan);
    }
}
