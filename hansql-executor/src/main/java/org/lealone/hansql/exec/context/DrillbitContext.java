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
package org.lealone.hansql.exec.context;

import static org.apache.drill.shaded.guava.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import org.lealone.hansql.common.config.DrillConfig;
import org.lealone.hansql.common.config.LogicalPlanPersistence;
import org.lealone.hansql.common.scanner.persistence.ScanResult;
import org.lealone.hansql.exec.compile.CodeCompiler;
import org.lealone.hansql.exec.context.options.SystemOptionManager;
import org.lealone.hansql.exec.coord.ClusterCoordinator;
import org.lealone.hansql.exec.expr.fn.FunctionImplementationRegistry;
import org.lealone.hansql.exec.expr.fn.registry.RemoteFunctionRegistry;
import org.lealone.hansql.exec.memory.BufferAllocator;
import org.lealone.hansql.exec.physical.impl.OperatorCreatorRegistry;
import org.lealone.hansql.exec.planner.PhysicalPlanReader;
import org.lealone.hansql.exec.planner.sql.DrillOperatorTable;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.lealone.hansql.exec.store.SchemaFactory;
import org.lealone.hansql.exec.store.StoragePluginRegistry;
import org.lealone.hansql.exec.store.sys.PersistentStoreProvider;

import com.codahale.metrics.MetricRegistry;

public class DrillbitContext implements AutoCloseable {

    private final BootStrapContext context;
    private final PhysicalPlanReader reader;
    private final ClusterCoordinator coord;
    private final DrillbitEndpoint endpoint;
    private final StoragePluginRegistry storagePlugins;
    private final OperatorCreatorRegistry operatorCreatorRegistry;
    private final FunctionImplementationRegistry functionRegistry;
    private final SystemOptionManager systemOptions;
    private final PersistentStoreProvider provider;
    private final CodeCompiler compiler;
    private final ScanResult classpathScan;
    private final LogicalPlanPersistence lpPersistence;
    // operator table for standard SQL operators and functions, Drill built-in UDFs
    private final DrillOperatorTable table;
    private final QueryProfileStoreContext profileStoreContext;

    public DrillbitContext(DrillbitEndpoint endpoint, BootStrapContext context, ClusterCoordinator coord,
            PersistentStoreProvider provider) {
        // PersistentStoreProvider is re-used for providing Query Profile Store as well
        this(endpoint, context, coord, provider, provider);
    }

    public DrillbitContext(DrillbitEndpoint endpoint, BootStrapContext context, ClusterCoordinator coord,
            PersistentStoreProvider provider, PersistentStoreProvider profileStoreProvider) {
        classpathScan = context.getClasspathScan();
        this.context = checkNotNull(context);
        this.coord = coord;
        this.endpoint = checkNotNull(endpoint);
        this.provider = provider;
        DrillConfig config = context.getConfig();
        lpPersistence = new LogicalPlanPersistence(config, classpathScan);

        storagePlugins = config.getInstance(StoragePluginRegistry.STORAGE_PLUGIN_REGISTRY_IMPL,
                StoragePluginRegistry.class, this);

        reader = new PhysicalPlanReader(config, classpathScan, lpPersistence, endpoint, storagePlugins);
        operatorCreatorRegistry = new OperatorCreatorRegistry(classpathScan);
        systemOptions = new SystemOptionManager(lpPersistence, provider, config, context.getDefinitions());
        functionRegistry = new FunctionImplementationRegistry(config, classpathScan, systemOptions);
        compiler = new CodeCompiler(config, systemOptions);

        // This operator table is built once and used for all queries which do not need dynamic UDF support.
        table = new DrillOperatorTable(functionRegistry, systemOptions);

        // This profile store context is built from the profileStoreProvider
        profileStoreContext = new QueryProfileStoreContext(context.getConfig(), profileStoreProvider, coord);
    }

    public QueryProfileStoreContext getProfileStoreContext() {
        return profileStoreContext;
    }

    public FunctionImplementationRegistry getFunctionImplementationRegistry() {
        return functionRegistry;
    }

    /**
     * @return the system options manager. It is important to note that this manager only contains options at the
     * "system" level and not "session" level.
     */
    public SystemOptionManager getOptionManager() {
        return systemOptions;
    }

    public DrillbitEndpoint getEndpoint() {
        return endpoint;
    }

    public DrillConfig getConfig() {
        return context.getConfig();
    }

    public Collection<DrillbitEndpoint> getAvailableBits() {
        return coord.getAvailableEndpoints();
    }

    public Collection<DrillbitEndpoint> getBits() {
        return coord.getOnlineEndPoints();
    }

    public BufferAllocator getAllocator() {
        return context.getAllocator();
    }

    public OperatorCreatorRegistry getOperatorCreatorRegistry() {
        return operatorCreatorRegistry;
    }

    public StoragePluginRegistry getStorage() {
        return this.storagePlugins;
    }

    public MetricRegistry getMetrics() {
        return context.getMetrics();
    }

    public PhysicalPlanReader getPlanReader() {
        return reader;
    }

    public PersistentStoreProvider getStoreProvider() {
        return provider;
    }

    public SchemaFactory getSchemaFactory() {
        return storagePlugins.getSchemaFactory();
    }

    public ClusterCoordinator getClusterCoordinator() {
        return coord;
    }

    public CodeCompiler getCompiler() {
        return compiler;
    }

    public ExecutorService getExecutor() {
        return context.getExecutor();
    }

    public ExecutorService getScanExecutor() {
        return context.getScanExecutor();
    }

    public ExecutorService getScanDecodeExecutor() {
        return context.getScanDecodeExecutor();
    }

    public LogicalPlanPersistence getLpPersistence() {
        return lpPersistence;
    }

    public ScanResult getClasspathScan() {
        return classpathScan;
    }

    public RemoteFunctionRegistry getRemoteFunctionRegistry() {
        return functionRegistry.getRemoteFunctionRegistry();
    }

    /**
     * Use the operator table built during startup when "exec.udf.use_dynamic" option
     * is set to false.
     * This operator table has standard SQL functions, operators and drill
     * built-in user defined functions (UDFs).
     * It does not include dynamic user defined functions (UDFs) that get added/removed
     * at run time.
     * This operator table is meant to be used for high throughput,
     * low latency operational queries, for which cost of building operator table is
     * high, both in terms of CPU and heap memory usage.
     *
     * @return - Operator table
     */
    public DrillOperatorTable getOperatorTable() {
        return table;
    }

    @Override
    public void close() throws Exception {
        getOptionManager().close();
        getFunctionImplementationRegistry().close();
        getRemoteFunctionRegistry().close();
        getCompiler().close();
    }
}
