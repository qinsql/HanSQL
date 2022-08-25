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
package org.lealone.hansql.engine;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.tools.ToolProvider;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluginManager;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.hansql.common.AutoCloseables;
import org.lealone.hansql.common.concurrent.ExtendedLatch;
import org.lealone.hansql.common.config.DrillConfig;
import org.lealone.hansql.common.exceptions.ExecutionSetupException;
import org.lealone.hansql.common.map.CaseInsensitiveMap;
import org.lealone.hansql.common.scanner.ClassPathScanner;
import org.lealone.hansql.common.scanner.persistence.ScanResult;
import org.lealone.hansql.common.util.DrillVersionInfo;
import org.lealone.hansql.engine.server.HanSQLServer;
import org.lealone.hansql.engine.sql.HanSQLEngine;
import org.lealone.hansql.engine.storage.LealoneScanSpec;
import org.lealone.hansql.engine.storage.LealoneStoragePlugin;
import org.lealone.hansql.engine.storage.LealoneStoragePluginConfig;
import org.lealone.hansql.engine.storage.LealoneTable;
import org.lealone.hansql.exec.ExecConstants;
import org.lealone.hansql.exec.SqlExecutor;
import org.lealone.hansql.exec.context.BootStrapContext;
import org.lealone.hansql.exec.context.DrillbitContext;
import org.lealone.hansql.exec.context.options.OptionDefinition;
import org.lealone.hansql.exec.context.options.OptionManager;
import org.lealone.hansql.exec.context.options.OptionValue;
import org.lealone.hansql.exec.context.options.OptionValue.OptionScope;
import org.lealone.hansql.exec.context.options.SystemOptionManager;
import org.lealone.hansql.exec.coord.ClusterCoordinator;
import org.lealone.hansql.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.lealone.hansql.exec.coord.local.LocalClusterCoordinator;
import org.lealone.hansql.exec.exception.DrillbitStartupException;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint.State;
import org.lealone.hansql.exec.proto.UserBitShared.QueryId;
import org.lealone.hansql.exec.proto.UserProtos;
import org.lealone.hansql.exec.session.UserClientConnection;
import org.lealone.hansql.exec.store.SchemaTreeProvider;
import org.lealone.hansql.exec.store.StoragePluginRegistry;
import org.lealone.hansql.exec.store.sys.PersistentStoreProvider;
import org.lealone.hansql.exec.store.sys.store.provider.CachingPersistentStoreProvider;
import org.lealone.hansql.exec.store.sys.store.provider.InMemoryStoreProvider;
import org.lealone.hansql.exec.store.sys.store.provider.LocalPersistentStoreProvider;
import org.lealone.hansql.optimizer.schema.CalciteSchema;
import org.lealone.hansql.optimizer.schema.SchemaPlus;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.sql.parser.SqlParseException;
import org.lealone.hansql.optimizer.sql.parser.SqlParser;
import org.lealone.server.ProtocolServerEngine;

/**
 * Starts, tracks and stops all the required services for a Drillbit daemon to work.
 */
public class HanEngine implements AutoCloseable {

    public static SqlNode parse(String sql) throws SqlParseException {
        SqlParser.Config config = SqlParser.configBuilder()
                .setUnquotedCasing(org.lealone.hansql.optimizer.util.Casing.TO_LOWER).build();
        return parse(sql, config);
    }

    public static SqlNode parse(String sql, SqlParser.Config config) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(sql, config);
        return sqlParser.parseQuery();
    }

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HanEngine.class);

    static {
        // Environment.logEnv("Drillbit environment: ", logger);
    }

    private final static String SYSTEM_OPTIONS_NAME = "org.apache.drill.exec.server.Drillbit.system_options";

    private final ClusterCoordinator coord;
    private final PersistentStoreProvider storeProvider;
    private final BootStrapContext context;
    private final int gracePeriod;

    private final DrillConfig config;
    private DrillbitContext dContext;
    private final Executor executor;

    private String hostName;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public DrillbitContext getDrillbitContext() {
        return dContext;
    }

    public DrillConfig getConfig() {
        return config;
    }

    public RegistrationHandle getRegistrationHandle() {
        return registrationHandle;
    }

    public StoragePluginRegistry getStoragePluginRegistry() {
        return storageRegistry;
    }

    private RegistrationHandle registrationHandle;
    private volatile StoragePluginRegistry storageRegistry;
    private final PersistentStoreProvider profileStoreProvider;

    public HanEngine(DrillConfig config) throws Exception {
        this(config, SystemOptionManager.createDefaultOptionDefinitions(),
                ClassPathScanner.fromPrescan(config));
    }

    public HanEngine(DrillConfig config, CaseInsensitiveMap<OptionDefinition> definitions,
            ScanResult classpathScan) throws Exception {
        this.config = config;
        // Must start up with access to JDK Compiler
        if (ToolProvider.getSystemJavaCompiler() == null) {
            throw new DrillbitStartupException(
                    "JDK Java compiler not available. Ensure Drill is running with the java executable from a JDK and not a JRE");
        }

        gracePeriod = config.getInt(ExecConstants.GRACE_PERIOD);
        final Stopwatch w = Stopwatch.createStarted();
        logger.debug("Construction started.");
        context = new BootStrapContext(config, definitions, classpathScan);
        executor = context.getExecutor();

        coord = new LocalClusterCoordinator();
        storeProvider = new CachingPersistentStoreProvider(new LocalPersistentStoreProvider(config));

        // Check if InMemory Profile Store, else use Default Store Provider
        if (config.getBoolean(ExecConstants.PROFILES_STORE_INMEMORY)) {
            profileStoreProvider = new InMemoryStoreProvider(
                    config.getInt(ExecConstants.PROFILES_STORE_CAPACITY));
            logger.info("Upto {} latest query profiles will be retained in-memory",
                    config.getInt(ExecConstants.PROFILES_STORE_CAPACITY));
        } else {
            profileStoreProvider = storeProvider;
        }

        logger.info("Construction completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    }

    public void run() throws Exception {
        final Stopwatch w = Stopwatch.createStarted();
        logger.debug("Startup begun.");
        coord.start(10000);
        storeProvider.start();
        if (profileStoreProvider != storeProvider) {
            profileStoreProvider.start();
        }
        DrillbitEndpoint endpoint = DrillbitEndpoint.newBuilder().setAddress(hostName)
                .setVersion(DrillVersionInfo.getVersion()).setState(State.STARTUP).build();
        dContext = new DrillbitContext(endpoint, context, coord, storeProvider, profileStoreProvider);
        storageRegistry = dContext.getStorage();
        storageRegistry.init();
        dContext.getOptionManager().init();
        javaPropertiesToSystemOptions();
        dContext.getRemoteFunctionRegistry().init(context.getConfig(), storeProvider, coord);
        registrationHandle = coord.register(endpoint);
        logger.info("Startup completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    }

    // Wait uninterruptibly
    public void waitForGracePeriod() {
        ExtendedLatch exitLatch = new ExtendedLatch();
        exitLatch.awaitUninterruptibly(gracePeriod);
    }

    private void updateState(State state) {
        if (registrationHandle != null) {
            coord.update(registrationHandle, state);
        }
    }

    public void shutdown() {
        this.close();
    }

    /*
     The drillbit is moved into Quiescent state and the drillbit waits for grace period amount of time.
     Then drillbit moves into draining state and waits for all the queries and fragments to complete.
     */
    @Override
    public synchronized void close() {
        final Stopwatch w = Stopwatch.createStarted();
        logger.debug("Shutdown begun.");
        updateState(State.QUIESCENT);
        waitForGracePeriod();
        // safe to exit
        updateState(State.OFFLINE);
        if (coord != null && registrationHandle != null) {
            coord.unregister(registrationHandle);
        }
        try {
            AutoCloseables.close(storeProvider, coord, storageRegistry, context);

            // Closing the profile store provider if distinct
            if (storeProvider != profileStoreProvider) {
                AutoCloseables.close(profileStoreProvider);
            }
        } catch (Exception e) {
            logger.warn("Failure on close()", e);
        }

        logger.info("Shutdown completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    }

    private void javaPropertiesToSystemOptions() {
        // get the system options property
        final String allSystemProps = System.getProperty(SYSTEM_OPTIONS_NAME);
        if ((allSystemProps == null) || allSystemProps.isEmpty()) {
            return;
        }

        final SystemOptionManager optionManager = dContext.getOptionManager();

        // parse out the properties, validate, and then set them
        final String systemProps[] = allSystemProps.split(",");
        for (final String systemProp : systemProps) {
            final String keyValue[] = systemProp.split("=");
            if (keyValue.length != 2) {
                throwInvalidSystemOption(systemProp, "does not contain a key=value assignment");
            }

            final String optionName = keyValue[0].trim();
            if (optionName.isEmpty()) {
                throwInvalidSystemOption(systemProp, "does not contain a key before the assignment");
            }

            final String optionString = stripQuotes(keyValue[1].trim(), systemProp);
            if (optionString.isEmpty()) {
                throwInvalidSystemOption(systemProp, "does not contain a value after the assignment");
            }

            final OptionValue defaultValue = optionManager.getOption(optionName);

            if (defaultValue == null) {
                throwInvalidSystemOption(systemProp, "does not specify a valid option name");
            }

            if (!defaultValue.accessibleScopes.inScopeOf(OptionScope.SYSTEM)) {
                throwInvalidSystemOption(systemProp, "does not specify a SYSTEM option ");
            }

            optionManager.setLocalOption(defaultValue.kind, optionName, optionString);
        }
    }

    private static void throwInvalidSystemOption(final String systemProp, final String errorMessage) {
        throw new IllegalStateException("Property \"" + SYSTEM_OPTIONS_NAME + "\" part \"" + systemProp
                + "\" " + errorMessage + ".");
    }

    private static String stripQuotes(final String s, final String systemProp) {
        if (s.isEmpty()) {
            return s;
        }

        final char cFirst = s.charAt(0);
        final char cLast = s.charAt(s.length() - 1);
        if ((cFirst == '"') || (cFirst == '\'')) {
            if (cLast != cFirst) {
                throwInvalidSystemOption(systemProp, "quoted value does not have closing quote");
            }

            return s.substring(1, s.length() - 2); // strip the quotes
        }

        if ((cLast == '"') || (cLast == '\'')) {
            throwInvalidSystemOption(systemProp, "value has unbalanced closing quote");
        }

        // return as-is
        return s;
    }

    /**
    * Helper method to generate QueryId
    * @return generated QueryId
    */
    private static QueryId queryIdGenerator() {
        ThreadLocalRandom r = ThreadLocalRandom.current();

        // create a new queryid where the first four bytes are a growing time (each new value comes earlier in
        // sequence). Last 12 bytes are random.
        long time = (int) (System.currentTimeMillis() / 1000);
        long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
        long p2 = r.nextLong();
        QueryId id = QueryId.newBuilder().setPart1(p1).setPart2(p2).build();
        return id;
    }

    public void submitWork(UserClientConnection connection, String sql) {
        SqlExecutor sqlExecutor = createSqlExecutor(connection, sql);
        executor.execute(sqlExecutor);
    }

    public SqlExecutor createSqlExecutor(UserClientConnection connection, String sql) {
        UserProtos.RunQuery runQuery = UserProtos.RunQuery.newBuilder().setPlan(sql)
                .setType(org.lealone.hansql.exec.proto.UserBitShared.QueryType.SQL).build();
        QueryId id = queryIdGenerator();
        return new SqlExecutor(executor, dContext, connection, id, runQuery);
    }

    public OptionManager getOptionManager() {
        return dContext.getOptionManager();
    }

    public SchemaPlus getRootSchema(ServerSession session, String sql, boolean useDefaultSchema,
            boolean isOlap) {
        if (isOlap) {
            LealoneStoragePlugin lsp;
            try {
                lsp = (LealoneStoragePlugin) getStoragePluginRegistry()
                        .getPlugin(LealoneStoragePluginConfig.NAME);
            } catch (ExecutionSetupException e) {
                throw DbException.throwInternalError();
            }
            SchemaPlus parent = CalciteSchema.createRootSchema(false, true, "").plus();
            SchemaPlus defaultSchema = CalciteSchema.createRootSchema(false, true, Constants.SCHEMA_MAIN)
                    .plus();
            String dbName = session.getDatabase().getShortName();
            Database db = LealoneDatabase.getInstance().getDatabase(dbName);
            for (Schema schema : db.getAllSchemas()) {
                final String schemaName = schema.getName();
                final SchemaPlus subSchema;
                if (schemaName.equalsIgnoreCase(Constants.SCHEMA_MAIN)) {
                    subSchema = defaultSchema;
                } else {
                    subSchema = CalciteSchema.createRootSchema(false, true, schemaName).plus();
                }
                for (Table table : schema.getAllTablesAndViews()) {
                    LealoneTable t = new LealoneTable(table, lsp,
                            new LealoneScanSpec(dbName, schemaName, table.getName()));
                    subSchema.add(table.getName().toUpperCase(), t);
                    subSchema.add(table.getName().toLowerCase(), t);
                }
                parent.add(schemaName, subSchema);
            }
            if (session.getCurrentSchemaName().equalsIgnoreCase(Constants.SCHEMA_MAIN))
                return defaultSchema;
            return parent;
        }
        SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(getDrillbitContext());
        SchemaPlus rootSchema = schemaTreeProvider.createRootSchema(getOptionManager());
        if (useDefaultSchema && (isOlap || sql.contains(LealoneStoragePluginConfig.NAME))) {
            LealoneStoragePlugin lsp;
            try {
                lsp = (LealoneStoragePlugin) getStoragePluginRegistry()
                        .getPlugin(LealoneStoragePluginConfig.NAME);
            } catch (ExecutionSetupException e) {
                throw DbException.throwInternalError();
            }

            SchemaPlus defaultSchema = CalciteSchema.createRootSchema(false, true, Constants.SCHEMA_MAIN)
                    .plus();
            String dbName = session.getDatabase().getShortName();
            SchemaPlus schema = CalciteSchema.createRootSchema(defaultSchema, false, true, dbName)
                    .plus();
            lsp.registerSchema(schema, dbName, defaultSchema);
            rootSchema.add(LealoneStoragePluginConfig.NAME, defaultSchema);
            rootSchema.add("", defaultSchema);
        }
        return rootSchema;
    }

    public static HanEngine getInstance() {
        return ((HanSQLServer) PluginManager.getPlugin(ProtocolServerEngine.class, HanSQLEngine.NAME)
                .getProtocolServer()).getHanEngine();
    }
}
