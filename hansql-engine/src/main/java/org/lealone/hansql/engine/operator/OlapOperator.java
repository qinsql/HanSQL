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
package org.lealone.hansql.engine.operator;

import org.lealone.db.result.LocalResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.hansql.engine.HanEngine;
import org.lealone.hansql.engine.server.HanClientConnection;
import org.lealone.hansql.exec.SqlExecutor;
import org.lealone.hansql.optimizer.schema.SchemaPlus;
import org.lealone.net.NetNode;
import org.lealone.sql.operator.Operator;
import org.lealone.sql.query.Select;

public class OlapOperator implements Operator {

    private Select select;
    private LocalResult localResult;
    private SqlExecutor sqlExecutor;

    public OlapOperator(Select select, LocalResult localResult) {
        this.select = select;
        this.localResult = localResult;
    }

    @Override
    public void start() {
        ServerSession session = select.getSession();
        String sql = select.getSQL();
        HanEngine hanEngine = HanEngine.getInstance();
        SchemaPlus rootSchema = hanEngine.getRootSchema(session, sql, true, true);
        HanClientConnection clientConnection = new HanClientConnection(rootSchema, session, hanEngine,
                NetNode.getLocalTcpNode().getInetSocketAddress(), localResult, res -> {
                    session.setStatus(SessionStatus.STATEMENT_COMPLETED);
                    session.getTransactionListener().wakeUp();
                });
        clientConnection.setCursor(select.getTableFilter().getCursor());
        sqlExecutor = hanEngine.createSqlExecutor(clientConnection, sql);
        sqlExecutor.start();
    }

    @Override
    public void run() {
        sqlExecutor.yieldableRun();
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isStopped() {
        return sqlExecutor.isStopped();
    }

    @Override
    public LocalResult getLocalResult() {
        return localResult;
    }
}
