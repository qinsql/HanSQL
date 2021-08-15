/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.engine.server;

import java.net.SocketAddress;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.hansql.engine.HanEngine;
import org.lealone.hansql.exec.physical.impl.materialize.QueryWritableBatch;
import org.lealone.hansql.exec.proto.UserBitShared.QueryResult;
import org.lealone.hansql.exec.proto.UserBitShared.UserCredentials;
import org.lealone.hansql.exec.record.RecordBatch;
import org.lealone.hansql.exec.session.UserSession;
import org.lealone.hansql.optimizer.schema.SchemaPlus;

public class HanClientConnection implements org.lealone.hansql.exec.session.UserClientConnection {

    private final HanBatchResult batchResult = new HanBatchResult();
    private final ServerSession serverSession;
    private final UserSession session;
    private final SocketAddress remoteAddress;
    private final AsyncHandler<AsyncResult<Result>> asyncHandler;

    public HanClientConnection(SchemaPlus schema, ServerSession serverSession, HanEngine engine,
            SocketAddress remoteAddress, AsyncHandler<AsyncResult<Result>> asyncHandler) {
        this.serverSession = serverSession;
        session = UserSession.Builder.newBuilder()
                .withCredentials(UserCredentials.newBuilder().setUserName(serverSession.getUser().getName()).build())
                .withOptionManager(engine.getOptionManager())
                // .withUserProperties(inbound.getProperties())
                // .setSupportComplexTypes(inbound.getSupportComplexTypes())
                .build();
        session.setDefaultSchema(schema);
        this.remoteAddress = remoteAddress;
        this.asyncHandler = asyncHandler;
    }

    public ServerSession getServerSession() {
        return serverSession;
    }

    @Override
    public UserSession getSession() {
        return session;
    }

    @Override
    public void sendResult(QueryResult result) {
        // logger.info("sendResult");
        AsyncResult<Result> ar = new AsyncResult<>();
        if (result != null && result.getQueryState() == QueryResult.QueryState.FAILED) {
            ar.setCause(new RuntimeException(result.getErrorList().get(0).getMessage()));
        } else {
            ar.setResult(batchResult);
        }
        asyncHandler.handle(ar);
    }

    @Override
    public void sendData(QueryWritableBatch result) {
        throw new UnsupportedOperationException("sendData");
    }

    @Override
    public boolean needsRawData() {
        return true;
    }

    @Override
    public void sendData(RecordBatch data) {
        HanResult result = new HanResult(data);
        batchResult.addResult(result);
    }

    public org.lealone.db.result.Result getResult() {
        return batchResult;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }
}
