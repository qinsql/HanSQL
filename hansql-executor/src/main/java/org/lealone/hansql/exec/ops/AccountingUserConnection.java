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
package org.lealone.hansql.exec.ops;

import org.lealone.hansql.exec.physical.impl.materialize.QueryWritableBatch;
import org.lealone.hansql.exec.record.RecordBatch;
import org.lealone.hansql.exec.session.UserClientConnection;

/**
 * Wrapper around a {@link UserClientConnection} that tracks the status of batches
 * sent to User.
 */
public class AccountingUserConnection {
    private final UserClientConnection connection;
    private final SendingAccountor sendingAccountor;

    public AccountingUserConnection(UserClientConnection connection, SendingAccountor sendingAccountor) {
        this.connection = connection;
        this.sendingAccountor = sendingAccountor;
    }

    public void sendData(QueryWritableBatch batch) {
        sendingAccountor.increment();
        connection.sendData(batch);
        if (connection.needsRawData()) {
            sendingAccountor.decrement();
        }
    }

    public boolean needsRawData() {
        return connection.needsRawData();
    }

    public void sendData(RecordBatch batch) {
        sendingAccountor.increment();
        connection.sendData(batch);
        if (connection.needsRawData()) {
            sendingAccountor.decrement();
        }
    }

    public UserClientConnection getConnection() {
        return connection;
    }
}
