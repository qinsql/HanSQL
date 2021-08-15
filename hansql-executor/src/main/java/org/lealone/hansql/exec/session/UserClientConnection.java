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
package org.lealone.hansql.exec.session;

import java.net.SocketAddress;

import org.lealone.hansql.exec.physical.impl.materialize.QueryWritableBatch;
import org.lealone.hansql.exec.proto.UserBitShared.QueryResult;
import org.lealone.hansql.exec.record.RecordBatch;

/**
 * Interface for getting user session properties and interacting with user connection. Separating this interface from
 * {@link AbstractRemoteConnection} implementation for user connection:
 * <p><ul>
 * <li> Connection is passed to Foreman and Screen operators. Instead passing this interface exposes few details.
 * <li> Makes it easy to have wrappers around user connection which can be helpful to tap the messages and data
 * going to the actual client.
 * </ul>
 */
public interface UserClientConnection {
    /**
     * @return User session object.
     */
    UserSession getSession();

    /**
     * Send query result outcome to client. Outcome is returned through <code>listener</code>
     *
     * @param listener
     * @param result
     */
    void sendResult(QueryResult result);

    /**
     * Send query data to client. Outcome is returned through <code>listener</code>
     *
     * @param listener
     * @param result
     */
    void sendData(QueryWritableBatch result);

    /**
     * @return Return the client node address.
     */
    SocketAddress getRemoteAddress();

    default boolean needsRawData() {
        return false;
    }

    default void sendData(RecordBatch result) {
    }
}
