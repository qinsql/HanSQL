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

import org.lealone.hansql.common.config.DrillConfig;
import org.lealone.hansql.common.exceptions.DrillRuntimeException;
import org.lealone.hansql.exec.coord.ClusterCoordinator;
import org.lealone.hansql.exec.coord.store.TransientStore;
import org.lealone.hansql.exec.coord.store.TransientStoreConfig;
import org.lealone.hansql.exec.proto.SchemaUserBitShared;
import org.lealone.hansql.exec.proto.UserBitShared;
import org.lealone.hansql.exec.proto.UserBitShared.QueryInfo;
import org.lealone.hansql.exec.proto.UserBitShared.QueryProfile;
import org.lealone.hansql.exec.store.sys.PersistentStore;
import org.lealone.hansql.exec.store.sys.PersistentStoreConfig;
import org.lealone.hansql.exec.store.sys.PersistentStoreProvider;

public class QueryProfileStoreContext {

  private static final String PROFILES = "profiles";

  private static final String RUNNING = "running";

  private final PersistentStore<UserBitShared.QueryProfile> completedProfiles;

  private final TransientStore<UserBitShared.QueryInfo> runningProfiles;

  private final PersistentStoreConfig<QueryProfile> profileStoreConfig;

  public QueryProfileStoreContext(DrillConfig config, PersistentStoreProvider storeProvider,
                                  ClusterCoordinator coordinator) {
    profileStoreConfig = PersistentStoreConfig.newProtoBuilder(SchemaUserBitShared.QueryProfile.WRITE,
        SchemaUserBitShared.QueryProfile.MERGE)
        .name(PROFILES)
        .blob()
        .build();

    try {
      completedProfiles = storeProvider.getOrCreateStore(profileStoreConfig);
    } catch (final Exception e) {
      throw new DrillRuntimeException(e);
    }

    runningProfiles = coordinator.getOrCreateTransientStore(TransientStoreConfig
        .newProtoBuilder(SchemaUserBitShared.QueryInfo.WRITE, SchemaUserBitShared.QueryInfo.MERGE)
        .name(RUNNING)
        .build());
  }

  public PersistentStoreConfig<QueryProfile> getProfileStoreConfig() {
    return profileStoreConfig;
  }

  public PersistentStore<QueryProfile> getCompletedProfileStore() {
    return completedProfiles;
  }

  public TransientStore<QueryInfo> getRunningProfileStore() {
    return runningProfiles;
  }
}