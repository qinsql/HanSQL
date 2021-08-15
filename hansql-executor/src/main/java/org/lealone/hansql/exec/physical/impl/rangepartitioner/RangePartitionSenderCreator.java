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
package org.lealone.hansql.exec.physical.impl.rangepartitioner;

import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.lealone.hansql.common.exceptions.ExecutionSetupException;
import org.lealone.hansql.exec.ops.ExecutorFragmentContext;
import org.lealone.hansql.exec.physical.config.HashPartitionSender;
import org.lealone.hansql.exec.physical.config.RangePartitionSender;
import org.lealone.hansql.exec.physical.impl.RootCreator;
import org.lealone.hansql.exec.physical.impl.RootExec;
import org.lealone.hansql.exec.physical.impl.partitionsender.PartitionSenderRootExec;
import org.lealone.hansql.exec.record.RecordBatch;

public class RangePartitionSenderCreator implements RootCreator<RangePartitionSender> {

  @Override
  public RootExec getRoot(ExecutorFragmentContext context, RangePartitionSender config,
                          List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);
    RangePartitionRecordBatch rangep = new RangePartitionRecordBatch(config, children.iterator().next(), context);
    HashPartitionSender hpc = new HashPartitionSender(config.getOppositeMajorFragmentId(), config,
        config.getPartitionFunction().getPartitionFieldRef(), config.getDestinations(),
        RangePartitionSender.RANGE_PARTITION_OUTGOING_BATCH_SIZE);
    return new PartitionSenderRootExec(context, rangep, hpc);
  }

}
