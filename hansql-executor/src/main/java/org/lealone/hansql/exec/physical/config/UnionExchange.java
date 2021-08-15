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
package org.lealone.hansql.exec.physical.config;

import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.lealone.hansql.exec.physical.PhysicalOperatorSetupException;
import org.lealone.hansql.exec.physical.base.AbstractExchange;
import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.physical.base.PhysicalOperatorUtil;
import org.lealone.hansql.exec.physical.base.Receiver;
import org.lealone.hansql.exec.physical.base.Sender;
import org.lealone.hansql.exec.planner.fragment.ParallelizationInfo;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("union-exchange")
public class UnionExchange extends AbstractExchange {

  public UnionExchange(@JsonProperty("child") PhysicalOperator child) {
    super(child);
  }

  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<DrillbitEndpoint> senderFragmentEndpoints) {
    Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0,
        "Sender fragment endpoint list should not be empty");

    return ParallelizationInfo.create(1, 1, getDefaultAffinityMap(senderFragmentEndpoints));
  }

  @Override
  public void setupSenders(List<DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    Preconditions.checkArgument(receiverLocations.size() == 1,
        "Union Exchange only supports a single receiver endpoint.");

    super.setupReceivers(receiverLocations);
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocations.get(0));
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new UnorderedReceiver(senderMajorFragmentId, PhysicalOperatorUtil.getIndexOrderedEndpoints(senderLocations), false);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new UnionExchange(child);
  }
}
