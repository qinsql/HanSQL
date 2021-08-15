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
package org.lealone.hansql.exec.store.direct;

import com.fasterxml.jackson.annotation.JsonTypeName;

import org.lealone.hansql.common.exceptions.ExecutionSetupException;
import org.lealone.hansql.common.expression.SchemaPath;
import org.lealone.hansql.exec.physical.PhysicalOperatorSetupException;
import org.lealone.hansql.exec.physical.base.AbstractGroupScan;
import org.lealone.hansql.exec.physical.base.GroupScan;
import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.physical.base.ScanStats;
import org.lealone.hansql.exec.physical.base.SubScan;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.lealone.hansql.exec.store.RecordReader;

import java.util.List;

@JsonTypeName("direct-scan")
public class DirectGroupScan extends AbstractGroupScan {

  protected final RecordReader reader;
  protected final ScanStats stats;

  public DirectGroupScan(RecordReader reader) {
    this(reader, ScanStats.TRIVIAL_TABLE);
  }

  public DirectGroupScan(RecordReader reader, ScanStats stats) {
    super((String) null);
    this.reader = reader;
    this.stats = stats;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    assert endpoints.size() == 1;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    assert minorFragmentId == 0;
    return new DirectSubScan(reader);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public ScanStats getScanStats() {
    return stats;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return new DirectGroupScan(reader, stats);
  }

  @Override
  public String getDigest() {
    return String.valueOf(reader);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return this;
  }

}
