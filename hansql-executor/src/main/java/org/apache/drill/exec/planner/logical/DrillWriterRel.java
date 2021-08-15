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
package org.apache.drill.exec.planner.logical;

import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeField;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Writer;
import org.apache.drill.exec.planner.common.DrillWriterRelBase;

public class DrillWriterRel extends DrillWriterRelBase implements DrillRel {

  private final List<Integer> partitionKeys;

  public DrillWriterRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, CreateTableEntry createTableEntry) {
    super(DRILL_LOGICAL, cluster, traitSet, input, createTableEntry);
    setRowType();
    this.partitionKeys = resolvePartitionKeys();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillWriterRel(getCluster(), traitSet, sole(inputs), getCreateTableEntry());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());
    return Writer
        .builder()
        .setInput(childOp)
        .setCreateTableEntry(getCreateTableEntry())
        .build();
  }


  private List<Integer> resolvePartitionKeys(){
    final List<Integer> keys = Lists.newArrayList();
    final RelDataType inputRowType = getInput().getRowType();
    final List<String> partitionCol = getCreateTableEntry().getPartitionColumns();

    for (final String col : partitionCol) {
      final RelDataTypeField field = inputRowType.getField(col, false, false);
      Preconditions.checkArgument(field != null,
          String.format("partition col %s could not be resolved in table's column lists!", col));
      keys.add(field.getIndex());
    }

    return keys;
  }

  public List<Integer> getPartitionKeys() {
    return this.partitionKeys;
  }


}
