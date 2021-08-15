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
package org.lealone.hansql.exec.planner.logical;

import org.lealone.hansql.common.logical.data.LateralJoin;
import org.lealone.hansql.common.logical.data.LogicalOperator;
import org.lealone.hansql.exec.planner.common.DrillLateralJoinRelBase;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Correlate;
import org.lealone.hansql.optimizer.rel.core.CorrelationId;
import org.lealone.hansql.optimizer.sql.SemiJoinType;
import org.lealone.hansql.optimizer.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;


public class DrillLateralJoinRel extends DrillLateralJoinRelBase implements DrillRel {

  protected DrillLateralJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, boolean includeCorrelateVar,
                                CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType semiJoinType) {
    super(cluster, traits, left, right, includeCorrelateVar, correlationId, requiredColumns, semiJoinType);
  }

  @Override
  public Correlate copy(RelTraitSet traitSet,
        RelNode left, RelNode right, CorrelationId correlationId,
        ImmutableBitSet requiredColumns, SemiJoinType joinType) {
    return new DrillLateralJoinRel(this.getCluster(), this.getTraitSet(), left, right, this.excludeCorrelateColumn, correlationId, requiredColumns,
        this.getJoinType());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    List<String> fields = new ArrayList<>();
    fields.addAll(getInput(0).getRowType().getFieldNames());
    fields.addAll(getInput(1).getRowType().getFieldNames());
    assert DrillJoinRel.isUnique(fields);
    final int leftCount = getInputSize(0);

    final LogicalOperator leftOp = DrillJoinRel.implementInput(implementor, 0, 0, left, this, fields);
    final LogicalOperator rightOp = DrillJoinRel.implementInput(implementor, 1, leftCount, right, this, fields);

    return new LateralJoin(leftOp, rightOp);
  }
}
