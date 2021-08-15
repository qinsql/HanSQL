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
package org.lealone.hansql.exec.planner.common;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.lealone.hansql.exec.ExecConstants;
import org.lealone.hansql.exec.planner.cost.DrillCostBase;
import org.lealone.hansql.exec.planner.physical.PrelUtil;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Correlate;
import org.lealone.hansql.optimizer.rel.core.CorrelationId;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeField;
import org.lealone.hansql.optimizer.sql.SemiJoinType;
import org.lealone.hansql.optimizer.sql.validate.SqlValidatorUtil;
import org.lealone.hansql.optimizer.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;


public abstract class DrillLateralJoinRelBase extends Correlate implements DrillRelNode {

  final public static String IMPLICIT_COLUMN = DrillRelOptUtil.IMPLICIT_COLUMN;

  final private static double CORRELATE_MEM_COPY_COST = DrillCostBase.MEMORY_TO_CPU_RATIO * DrillCostBase.BASE_CPU_COST;
  final public boolean excludeCorrelateColumn;
  public DrillLateralJoinRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, boolean excludeCorrelateCol,
                               CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType semiJoinType) {
    super(cluster, traits, left, right, correlationId, requiredColumns, semiJoinType);
    this.excludeCorrelateColumn = excludeCorrelateCol;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    DrillCostBase.DrillCostFactory costFactory = (DrillCostBase.DrillCostFactory) planner.getCostFactory();

    double rowCount = estimateRowCount(mq);
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
        .getLong(ExecConstants.AVERAGE_FIELD_WIDTH_KEY);

    double rowSize = left.getRowType().getFieldList().size() * fieldWidth;

    double cpuCost = rowCount * rowSize * DrillCostBase.BASE_CPU_COST;
    double memCost = !excludeCorrelateColumn ? CORRELATE_MEM_COPY_COST : 0.0;
    return costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost);
  }

  @Override
  protected RelDataType deriveRowType() {
    switch (joinType) {
      case LEFT:
      case INNER:
        return constructRowType(SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
          removeImplicitField(right.getRowType()), joinType.toJoinType(),
          getCluster().getTypeFactory(), null,
          ImmutableList.of()));
      case ANTI:
      case SEMI:
        return constructRowType(left.getRowType());
      default:
        throw new IllegalStateException("Unknown join type " + joinType);
    }
  }

  /**
   * Returns number of fields in {@link RelDataType} for
   * input rel node with specified ordinal considering value of
   * {@code excludeCorrelateColumn}.
   *
   * @param ordinal ordinal of input rel node
   * @return number of fields in input's {@link RelDataType}
   */
  public int getInputSize(int ordinal) {
    if (this.excludeCorrelateColumn && ordinal == 0) {
      return getInput(ordinal).getRowType().getFieldList().size() - 1;
    }
    return getInput(ordinal).getRowType().getFieldList().size();
  }

  public RelDataType constructRowType(RelDataType inputRowType) {
    Preconditions.checkArgument(this.requiredColumns.cardinality() == 1);

    List<RelDataType> fields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    if (excludeCorrelateColumn) {
      int corrVariable = this.requiredColumns.nextSetBit(0);

      for (RelDataTypeField field : inputRowType.getFieldList()) {
        if (field.getIndex() == corrVariable) {
          continue;
        }
        fieldNames.add(field.getName());
        fields.add(field.getType());
      }

      return getCluster().getTypeFactory().createStructType(fields, fieldNames);
    }
    return inputRowType;
  }

  public RelDataType removeImplicitField(RelDataType inputRowType) {
    List<RelDataType> fields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    for (RelDataTypeField field : inputRowType.getFieldList()) {
      if (field.getName().equals(IMPLICIT_COLUMN)) {
        continue;
      }
      fieldNames.add(field.getName());
      fields.add(field.getType());
    }

    return getCluster().getTypeFactory().createStructType(fields, fieldNames);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(left);
  }
}
