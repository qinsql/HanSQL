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
package org.lealone.hansql.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.physical.config.HashToMergeExchange;
import org.lealone.hansql.exec.planner.cost.DrillCostBase;
import org.lealone.hansql.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.lealone.hansql.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.lealone.hansql.exec.record.BatchSchema.SelectionVectorMode;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelCollation;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;

public class HashToMergeExchangePrel extends ExchangePrel {

  private final List<DistributionField> distFields;
  private int numEndPoints = 0;
  private final RelCollation collation;

  public HashToMergeExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                 List<DistributionField> fields,
                                 RelCollation collation,
                                 int numEndPoints) {
    super(cluster, traitSet, input);
    this.distFields = fields;
    this.collation = collation;
    this.numEndPoints = numEndPoints;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);

    int rowWidth = child.getRowType().getFieldCount() * DrillCostBase.AVG_FIELD_WIDTH;
    double hashCpuCost = DrillCostBase.HASH_CPU_COST * inputRows * distFields.size();
    double svrCpuCost = DrillCostBase.SVR_CPU_COST * inputRows;
    double mergeCpuCost = DrillCostBase.COMPARE_CPU_COST * inputRows * (Math.log(numEndPoints) / Math.log(2));
    double networkCost = DrillCostBase.BYTE_NETWORK_COST * inputRows * rowWidth;
    DrillCostFactory costFactory = (DrillCostFactory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, hashCpuCost + svrCpuCost + mergeCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToMergeExchangePrel(getCluster(), traitSet, sole(inputs), distFields,
        this.collation, numEndPoints);
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    if (PrelUtil.getSettings(getCluster()).isSingleMode()) {
      return childPOP;
    }

    HashToMergeExchange g = new HashToMergeExchange(childPOP,
        HashPrelUtil.getHashExpression(this.distFields, getInput().getRowType()),
        PrelUtil.getOrdering(this.collation, getInput().getRowType()));
    return creator.addMetadata(this, g);

  }

  public RelCollation getCollation() {
    return this.collation;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
