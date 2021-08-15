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

import java.util.List;

import org.lealone.hansql.common.logical.data.Analyze;
import org.lealone.hansql.common.logical.data.LogicalOperator;
import org.lealone.hansql.exec.planner.cost.DrillCostBase;
import org.lealone.hansql.exec.planner.torel.ConversionContext;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.InvalidRelException;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.SingleRel;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;

/**
 * Drill logical node for "Analyze".
 */
public class DrillAnalyzeRel extends SingleRel implements DrillRel {

  double samplePercent;    // sampling percentage between 0-100

  public DrillAnalyzeRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, double samplePercent) {
    super(cluster, traits, child);
    this.samplePercent = samplePercent;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    final double dRows = mq.getRowCount(getInput());
    final double dCpu = dRows * DrillCostBase.COMPARE_CPU_COST;
    final double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillAnalyzeRel(getCluster(), traitSet, sole(inputs), samplePercent);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator inputOp = implementor.visitChild(this, 0, getInput());
    final Analyze rel = new Analyze();
    rel.setInput(inputOp);
    return rel;
  }

  public double getSamplePercent() {
    return samplePercent;
  }

  public static DrillAnalyzeRel convert(Analyze analyze, ConversionContext context)
      throws InvalidRelException {
    RelNode input = context.toRel(analyze.getInput());
    return new DrillAnalyzeRel(context.getCluster(), context.getLogicalTraits(), input, analyze.getSamplePercent());
  }
}
