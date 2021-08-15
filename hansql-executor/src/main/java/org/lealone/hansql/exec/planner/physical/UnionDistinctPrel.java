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

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.physical.config.UnionAll;
import org.lealone.hansql.exec.planner.cost.DrillCostBase;
import org.lealone.hansql.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.lealone.hansql.exec.record.BatchSchema.SelectionVectorMode;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.InvalidRelException;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Union;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;

public class UnionDistinctPrel extends UnionPrel {

  public UnionDistinctPrel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
      boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, false /* all = false */, checkCompatibility);
  }


  @Override
  public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    try {
      return new UnionDistinctPrel(this.getCluster(), traitSet, inputs,
          false /* don't check compatibility during copy */);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    double totalInputRowCount = 0;
    for (int i = 0; i < this.getInputs().size(); i++) {
      totalInputRowCount += mq.getRowCount(this.getInputs().get(i));
    }

    double cpuCost = totalInputRowCount * DrillCostBase.BASE_CPU_COST;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(totalInputRowCount, cpuCost, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    List<PhysicalOperator> inputPops = Lists.newArrayList();

    for (int i = 0; i < this.getInputs().size(); i++) {
      inputPops.add( ((Prel)this.getInputs().get(i)).getPhysicalOperator(creator));
    }

    ///TODO: change this to UnionDistinct once implemented end-to-end..
    UnionAll unionAll = new UnionAll(inputPops);
    return creator.addMetadata(this, unionAll);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
