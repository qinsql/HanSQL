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
package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rex.RexBuilder;
import org.lealone.hansql.optimizer.rex.RexNode;


public class FilterPrel extends DrillFilterRelBase implements Prel {
  public FilterPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(Prel.DRILL_PHYSICAL, cluster, traits, child, condition);
  }

  @Override
  public org.lealone.hansql.optimizer.rel.core.Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new FilterPrel(getCluster(), traitSet, input, condition);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {

    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    Filter p = new Filter(childPOP, getFilterExpression(new DrillParseContext(PrelUtil.getSettings(getCluster()))), 1.0f);
    return creator.addMetadata(this, p);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.NONE_AND_TWO;
  }

  /**
   * FilterPrel adds an SV2 (TWO_BYTE mode SelectionVector).
   */
  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.TWO_BYTE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  @Override
  public Prel prepareForLateralUnnestPipeline(List<RelNode> children) {
    RexBuilder builder = this.getCluster().getRexBuilder();
    // right shift the previous field indices.
    return (Prel) this.copy(this.traitSet, children.get(0), DrillRelOptUtil.transformExpr(builder,
            condition, DrillRelOptUtil.rightShiftColsInRowType(this.getInput().getRowType())));
  }
}
