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

import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.physical.config.Limit;
import org.lealone.hansql.exec.physical.config.PartitionLimit;
import org.lealone.hansql.exec.planner.common.DrillLimitRelBase;
import org.lealone.hansql.exec.planner.common.DrillRelOptUtil;
import org.lealone.hansql.exec.planner.physical.visitor.PrelVisitor;
import org.lealone.hansql.exec.record.BatchSchema.SelectionVectorMode;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelWriter;
import org.lealone.hansql.optimizer.rex.RexLiteral;
import org.lealone.hansql.optimizer.rex.RexNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class LimitPrel extends DrillLimitRelBase implements Prel {
  private boolean isPartitioned = false;

  public LimitPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, offset, fetch);
  }

  public LimitPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch, boolean pushDown) {
    super(cluster, traitSet, child, offset, fetch, pushDown);
  }

  public LimitPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch, boolean pushDown, boolean isPartitioned) {
    super(cluster, traitSet, child, offset, fetch, pushDown);
    this.isPartitioned = isPartitioned;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LimitPrel(getCluster(), traitSet, sole(inputs), offset, fetch, isPushDown(), isPartitioned);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    int first = offset != null ? Math.max(0, RexLiteral.intValue(offset)) : 0;

    // Last offset to stop including into results (exclusive), translating fetch row counts into an offset.
    // Null value implies including entire remaining result set from first offset
    Integer last = fetch != null ? Math.max(0, RexLiteral.intValue(fetch)) + first : null;

    Limit limit;
    if (isPartitioned) {
      limit = new PartitionLimit(childPOP, first, last, DrillRelOptUtil.IMPLICIT_COLUMN);
    } else {
      limit = new Limit(childPOP, first, last);
    }
    return creator.addMetadata(this, limit);
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
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.itemIf("partitioned", isPartitioned, isPartitioned);
    return pw;
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.NONE_AND_TWO;
  }

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
    return new LimitPrel(this.getCluster(), this.traitSet, children.get(0), getOffset(), getFetch(), isPushDown(), true);
  }
}
