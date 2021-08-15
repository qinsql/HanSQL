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

import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.logical.LogicalCorrelate;
import org.lealone.hansql.optimizer.util.trace.CalciteTrace;
import org.slf4j.Logger;

public class DrillCorrelateRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillCorrelateRule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private DrillCorrelateRule() {
    super(RelOptHelper.any(LogicalCorrelate.class, Convention.NONE),
        DrillRelFactories.LOGICAL_BUILDER,
        "DrillLateralJoinRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalCorrelate correlate = call.rel(0);
    final RelNode left = correlate.getLeft();
    final RelNode right = correlate.getRight();
    final RelNode convertedLeft = convert(left, left.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
    final RelNode convertedRight = convert(right, right.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());

    final RelTraitSet traits = correlate.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    DrillLateralJoinRel lateralJoinRel = new DrillLateralJoinRel(correlate.getCluster(),
        traits, convertedLeft, convertedRight, false, correlate.getCorrelationId(),
        correlate.getRequiredColumns(), correlate.getJoinType());
    call.transformTo(lateralJoinRel);
  }
}
