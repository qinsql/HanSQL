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

import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.core.Uncollect;
import org.lealone.hansql.optimizer.rel.logical.LogicalProject;
import org.lealone.hansql.optimizer.rel.logical.LogicalValues;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.sql.SqlKind;

public class DrillUnnestRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillUnnestRule();

  private DrillUnnestRule() {
    super(RelOptHelper.some(Uncollect.class,
        RelOptHelper.some(LogicalProject.class, RelOptHelper.any(LogicalValues.class, Convention.NONE))),
        DrillRelFactories.LOGICAL_BUILDER, "DrillUnnestRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Uncollect uncollect = call.rel(0);
    final LogicalProject project = call.rel(1);

    RexNode projectedNode = project.getProjects().iterator().next();
    if (projectedNode.getKind() != SqlKind.FIELD_ACCESS) {
      return;
    }
    final RelTraitSet traits = uncollect.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    DrillUnnestRel unnest = new DrillUnnestRel(uncollect.getCluster(),
        traits, uncollect.getRowType(), projectedNode);
    call.transformTo(unnest);
  }
}
