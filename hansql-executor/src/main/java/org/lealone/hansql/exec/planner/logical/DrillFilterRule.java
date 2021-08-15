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
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.logical.LogicalFilter;

/**
 * Rule that converts a {@link org.lealone.hansql.optimizer.rel.logical.LogicalFilter} to a Drill "filter" operation.
 */
public class DrillFilterRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillFilterRule();

  private DrillFilterRule() {
    super(RelOptHelper.any(LogicalFilter.class, Convention.NONE),
        DrillRelFactories.LOGICAL_BUILDER, "DrillFilterRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final RelNode input = filter.getInput();
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
    call.transformTo(new DrillFilterRel(
        filter.getCluster(), convertedInput.getTraitSet(),
        convertedInput, filter.getCondition()));
  }
}
