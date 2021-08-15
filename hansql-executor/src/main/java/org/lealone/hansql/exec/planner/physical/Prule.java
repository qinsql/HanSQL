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

import org.lealone.hansql.exec.planner.logical.DrillRelFactories;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.RelOptRuleOperand;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;

public abstract class Prule extends RelOptRule {
  public Prule(RelOptRuleOperand operand, String description) {
    super(operand, DrillRelFactories.LOGICAL_BUILDER, description);
  }

  public Prule(RelOptRuleOperand operand) {
    this(operand, null);
  }

  public static RelNode convert(RelNode rel, RelTraitSet toTraits) {
    toTraits = toTraits.simplify();

    PlannerSettings settings = PrelUtil.getSettings(rel.getCluster());
    if(settings.isSingleMode()){
      toTraits = toTraits.replace(DrillDistributionTrait.ANY);
    }

    return RelOptRule.convert(rel, toTraits.simplify());
  }

  public static boolean isSingleMode(RelOptRuleCall call) {
    return PrelUtil.getPlannerSettings(call.getPlanner()).isSingleMode();
  }
}
