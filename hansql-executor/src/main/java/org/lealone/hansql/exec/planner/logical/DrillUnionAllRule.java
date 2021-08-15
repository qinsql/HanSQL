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

import java.util.ArrayList;
import java.util.List;

import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.InvalidRelException;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.logical.LogicalUnion;
import org.lealone.hansql.optimizer.util.trace.CalciteTrace;
import org.slf4j.Logger;

/**
 * Rule that converts a {@link LogicalUnion} to a {@link DrillUnionRelBase}, implemented by a "union" operation.
 */
public class DrillUnionAllRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillUnionAllRule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private DrillUnionAllRule() {
    super(RelOptHelper.any(LogicalUnion.class, Convention.NONE),
        DrillRelFactories.LOGICAL_BUILDER, "DrillUnionRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalUnion union = call.rel(0);

    // This rule applies to Union-All only
    if(!union.all) {
      return;
    }

    final RelTraitSet traits = union.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input : union.getInputs()) {
      final RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
      convertedInputs.add(convertedInput);
    }
    try {
      call.transformTo(new DrillUnionRel(union.getCluster(), traits, convertedInputs, union.all,
          true /* check compatibility */));
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }
}
