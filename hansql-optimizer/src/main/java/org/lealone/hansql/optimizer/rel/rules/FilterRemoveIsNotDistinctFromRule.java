/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.optimizer.rel.rules;

import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.RelOptUtil;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Filter;
import org.lealone.hansql.optimizer.rel.core.RelFactories;
import org.lealone.hansql.optimizer.rex.RexBuilder;
import org.lealone.hansql.optimizer.rex.RexCall;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.rex.RexShuttle;
import org.lealone.hansql.optimizer.rex.RexUtil;
import org.lealone.hansql.optimizer.sql.fun.SqlStdOperatorTable;
import org.lealone.hansql.optimizer.tools.RelBuilder;
import org.lealone.hansql.optimizer.tools.RelBuilderFactory;

/**
 * Planner rule that replaces {@code IS NOT DISTINCT FROM}
 * in a {@link Filter} with logically equivalent operations.
 *
 * @see org.lealone.hansql.optimizer.sql.fun.SqlStdOperatorTable#IS_NOT_DISTINCT_FROM
 */
public final class FilterRemoveIsNotDistinctFromRule extends RelOptRule {
    // ~ Static fields/initializers ---------------------------------------------

    /** The singleton. */
    public static final FilterRemoveIsNotDistinctFromRule INSTANCE = new FilterRemoveIsNotDistinctFromRule(
            RelFactories.LOGICAL_BUILDER);

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a FilterRemoveIsNotDistinctFromRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public FilterRemoveIsNotDistinctFromRule(RelBuilderFactory relBuilderFactory) {
        super(operand(Filter.class, any()), relBuilderFactory, null);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter oldFilter = call.rel(0);
        RexNode oldFilterCond = oldFilter.getCondition();

        if (RexUtil.findOperatorCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, oldFilterCond) == null) {
            // no longer contains isNotDistinctFromOperator
            return;
        }

        // Now replace all the "a isNotDistinctFrom b"
        // with the RexNode given by RelOptUtil.isDistinctFrom() method

        RemoveIsNotDistinctFromRexShuttle rewriteShuttle = new RemoveIsNotDistinctFromRexShuttle(
                oldFilter.getCluster().getRexBuilder());

        final RelBuilder relBuilder = call.builder();
        final RelNode newFilterRel = relBuilder.push(oldFilter.getInput()).filter(oldFilterCond.accept(rewriteShuttle))
                .build();

        call.transformTo(newFilterRel);
    }

    // ~ Inner Classes ----------------------------------------------------------

    /** Shuttle that removes 'x IS NOT DISTINCT FROM y' and converts it
     * to 'CASE WHEN x IS NULL THEN y IS NULL WHEN y IS NULL THEN x IS
     * NULL ELSE x = y END'. */
    private class RemoveIsNotDistinctFromRexShuttle extends RexShuttle {
        RexBuilder rexBuilder;

        RemoveIsNotDistinctFromRexShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        // override RexShuttle
        @Override
        public RexNode visitCall(RexCall call) {
            RexNode newCall = super.visitCall(call);

            if (call.getOperator() == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
                RexCall tmpCall = (RexCall) newCall;
                newCall = RelOptUtil.isDistinctFrom(rexBuilder, tmpCall.getOperands().get(0),
                        tmpCall.getOperands().get(1), true);
            }
            return newCall;
        }
    }
}

// End FilterRemoveIsNotDistinctFromRule.java
