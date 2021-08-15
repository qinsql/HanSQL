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
package org.lealone.hansql.exec.planner;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.lealone.hansql.exec.planner.logical.DrillConditions;
import org.lealone.hansql.exec.planner.logical.DrillRelFactories;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.volcano.AbstractConverter;
import org.lealone.hansql.optimizer.rel.core.Aggregate;
import org.lealone.hansql.optimizer.rel.core.Join;
import org.lealone.hansql.optimizer.rel.core.Project;
import org.lealone.hansql.optimizer.rel.logical.LogicalAggregate;
import org.lealone.hansql.optimizer.rel.logical.LogicalCalc;
import org.lealone.hansql.optimizer.rel.logical.LogicalJoin;
import org.lealone.hansql.optimizer.rel.logical.LogicalProject;
import org.lealone.hansql.optimizer.rel.logical.LogicalUnion;
import org.lealone.hansql.optimizer.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.lealone.hansql.optimizer.rel.rules.AggregateRemoveRule;
import org.lealone.hansql.optimizer.rel.rules.FilterCorrelateRule;
import org.lealone.hansql.optimizer.rel.rules.FilterMergeRule;
import org.lealone.hansql.optimizer.rel.rules.FilterRemoveIsNotDistinctFromRule;
import org.lealone.hansql.optimizer.rel.rules.FilterSetOpTransposeRule;
import org.lealone.hansql.optimizer.rel.rules.JoinPushExpressionsRule;
import org.lealone.hansql.optimizer.rel.rules.JoinPushThroughJoinRule;
import org.lealone.hansql.optimizer.rel.rules.JoinPushTransitivePredicatesRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectRemoveRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectSetOpTransposeRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectToWindowRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectWindowTransposeRule;
import org.lealone.hansql.optimizer.rel.rules.ReduceExpressionsRule;
import org.lealone.hansql.optimizer.rel.rules.SemiJoinRule;
import org.lealone.hansql.optimizer.rel.rules.SortRemoveRule;
import org.lealone.hansql.optimizer.rel.rules.SubQueryRemoveRule;
import org.lealone.hansql.optimizer.rel.rules.UnionToDistinctRule;

/**
 * Contains rule instances which use custom RelBuilder.
 */
public interface RuleInstance {

  ReduceExpressionsRule PROJECT_INSTANCE =
      new ReduceExpressionsRule.ProjectReduceExpressionsRule(LogicalProject.class, true,
          DrillRelFactories.LOGICAL_BUILDER);

  UnionToDistinctRule UNION_TO_DISTINCT_RULE =
      new UnionToDistinctRule(LogicalUnion.class,
          DrillRelFactories.LOGICAL_BUILDER);

  SemiJoinRule SEMI_JOIN_PROJECT_RULE = new SemiJoinRule.ProjectToSemiJoinRule(Project.class, Join.class, Aggregate.class,
          DrillRelFactories.LOGICAL_BUILDER, "DrillSemiJoinRule:project") {
    public boolean matches(RelOptRuleCall call) {
      Preconditions.checkArgument(call.rel(1) instanceof Join);
      Join join = call.rel(1);
      return !(join.getCondition().isAlwaysTrue() || join.getCondition().isAlwaysFalse());
    }
  };

  JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_RULE =
      new JoinPushExpressionsRule(Join.class,
          DrillRelFactories.LOGICAL_BUILDER);

  FilterMergeRule FILTER_MERGE_RULE =
      new FilterMergeRule(DrillRelFactories.LOGICAL_BUILDER);

  FilterMergeRule DRILL_FILTER_MERGE_RULE =
      new FilterMergeRule(DrillRelBuilder.proto(DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY));

  FilterCorrelateRule FILTER_CORRELATE_RULE =
      new FilterCorrelateRule(DrillRelFactories.LOGICAL_BUILDER);

  AggregateRemoveRule AGGREGATE_REMOVE_RULE =
      new AggregateRemoveRule(LogicalAggregate.class, DrillRelFactories.LOGICAL_BUILDER);

  AggregateExpandDistinctAggregatesRule AGGREGATE_EXPAND_DISTINCT_AGGREGATES_RULE =
      new AggregateExpandDistinctAggregatesRule(LogicalAggregate.class, false,
          DrillRelFactories.LOGICAL_BUILDER);

  /**
   * Instance of the rule that works on logical joins only, and pushes to the
   * right.
   */
  RelOptRule JOIN_PUSH_THROUGH_JOIN_RULE_RIGHT =
      new JoinPushThroughJoinRule("JoinPushThroughJoinRule:right", true,
          LogicalJoin.class, DrillRelFactories.LOGICAL_BUILDER);

  /**
   * Instance of the rule that works on logical joins only, and pushes to the
   * left.
   */
  RelOptRule JOIN_PUSH_THROUGH_JOIN_RULE_LEFT =
      new JoinPushThroughJoinRule("JoinPushThroughJoinRule:left", false,
          LogicalJoin.class, DrillRelFactories.LOGICAL_BUILDER);

  ReduceExpressionsRule CALC_INSTANCE =
      new ReduceExpressionsRule.CalcReduceExpressionsRule(LogicalCalc.class, true,
          DrillRelFactories.LOGICAL_BUILDER);

  FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE_RULE =
      new FilterSetOpTransposeRule(DrillRelFactories.LOGICAL_BUILDER);

  ProjectSetOpTransposeRule PROJECT_SET_OP_TRANSPOSE_RULE =
      new ProjectSetOpTransposeRule(DrillConditions.PRESERVE_ITEM, DrillRelFactories.LOGICAL_BUILDER);

  ProjectRemoveRule PROJECT_REMOVE_RULE =
      new ProjectRemoveRule(DrillRelFactories.LOGICAL_BUILDER);

  ProjectToWindowRule PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW_RULE =
      new ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule(DrillRelFactories.LOGICAL_BUILDER);

  SortRemoveRule SORT_REMOVE_RULE =
      new SortRemoveRule(DrillRelFactories.LOGICAL_BUILDER);

  ProjectWindowTransposeRule PROJECT_WINDOW_TRANSPOSE_RULE =
      new ProjectWindowTransposeRule(DrillRelFactories.LOGICAL_BUILDER);

  AbstractConverter.ExpandConversionRule EXPAND_CONVERSION_RULE =
      new AbstractConverter.ExpandConversionRule(DrillRelFactories.LOGICAL_BUILDER);

  /**
   * Instance of the rule that infers predicates from on a
   * {@link org.lealone.hansql.optimizer.rel.core.Join} and creates
   * {@link org.lealone.hansql.optimizer.rel.core.Filter}s if those predicates can be pushed
   * to its inputs.
   */
  JoinPushTransitivePredicatesRule DRILL_JOIN_PUSH_TRANSITIVE_PREDICATES_RULE =
      new JoinPushTransitivePredicatesRule(Join.class, DrillRelBuilder.proto(
          DrillRelFactories.DRILL_LOGICAL_JOIN_FACTORY, DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY));

  FilterRemoveIsNotDistinctFromRule REMOVE_IS_NOT_DISTINCT_FROM_RULE =
      new FilterRemoveIsNotDistinctFromRule(DrillRelBuilder.proto(DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY));

  SubQueryRemoveRule SUB_QUERY_FILTER_REMOVE_RULE =
      new SubQueryRemoveRule.SubQueryFilterRemoveRule(DrillRelFactories.LOGICAL_BUILDER);

  SubQueryRemoveRule SUB_QUERY_PROJECT_REMOVE_RULE =
      new SubQueryRemoveRule.SubQueryProjectRemoveRule(DrillRelFactories.LOGICAL_BUILDER);

  SubQueryRemoveRule SUB_QUERY_JOIN_REMOVE_RULE =
      new SubQueryRemoveRule.SubQueryJoinRemoveRule(DrillRelFactories.LOGICAL_BUILDER);
}
