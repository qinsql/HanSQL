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
package org.lealone.hansql.optimizer.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.lealone.hansql.optimizer.config.CalciteConnectionConfig;
import org.lealone.hansql.optimizer.config.CalciteSystemProperty;
import org.lealone.hansql.optimizer.plan.RelOptCostImpl;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptUtil;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.plan.hep.HepMatchOrder;
import org.lealone.hansql.optimizer.plan.hep.HepPlanner;
import org.lealone.hansql.optimizer.plan.hep.HepProgram;
import org.lealone.hansql.optimizer.plan.hep.HepProgramBuilder;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Calc;
import org.lealone.hansql.optimizer.rel.core.RelFactories;
import org.lealone.hansql.optimizer.rel.metadata.ChainedRelMetadataProvider;
import org.lealone.hansql.optimizer.rel.metadata.DefaultRelMetadataProvider;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataProvider;
import org.lealone.hansql.optimizer.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.lealone.hansql.optimizer.rel.rules.AggregateReduceFunctionsRule;
import org.lealone.hansql.optimizer.rel.rules.CalcMergeRule;
import org.lealone.hansql.optimizer.rel.rules.FilterAggregateTransposeRule;
import org.lealone.hansql.optimizer.rel.rules.FilterCalcMergeRule;
import org.lealone.hansql.optimizer.rel.rules.FilterJoinRule;
import org.lealone.hansql.optimizer.rel.rules.FilterProjectTransposeRule;
import org.lealone.hansql.optimizer.rel.rules.FilterToCalcRule;
import org.lealone.hansql.optimizer.rel.rules.JoinAssociateRule;
import org.lealone.hansql.optimizer.rel.rules.JoinCommuteRule;
import org.lealone.hansql.optimizer.rel.rules.JoinPushThroughJoinRule;
import org.lealone.hansql.optimizer.rel.rules.JoinToMultiJoinRule;
import org.lealone.hansql.optimizer.rel.rules.LoptOptimizeJoinRule;
import org.lealone.hansql.optimizer.rel.rules.MultiJoinOptimizeBushyRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectCalcMergeRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectMergeRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectToCalcRule;
import org.lealone.hansql.optimizer.rel.rules.SemiJoinRule;
import org.lealone.hansql.optimizer.rel.rules.SortProjectTransposeRule;
import org.lealone.hansql.optimizer.rel.rules.SubQueryRemoveRule;
import org.lealone.hansql.optimizer.rel.rules.TableScanRule;
import org.lealone.hansql.optimizer.sql2rel.RelDecorrelator;
import org.lealone.hansql.optimizer.sql2rel.RelFieldTrimmer;
import org.lealone.hansql.optimizer.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Utilities for creating {@link Program}s.
 */
public class Programs {
    public static final ImmutableList<RelOptRule> CALC_RULES = ImmutableList.of(CalcMergeRule.INSTANCE,
            FilterCalcMergeRule.INSTANCE, ProjectCalcMergeRule.INSTANCE, FilterToCalcRule.INSTANCE,
            ProjectToCalcRule.INSTANCE, CalcMergeRule.INSTANCE,

            // REVIEW jvs 9-Apr-2006: Do we still need these two? Doesn't the
            // combination of CalcMergeRule, FilterToCalcRule, and
            // ProjectToCalcRule have the same effect?
            FilterCalcMergeRule.INSTANCE, ProjectCalcMergeRule.INSTANCE);

    /** Program that converts filters and projects to {@link Calc}s. */
    public static final Program CALC_PROGRAM = calc(DefaultRelMetadataProvider.INSTANCE);

    /** Program that expands sub-queries. */
    public static final Program SUB_QUERY_PROGRAM = subQuery(DefaultRelMetadataProvider.INSTANCE);

    public static final ImmutableSet<RelOptRule> RULE_SET = ImmutableSet.of(SemiJoinRule.PROJECT, SemiJoinRule.JOIN,
            TableScanRule.INSTANCE,
            CalciteSystemProperty.COMMUTE.value() ? JoinAssociateRule.INSTANCE : ProjectMergeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE, FilterJoinRule.FILTER_ON_JOIN,
            AggregateExpandDistinctAggregatesRule.INSTANCE, AggregateReduceFunctionsRule.INSTANCE,
            FilterAggregateTransposeRule.INSTANCE, JoinCommuteRule.INSTANCE, JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT, SortProjectTransposeRule.INSTANCE);

    // private constructor for utility class
    private Programs() {
    }

    /** Creates a program that executes a rule set. */
    public static Program of(RuleSet ruleSet) {
        return new RuleSetProgram(ruleSet);
    }

    /** Creates a list of programs based on an array of rule sets. */
    public static List<Program> listOf(RuleSet... ruleSets) {
        return Lists.transform(Arrays.asList(ruleSets), Programs::of);
    }

    /** Creates a list of programs based on a list of rule sets. */
    public static List<Program> listOf(List<RuleSet> ruleSets) {
        return Lists.transform(ruleSets, Programs::of);
    }

    /** Creates a program from a list of rules. */
    public static Program ofRules(RelOptRule... rules) {
        return of(RuleSets.ofList(rules));
    }

    /** Creates a program from a list of rules. */
    public static Program ofRules(Iterable<? extends RelOptRule> rules) {
        return of(RuleSets.ofList(rules));
    }

    /** Creates a program that executes a sequence of programs. */
    public static Program sequence(Program... programs) {
        return new SequenceProgram(ImmutableList.copyOf(programs));
    }

    /** Creates a program that executes a list of rules in a HEP planner. */
    public static Program hep(Iterable<? extends RelOptRule> rules, boolean noDag,
            RelMetadataProvider metadataProvider) {
        final HepProgramBuilder builder = HepProgram.builder();
        for (RelOptRule rule : rules) {
            builder.addRuleInstance(rule);
        }
        return of(builder.build(), noDag, metadataProvider);
    }

    /** Creates a program that executes a {@link HepProgram}. */
    public static Program of(final HepProgram hepProgram, final boolean noDag,
            final RelMetadataProvider metadataProvider) {
        return (planner, rel, requiredOutputTraits) -> {
            final HepPlanner hepPlanner = new HepPlanner(hepProgram, null, noDag, null, RelOptCostImpl.FACTORY);

            List<RelMetadataProvider> list = new ArrayList<>();
            if (metadataProvider != null) {
                list.add(metadataProvider);
            }
            hepPlanner.registerMetadataProviders(list);
            RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of(list);
            rel.getCluster().setMetadataProvider(plannerChain);

            hepPlanner.setRoot(rel);
            return hepPlanner.findBestExp();
        };
    }

    /** Creates a program that invokes heuristic join-order optimization
     * (via {@link org.lealone.hansql.optimizer.rel.rules.JoinToMultiJoinRule},
     * {@link org.lealone.hansql.optimizer.rel.rules.MultiJoin} and
     * {@link org.lealone.hansql.optimizer.rel.rules.LoptOptimizeJoinRule})
     * if there are 6 or more joins (7 or more relations). */
    public static Program heuristicJoinOrder(final Iterable<? extends RelOptRule> rules, final boolean bushy,
            final int minJoinCount) {
        return (planner, rel, requiredOutputTraits) -> {
            final int joinCount = RelOptUtil.countJoins(rel);
            final Program program;
            if (joinCount < minJoinCount) {
                program = ofRules(rules);
            } else {
                // Create a program that gathers together joins as a MultiJoin.
                final HepProgram hep = new HepProgramBuilder().addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
                        .addMatchOrder(HepMatchOrder.BOTTOM_UP).addRuleInstance(JoinToMultiJoinRule.INSTANCE).build();
                final Program program1 = of(hep, false, DefaultRelMetadataProvider.INSTANCE);

                // Create a program that contains a rule to expand a MultiJoin
                // into heuristically ordered joins.
                // We use the rule set passed in, but remove JoinCommuteRule and
                // JoinPushThroughJoinRule, because they cause exhaustive search.
                final List<RelOptRule> list = Lists.newArrayList(rules);
                list.removeAll(ImmutableList.of(JoinCommuteRule.INSTANCE, JoinAssociateRule.INSTANCE,
                        JoinPushThroughJoinRule.LEFT, JoinPushThroughJoinRule.RIGHT));
                list.add(bushy ? MultiJoinOptimizeBushyRule.INSTANCE : LoptOptimizeJoinRule.INSTANCE);
                final Program program2 = ofRules(list);

                program = sequence(program1, program2);
            }
            return program.run(planner, rel, requiredOutputTraits);
        };
    }

    public static Program calc(RelMetadataProvider metadataProvider) {
        return hep(CALC_RULES, true, metadataProvider);
    }

    public static Program subQuery(RelMetadataProvider metadataProvider) {
        final HepProgramBuilder builder = HepProgram.builder();
        builder.addRuleCollection(ImmutableList.of((RelOptRule) SubQueryRemoveRule.FILTER, SubQueryRemoveRule.PROJECT,
                SubQueryRemoveRule.JOIN));
        return of(builder.build(), true, metadataProvider);
    }

    public static Program getProgram() {
        return (planner, rel, requiredOutputTraits) -> null;
    }

    /** Returns the standard program used by Prepare. */
    public static Program standard() {
        return standard(DefaultRelMetadataProvider.INSTANCE);
    }

    /** Returns the standard program with user metadata provider. */
    public static Program standard(RelMetadataProvider metadataProvider) {
        final Program program1 = (planner, rel, requiredOutputTraits) -> {
            planner.setRoot(rel);

            final RelNode rootRel2 = rel.getTraitSet().equals(requiredOutputTraits) ? rel
                    : planner.changeTraits(rel, requiredOutputTraits);
            assert rootRel2 != null;

            planner.setRoot(rootRel2);
            final RelOptPlanner planner2 = planner.chooseDelegate();
            final RelNode rootRel3 = planner2.findBestExp();
            assert rootRel3 != null : "could not implement exp";
            return rootRel3;
        };

        return sequence(subQuery(metadataProvider), new DecorrelateProgram(), new TrimFieldsProgram(), program1,

                // Second planner pass to do physical "tweaks". This the first time
                // that EnumerableCalcRel is introduced.
                calc(metadataProvider));
    }

    /** Program backed by a {@link RuleSet}. */
    static class RuleSetProgram implements Program {
        final RuleSet ruleSet;

        private RuleSetProgram(RuleSet ruleSet) {
            this.ruleSet = ruleSet;
        }

        @Override
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits) {
            planner.clear();
            for (RelOptRule rule : ruleSet) {
                planner.addRule(rule);
            }
            if (!rel.getTraitSet().equals(requiredOutputTraits)) {
                rel = planner.changeTraits(rel, requiredOutputTraits);
            }
            planner.setRoot(rel);
            return planner.findBestExp();

        }
    }

    /** Program that runs sub-programs, sending the output of the previous as
     * input to the next. */
    private static class SequenceProgram implements Program {
        private final ImmutableList<Program> programs;

        SequenceProgram(ImmutableList<Program> programs) {
            this.programs = programs;
        }

        @Override
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits) {
            for (Program program : programs) {
                rel = program.run(planner, rel, requiredOutputTraits);
            }
            return rel;
        }
    }

    /** Program that de-correlates a query.
     *
     * <p>To work around
     * <a href="https://issues.apache.org/jira/browse/CALCITE-842">[CALCITE-842]
     * Decorrelator gets field offsets confused if fields have been trimmed</a>,
     * disable field-trimming in {@link SqlToRelConverter}, and run
     * {@link TrimFieldsProgram} after this program. */
    private static class DecorrelateProgram implements Program {
        @Override
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits) {
            final CalciteConnectionConfig config = planner.getContext().unwrap(CalciteConnectionConfig.class);
            if (config != null && config.forceDecorrelate()) {
                final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
                return RelDecorrelator.decorrelateQuery(rel, relBuilder);
            }
            return rel;
        }
    }

    /** Program that trims fields. */
    private static class TrimFieldsProgram implements Program {
        @Override
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return new RelFieldTrimmer(null, relBuilder).trim(rel);
        }
    }
}

// End Programs.java
