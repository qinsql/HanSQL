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
package org.lealone.hansql.optimizer.rel.logical;

import java.util.List;
import java.util.Set;

import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptUtil;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelCollation;
import org.lealone.hansql.optimizer.rel.RelCollationTraitDef;
import org.lealone.hansql.optimizer.rel.RelDistributionTraitDef;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Calc;
import org.lealone.hansql.optimizer.rel.core.CorrelationId;
import org.lealone.hansql.optimizer.rel.metadata.RelMdCollation;
import org.lealone.hansql.optimizer.rel.metadata.RelMdDistribution;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;
import org.lealone.hansql.optimizer.rel.rules.FilterToCalcRule;
import org.lealone.hansql.optimizer.rel.rules.ProjectToCalcRule;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.rex.RexProgram;
import org.lealone.hansql.optimizer.util.Util;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link LogicalProject} and {@link LogicalFilter}.
 * It should be created in the later
 * stages of optimization, by merging consecutive {@link LogicalProject} and
 * {@link LogicalFilter} nodes together.
 *
 * <p>The following rules relate to <code>LogicalCalc</code>:</p>
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link LogicalFilter}
 * <li>{@link ProjectToCalcRule} creates this from a {@link LogicalFilter}
 * <li>{@link org.lealone.hansql.optimizer.rel.rules.FilterCalcMergeRule}
 *     merges this with a {@link LogicalFilter}
 * <li>{@link org.lealone.hansql.optimizer.rel.rules.ProjectCalcMergeRule}
 *     merges this with a {@link LogicalProject}
 * <li>{@link org.lealone.hansql.optimizer.rel.rules.CalcMergeRule}
 *     merges two {@code LogicalCalc}s
 * </ul>
 */
public final class LogicalCalc extends Calc {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /** Creates a LogicalCalc. */
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program) {
    super(cluster, traitSet, child, program);
  }

  @Deprecated // to be removed before 2.0
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList) {
    this(cluster, traitSet, child, program);
    Util.discard(collationList);
  }

  public static LogicalCalc create(final RelNode input,
      final RexProgram program) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet()
        .replace(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.calc(mq, input, program))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.calc(mq, input, program));
    return new LogicalCalc(cluster, traitSet, input, program);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalCalc copy(RelTraitSet traitSet, RelNode child,
      RexProgram program) {
    return new LogicalCalc(getCluster(), traitSet, child, program);
  }

  @Override public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    final RelOptUtil.VariableUsedVisitor vuv =
        new RelOptUtil.VariableUsedVisitor(null);
    for (RexNode expr : program.getExprList()) {
      expr.accept(vuv);
    }
    variableSet.addAll(vuv.variables);
  }
}

// End LogicalCalc.java
