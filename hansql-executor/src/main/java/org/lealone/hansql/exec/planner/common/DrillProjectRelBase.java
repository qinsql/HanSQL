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
package org.lealone.hansql.exec.planner.common;

import java.util.HashMap;
import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.common.expression.FieldReference;
import org.lealone.hansql.common.expression.LogicalExpression;
import org.lealone.hansql.common.logical.data.NamedExpression;
import org.lealone.hansql.exec.planner.StarColumnHelper;
import org.lealone.hansql.exec.planner.cost.DrillCostBase;
import org.lealone.hansql.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.lealone.hansql.exec.planner.logical.DrillOptiq;
import org.lealone.hansql.exec.planner.logical.DrillParseContext;
import org.lealone.hansql.exec.planner.physical.PrelUtil;
import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Project;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rex.RexCall;
import org.lealone.hansql.optimizer.rex.RexCorrelVariable;
import org.lealone.hansql.optimizer.rex.RexDynamicParam;
import org.lealone.hansql.optimizer.rex.RexFieldAccess;
import org.lealone.hansql.optimizer.rex.RexInputRef;
import org.lealone.hansql.optimizer.rex.RexLiteral;
import org.lealone.hansql.optimizer.rex.RexLocalRef;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.rex.RexOver;
import org.lealone.hansql.optimizer.rex.RexRangeRef;
import org.lealone.hansql.optimizer.rex.RexVisitorImpl;
import org.lealone.hansql.optimizer.sql.fun.SqlStdOperatorTable;
import org.lealone.hansql.optimizer.sql.type.SqlTypeName;
import org.lealone.hansql.optimizer.util.Pair;

/**
 *
 * Base class for logical and physical Project implemented in Drill
 */
public abstract class DrillProjectRelBase extends Project implements DrillRelNode {
  private final int nonSimpleFieldCount;

  protected DrillProjectRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelNode child, List<? extends RexNode> exps,
      RelDataType rowType) {
    super(cluster, traits, child, exps, rowType);
    assert getConvention() == convention;
    nonSimpleFieldCount = this.getRowType().getFieldCount() - getSimpleFieldCount();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    double rowCount = mq.getRowCount(this);
    // Attribute small cost for projecting simple fields. In reality projecting simple columns in not free and
    // this allows projection pushdown/project-merge rules to kick-in thereby eliminating unneeded columns from
    // the projection.
    double cpuCost = DrillCostBase.PROJECT_CPU_COST * rowCount * nonSimpleFieldCount
        + (this.getRowType().getFieldCount() - nonSimpleFieldCount) * rowCount * DrillCostBase.BASE_CPU_COST;

    DrillCostFactory costFactory = (DrillCostFactory) planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, 0, 0);
  }

  private List<Pair<RexNode, String>> projects() {
    return Pair.zip(exps, getRowType().getFieldNames());
  }

  protected List<NamedExpression> getProjectExpressions(DrillParseContext context) {
    List<NamedExpression> expressions = Lists.newArrayList();

    HashMap<String, String> starColPrefixes = new HashMap<>();

    // T1.* will subsume T1.*0, but will not subsume any regular column/expression.
    // Select *, col1, *, col2 : the intermediate will output one set of regular columns expanded from star with prefix,
    // plus col1 and col2 without prefix.
    // This will allow us to differentiate the regular expanded from *, and the regular column referenced in the query.
    for (Pair<RexNode, String> pair : projects()) {
      if (StarColumnHelper.isPrefixedStarColumn(pair.right)) {
        String prefix = StarColumnHelper.extractStarColumnPrefix(pair.right);

        if (! starColPrefixes.containsKey(prefix)) {
          starColPrefixes.put(prefix, pair.right);
        }
      }
    }

    for (Pair<RexNode, String> pair : projects()) {
      if (! StarColumnHelper.subsumeColumn(starColPrefixes, pair.right)) {
        LogicalExpression expr = DrillOptiq.toDrill(context, getInput(), pair.left);
        expressions.add(new NamedExpression(expr, FieldReference.getWithQuotedRef(pair.right)));
      }
    }
    return expressions;
  }

  private int getSimpleFieldCount() {
    int cnt = 0;

    final ComplexFieldWithNamedSegmentIdentifier complexFieldIdentifer = new ComplexFieldWithNamedSegmentIdentifier();
    // SimpleField, either column name, or complex field reference with only named segment ==> no array segment
    // a, a.b.c are simple fields.
    // a[1].b.c, a.b[1], a.b.c[1] are not simple fields, since they all contain array segment.
    //  a + b, a * 10 + b, etc are not simple fields, since they are expressions.
    for (RexNode expr : this.getProjects()) {
      if (expr instanceof RexInputRef) {
        // Simple Field reference.
        cnt++;
      } else if (expr instanceof RexCall && expr.accept(complexFieldIdentifer)) {
        // Complex field with named segments only.
        cnt++;
      }
    }
    return cnt;
  }

  private static class ComplexFieldWithNamedSegmentIdentifier extends RexVisitorImpl<Boolean> {
    protected ComplexFieldWithNamedSegmentIdentifier() {
      super(true);
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return true;
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
      return doUnknown(localRef);
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
      return doUnknown(literal);
    }

    @Override
    public Boolean visitOver(RexOver over) {
      return doUnknown(over);
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return doUnknown(correlVariable);
    }

    @Override
    public Boolean visitCall(RexCall call) {
      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);

        if (op0 instanceof RexInputRef &&
            op1 instanceof RexLiteral && ((RexLiteral) op1).getTypeName() == SqlTypeName.CHAR) {
          return true;
        } else if (op0 instanceof RexCall &&
            op1 instanceof RexLiteral && ((RexLiteral) op1).getTypeName() == SqlTypeName.CHAR) {
          return op0.accept(this);
        }
      }

      return false;
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return doUnknown(dynamicParam);
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return doUnknown(rangeRef);
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return doUnknown(fieldAccess);
    }

    private boolean doUnknown(Object o) {
      return false;
    }
  }

}
