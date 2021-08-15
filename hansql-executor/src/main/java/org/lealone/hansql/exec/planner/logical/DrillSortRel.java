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

import java.util.List;
import java.util.Map;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.lealone.hansql.common.expression.FieldReference;
import org.lealone.hansql.common.logical.data.LogicalOperator;
import org.lealone.hansql.common.logical.data.Order;
import org.lealone.hansql.common.logical.data.Order.Ordering;
import org.lealone.hansql.exec.planner.common.OrderedRel;
import org.lealone.hansql.exec.planner.torel.ConversionContext;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.InvalidRelException;
import org.lealone.hansql.optimizer.rel.RelCollation;
import org.lealone.hansql.optimizer.rel.RelCollations;
import org.lealone.hansql.optimizer.rel.RelFieldCollation;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Sort;
import org.lealone.hansql.optimizer.rex.RexNode;

/**
 * Sort implemented in Drill.
 */
public class DrillSortRel extends Sort implements DrillRel,OrderedRel {

  /** Creates a DrillSortRel. */
  public DrillSortRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation) {
    super(cluster, traits, input, collation);
  }

  /** Creates a DrillSortRel with offset and fetch. */
  public DrillSortRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
  }

  @Override
  public DrillSortRel copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    return new DrillSortRel(getCluster(), traitSet, input, collation, offset, fetch);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final Order.Builder builder = Order.builder();
    builder.setInput(implementor.visitChild(this, 0, getInput()));

    final List<String> childFields = getInput().getRowType().getFieldNames();
    for(RelFieldCollation fieldCollation : this.collation.getFieldCollations()){
      builder.addOrdering(fieldCollation.getDirection(),
          new FieldReference(childFields.get(fieldCollation.getFieldIndex())),
          fieldCollation.nullDirection);
    }
    return builder.build();
  }


  public static RelNode convert(Order order, ConversionContext context) throws InvalidRelException{

    // if there are compound expressions in the order by, we need to convert into projects on either side.
    RelNode input = context.toRel(order.getInput());
    List<String> fields = input.getRowType().getFieldNames();

    // build a map of field names to indices.
    Map<String, Integer> fieldMap = Maps.newHashMap();
    int i =0;
    for(String field : fields){
      fieldMap.put(field, i);
      i++;
    }

    List<RelFieldCollation> collations = Lists.newArrayList();

    for(Ordering o : order.getOrderings()){
      String fieldName = ExprHelper.getFieldName(o.getExpr());
      int fieldId = fieldMap.get(fieldName);
      RelFieldCollation c = new RelFieldCollation(fieldId, o.getDirection(), o.getNullDirection());
      collations.add(c);
    }
    return new DrillSortRel(context.getCluster(), context.getLogicalTraits(), input, RelCollations.of(collations));
  }

  @Override
  public RexNode getOffset() {
    return offset;
  }

  @Override
  public RexNode getFetch() {
    return fetch;
  }

  @Override
  public boolean canBeDropped() {
    return true;
  }
}
