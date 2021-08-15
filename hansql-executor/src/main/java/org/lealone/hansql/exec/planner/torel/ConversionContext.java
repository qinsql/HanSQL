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
package org.lealone.hansql.exec.planner.torel;

import java.util.List;
import java.util.Map;

import org.lealone.hansql.common.expression.LogicalExpression;
import org.lealone.hansql.common.logical.LogicalPlan;
import org.lealone.hansql.common.logical.data.Filter;
import org.lealone.hansql.common.logical.data.GroupingAggregate;
import org.lealone.hansql.common.logical.data.Join;
import org.lealone.hansql.common.logical.data.Limit;
import org.lealone.hansql.common.logical.data.LogicalOperator;
import org.lealone.hansql.common.logical.data.Order;
import org.lealone.hansql.common.logical.data.Project;
import org.lealone.hansql.common.logical.data.Scan;
import org.lealone.hansql.common.logical.data.Union;
import org.lealone.hansql.common.logical.data.visitors.AbstractLogicalVisitor;
import org.lealone.hansql.exec.planner.logical.DrillAggregateRel;
import org.lealone.hansql.exec.planner.logical.DrillJoinRel;
import org.lealone.hansql.exec.planner.logical.DrillLimitRel;
import org.lealone.hansql.exec.planner.logical.DrillRel;
import org.lealone.hansql.exec.planner.logical.DrillSortRel;
import org.lealone.hansql.exec.planner.logical.DrillUnionRel;
import org.lealone.hansql.exec.planner.logical.ScanFieldDeterminer;
import org.lealone.hansql.exec.planner.logical.ScanFieldDeterminer.FieldList;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptTable;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.plan.RelOptTable.ToRelContext;
import org.lealone.hansql.optimizer.rel.InvalidRelException;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelRoot;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeFactory;
import org.lealone.hansql.optimizer.rex.RexBuilder;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.schema.SchemaPlus;

public class ConversionContext implements ToRelContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConversionContext.class);

  private static final ConverterVisitor VISITOR = new ConverterVisitor();

  private final Map<Scan, FieldList> scanFieldLists;
  private final RelOptCluster cluster;

  public ConversionContext(RelOptCluster cluster, LogicalPlan plan) {
    super();
    scanFieldLists = ScanFieldDeterminer.getFieldLists(plan);
    this.cluster = cluster;
  }

  @Override
  public RelOptCluster getCluster() {
    return cluster;
  }


  private FieldList getFieldList(Scan scan) {
    assert scanFieldLists.containsKey(scan);
    return scanFieldLists.get(scan);
  }


  public RexBuilder getRexBuilder(){
    return cluster.getRexBuilder();
  }

  public RelTraitSet getLogicalTraits(){
    RelTraitSet set = RelTraitSet.createEmpty();
    set.add(DrillRel.DRILL_LOGICAL);
    return set;
  }

  public RelNode toRel(LogicalOperator operator) throws InvalidRelException{
    return operator.accept(VISITOR, this);
  }

  public RexNode toRex(LogicalExpression e){
    return null;
  }

  public RelDataTypeFactory getTypeFactory(){
    return cluster.getTypeFactory();
  }

  public RelOptTable getTable(Scan scan){
    FieldList list = getFieldList(scan);

    return null;
  }

  @Override
  public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
    throw new UnsupportedOperationException();
  }

  //@Override
  public RelRoot expandView(RelDataType rowType, String queryString, SchemaPlus rootSchema, List<String> schemaPath) {
    throw new UnsupportedOperationException();
  }

  private static class ConverterVisitor extends AbstractLogicalVisitor<RelNode, ConversionContext, InvalidRelException>{

    @Override
    public RelNode visitScan(Scan scan, ConversionContext context){
      //return BaseScanRel.convert(scan, context);
      return null;
    }

    @Override
    public RelNode visitFilter(Filter filter, ConversionContext context) throws InvalidRelException{
      //return BaseFilterRel.convert(filter, context);
      return null;
    }

    @Override
    public RelNode visitProject(Project project, ConversionContext context) throws InvalidRelException{
      //return BaseProjectRel.convert(project, context);
      return null;
    }

    @Override
    public RelNode visitOrder(Order order, ConversionContext context) throws InvalidRelException{
      return DrillSortRel.convert(order, context);
    }

    @Override
    public RelNode visitJoin(Join join, ConversionContext context) throws InvalidRelException{
      return DrillJoinRel.convert(join, context);
    }

    @Override
    public RelNode visitLimit(Limit limit, ConversionContext context) throws InvalidRelException{
      return DrillLimitRel.convert(limit, context);
    }

    @Override
    public RelNode visitUnion(Union union, ConversionContext context) throws InvalidRelException{
      return DrillUnionRel.convert(union, context);
    }

    @Override
    public RelNode visitGroupingAggregate(GroupingAggregate groupBy, ConversionContext context)
        throws InvalidRelException {
      return DrillAggregateRel.convert(groupBy, context);
    }

  }



}
