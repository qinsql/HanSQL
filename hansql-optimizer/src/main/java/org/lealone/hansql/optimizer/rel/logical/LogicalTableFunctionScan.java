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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelInput;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.TableFunctionScan;
import org.lealone.hansql.optimizer.rel.metadata.RelColumnMapping;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rex.RexNode;

/**
 * Sub-class of {@link org.lealone.hansql.optimizer.rel.core.TableFunctionScan}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalTableFunctionScan extends TableFunctionScan {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>LogicalTableFunctionScan</code>.
   *
   * @param cluster        Cluster that this relational expression belongs to
   * @param inputs         0 or more relational inputs
   * @param rexCall        function invocation expression
   * @param elementType    element type of the collection that will implement
   *                       this table
   * @param rowType        row type produced by function
   * @param columnMappings column mappings associated with this function
   */
  public LogicalTableFunctionScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    super(cluster, traitSet, inputs, rexCall, elementType, rowType,
        columnMappings);
  }

  @Deprecated // to be removed before 2.0
  public LogicalTableFunctionScan(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    this(cluster, cluster.traitSetOf(Convention.NONE), inputs, rexCall,
        elementType, rowType, columnMappings);
  }

  /**
   * Creates a LogicalTableFunctionScan by parsing serialized output.
   */
  public LogicalTableFunctionScan(RelInput input) {
    super(input);
  }

  /** Creates a LogicalTableFunctionScan. */
  public static LogicalTableFunctionScan create(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableFunctionScan(cluster, traitSet, inputs, rexCall,
        elementType, rowType, columnMappings);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalTableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType,
      RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalTableFunctionScan(
        getCluster(),
        traitSet,
        inputs,
        rexCall,
        elementType,
        rowType,
        columnMappings);
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // REVIEW jvs 8-Jan-2006:  what is supposed to be here
    // for an abstract rel?
    return planner.getCostFactory().makeHugeCost();
  }
}

// End LogicalTableFunctionScan.java
