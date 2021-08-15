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
import org.lealone.hansql.optimizer.plan.RelOptTable;
import org.lealone.hansql.optimizer.plan.ViewExpanders;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.RelFactories;
import org.lealone.hansql.optimizer.rel.logical.LogicalTableScan;
import org.lealone.hansql.optimizer.tools.RelBuilderFactory;

/**
 * Planner rule that converts a
 * {@link org.lealone.hansql.optimizer.rel.logical.LogicalTableScan} to the result
 * of calling {@link RelOptTable#toRel}.
 */
public class TableScanRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final TableScanRule INSTANCE =
      new TableScanRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a TableScanRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public TableScanRule(RelBuilderFactory relBuilderFactory) {
    super(operand(LogicalTableScan.class, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalTableScan oldRel = call.rel(0);
    RelNode newRel =
        oldRel.getTable().toRel(
            ViewExpanders.simpleContext(oldRel.getCluster()));
    call.transformTo(newRel);
  }
}

// End TableScanRule.java
