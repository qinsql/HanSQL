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

import org.lealone.hansql.common.logical.data.LogicalOperator;
import org.lealone.hansql.common.logical.data.Store;
import org.lealone.hansql.exec.planner.common.DrillScreenRelBase;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DrillScreenRel extends DrillScreenRelBase implements DrillRel {
  private static final Logger logger = LoggerFactory.getLogger(DrillScreenRel.class);

  public DrillScreenRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(DRILL_LOGICAL, cluster, traitSet, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillScreenRel(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());
    return Store.builder().setInput(childOp).storageEngine("--SCREEN--").build();
  }

}
