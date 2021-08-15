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
package org.apache.drill.exec.planner.physical;


import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.RelFactories;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.rex.RexUtil;

import java.util.List;

public class PrelFactories {
  public static final RelFactories.ProjectFactory PROJECT_FACTORY =
      new DrillProjectPrelFactory();

  /**
   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
   * {@link org.lealone.hansql.optimizer.rel.logical.LogicalProject}.
   */
  private static class DrillProjectPrelFactory implements RelFactories.ProjectFactory {
    @Override
    public RelNode createProject(RelNode child,
                                 List<? extends RexNode> childExprs, List<String> fieldNames) {
      final RelOptCluster cluster = child.getCluster();
      final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames);
      final RelNode project = new ProjectPrel(cluster, child.getTraitSet().plus(Prel.DRILL_PHYSICAL),
          child, Lists.newArrayList(childExprs), rowType);

      return project;
    }
  }
}
