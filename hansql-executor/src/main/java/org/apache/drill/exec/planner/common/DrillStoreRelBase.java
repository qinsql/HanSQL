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
package org.apache.drill.exec.planner.common;

import java.util.List;

import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptTable;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.TableModify;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.schema.CatalogReader;

/**
 * Base class for logical and physical Store implemented in Drill
 */
public abstract class DrillStoreRelBase extends TableModify implements DrillRelNode {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStoreRelBase.class);

  protected DrillStoreRelBase(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, CatalogReader catalogReader, RelNode child,
                              Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened);

  }

}
