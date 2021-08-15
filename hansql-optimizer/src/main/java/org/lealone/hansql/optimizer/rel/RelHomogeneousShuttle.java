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
package org.lealone.hansql.optimizer.rel;

import org.lealone.hansql.optimizer.rel.core.TableFunctionScan;
import org.lealone.hansql.optimizer.rel.core.TableScan;
import org.lealone.hansql.optimizer.rel.logical.LogicalAggregate;
import org.lealone.hansql.optimizer.rel.logical.LogicalCorrelate;
import org.lealone.hansql.optimizer.rel.logical.LogicalExchange;
import org.lealone.hansql.optimizer.rel.logical.LogicalFilter;
import org.lealone.hansql.optimizer.rel.logical.LogicalIntersect;
import org.lealone.hansql.optimizer.rel.logical.LogicalJoin;
import org.lealone.hansql.optimizer.rel.logical.LogicalMatch;
import org.lealone.hansql.optimizer.rel.logical.LogicalMinus;
import org.lealone.hansql.optimizer.rel.logical.LogicalProject;
import org.lealone.hansql.optimizer.rel.logical.LogicalSort;
import org.lealone.hansql.optimizer.rel.logical.LogicalUnion;
import org.lealone.hansql.optimizer.rel.logical.LogicalValues;

/**
 * Visits all the relations in a homogeneous way: always redirects calls to
 * {@code accept(RelNode)}.
 */
public class RelHomogeneousShuttle extends RelShuttleImpl {
  @Override public RelNode visit(LogicalAggregate aggregate) {
    return visit((RelNode) aggregate);
  }

  @Override public RelNode visit(LogicalMatch match) {
    return visit((RelNode) match);
  }

  @Override public RelNode visit(TableScan scan) {
    return visit((RelNode) scan);
  }

  @Override public RelNode visit(TableFunctionScan scan) {
    return visit((RelNode) scan);
  }

  @Override public RelNode visit(LogicalValues values) {
    return visit((RelNode) values);
  }

  @Override public RelNode visit(LogicalFilter filter) {
    return visit((RelNode) filter);
  }

  @Override public RelNode visit(LogicalProject project) {
    return visit((RelNode) project);
  }

  @Override public RelNode visit(LogicalJoin join) {
    return visit((RelNode) join);
  }

  @Override public RelNode visit(LogicalCorrelate correlate) {
    return visit((RelNode) correlate);
  }

  @Override public RelNode visit(LogicalUnion union) {
    return visit((RelNode) union);
  }

  @Override public RelNode visit(LogicalIntersect intersect) {
    return visit((RelNode) intersect);
  }

  @Override public RelNode visit(LogicalMinus minus) {
    return visit((RelNode) minus);
  }

  @Override public RelNode visit(LogicalSort sort) {
    return visit((RelNode) sort);
  }

  @Override public RelNode visit(LogicalExchange exchange) {
    return visit((RelNode) exchange);
  }
}

// End RelHomogeneousShuttle.java
