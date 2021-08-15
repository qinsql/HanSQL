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

import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelDistribution;
import org.lealone.hansql.optimizer.rel.RelDistributionTraitDef;
import org.lealone.hansql.optimizer.rel.RelInput;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelShuttle;
import org.lealone.hansql.optimizer.rel.core.Exchange;

/**
 * Sub-class of {@link Exchange} not
 * targeted at any particular engine or calling convention.
 */
public final class LogicalExchange extends Exchange {
  private LogicalExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  /**
   * Creates a LogicalExchange by parsing serialized output.
   */
  public LogicalExchange(RelInput input) {
    super(input);
  }

  /**
   * Creates a LogicalExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   */
  public static LogicalExchange create(RelNode input,
      RelDistribution distribution) {
    RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(distribution);
    return new LogicalExchange(cluster, traitSet, input, distribution);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Exchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution) {
    return new LogicalExchange(getCluster(), traitSet, newInput,
        newDistribution);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalExchange.java
