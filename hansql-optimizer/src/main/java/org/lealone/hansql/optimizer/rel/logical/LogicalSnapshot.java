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
import org.lealone.hansql.optimizer.rel.RelCollationTraitDef;
import org.lealone.hansql.optimizer.rel.RelDistributionTraitDef;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.Snapshot;
import org.lealone.hansql.optimizer.rel.metadata.RelMdCollation;
import org.lealone.hansql.optimizer.rel.metadata.RelMdDistribution;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;
import org.lealone.hansql.optimizer.rex.RexNode;

/**
 * Sub-class of {@link org.lealone.hansql.optimizer.rel.core.Snapshot}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalSnapshot extends Snapshot {

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalSnapshot.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traitSet  The traits of this relational expression
   * @param input     Input relational expression
   * @param period    Timestamp expression which as the table was at the given
   *                  time in the past
   */
  public LogicalSnapshot(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RexNode period) {
    super(cluster, traitSet, input, period);
  }

  @Override public Snapshot copy(RelTraitSet traitSet, RelNode input,
      RexNode period) {
    return new LogicalSnapshot(getCluster(), traitSet, input, period);
  }

  /** Creates a LogicalSnapshot. */
  public static LogicalSnapshot create(RelNode input, RexNode period) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet()
        .replace(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.snapshot(mq, input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.snapshot(mq, input));
    return new LogicalSnapshot(cluster, traitSet, input, period);
  }
}

// End LogicalSnapshot.java
