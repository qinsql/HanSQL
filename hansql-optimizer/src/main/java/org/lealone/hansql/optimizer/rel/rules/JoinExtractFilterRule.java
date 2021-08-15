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

import org.lealone.hansql.optimizer.rel.core.Join;
import org.lealone.hansql.optimizer.rel.core.RelFactories;
import org.lealone.hansql.optimizer.rel.logical.LogicalJoin;
import org.lealone.hansql.optimizer.tools.RelBuilderFactory;

/**
 * Rule to convert an
 * {@link org.lealone.hansql.optimizer.rel.logical.LogicalJoin inner join} to a
 * {@link org.lealone.hansql.optimizer.rel.logical.LogicalFilter filter} on top of a
 * {@link org.lealone.hansql.optimizer.rel.logical.LogicalJoin cartesian inner join}.
 *
 * <p>One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the <code>FennelCartesianJoinRule</code> applicable.
 *
 * <p>The constructor is parameterized to allow any sub-class of
 * {@link org.lealone.hansql.optimizer.rel.core.Join}, not just
 * {@link org.lealone.hansql.optimizer.rel.logical.LogicalJoin}.</p>
 */
public final class JoinExtractFilterRule extends AbstractJoinExtractFilterRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final JoinExtractFilterRule INSTANCE =
      new JoinExtractFilterRule(LogicalJoin.class,
          RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a JoinExtractFilterRule.
   */
  public JoinExtractFilterRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory) {
    super(operand(clazz, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

}

// End JoinExtractFilterRule.java
