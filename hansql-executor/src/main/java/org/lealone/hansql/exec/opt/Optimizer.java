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
package org.lealone.hansql.exec.opt;

import org.lealone.hansql.common.config.DrillConfig;
import org.lealone.hansql.common.exceptions.DrillConfigurationException;
import org.lealone.hansql.common.logical.LogicalPlan;
import org.lealone.hansql.exec.context.options.OptionManager;
import org.lealone.hansql.exec.exception.OptimizerException;
import org.lealone.hansql.exec.physical.PhysicalPlan;

public abstract class Optimizer {
  public static String OPTIMIZER_IMPL_KEY = "drill.exec.optimizer.implementation";

  public abstract void init(DrillConfig config);
  public abstract PhysicalPlan optimize(OptimizationContext context, LogicalPlan plan) throws OptimizerException;

  public static Optimizer getOptimizer(final DrillConfig config) throws DrillConfigurationException {
    final Optimizer optimizer = config.getInstanceOf(OPTIMIZER_IMPL_KEY, Optimizer.class);
    optimizer.init(config);
    return optimizer;
  }

  public interface OptimizationContext {
    public int getPriority();
    public OptionManager getOptions();
  }
}
