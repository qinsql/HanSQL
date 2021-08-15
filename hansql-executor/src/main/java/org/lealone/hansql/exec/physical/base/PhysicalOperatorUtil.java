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
package org.lealone.hansql.exec.physical.base;

import java.util.List;
import java.util.Set;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.common.expression.ErrorCollector;
import org.lealone.hansql.common.expression.ErrorCollectorImpl;
import org.lealone.hansql.common.expression.LogicalExpression;
import org.lealone.hansql.common.scanner.persistence.ScanResult;
import org.lealone.hansql.exec.exception.SchemaChangeException;
import org.lealone.hansql.exec.expr.ExpressionTreeMaterializer;
import org.lealone.hansql.exec.ops.FragmentContext;
import org.lealone.hansql.exec.physical.MinorFragmentEndpoint;
import org.lealone.hansql.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.lealone.hansql.exec.record.VectorAccessible;

public class PhysicalOperatorUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalOperatorUtil.class);


  private PhysicalOperatorUtil() {}

  public static Set<Class<? extends PhysicalOperator>> getSubTypes(ScanResult classpathScan) {
    final Set<Class<? extends PhysicalOperator>> ops = classpathScan.getImplementations(PhysicalOperator.class);
    logger.debug("Found {} physical operator classes: {}.", ops.size(),
                 ops);
    return ops;
  }

  /**
   * Helper method to create a list of MinorFragmentEndpoint instances from a given endpoint assignment list.
   *
   * @param endpoints Assigned endpoint list. Index of each endpoint in list indicates the MinorFragmentId of the
   *                  fragment that is assigned to the endpoint.
   * @return
   */
  public static List<MinorFragmentEndpoint> getIndexOrderedEndpoints(List<DrillbitEndpoint> endpoints) {
    List<MinorFragmentEndpoint> destinations = Lists.newArrayList();
    int minorFragmentId = 0;
    for(DrillbitEndpoint endpoint : endpoints) {
      destinations.add(new MinorFragmentEndpoint(minorFragmentId, endpoint));
      minorFragmentId++;
    }

    return destinations;
  }

  /**
   * Helper method tp materialize the given logical expression using the ExpressionTreeMaterializer
   * @param expr Logical expression to materialize
   * @param incoming Incoming record batch
   * @param context Fragment context
   */
  public static LogicalExpression materializeExpression(LogicalExpression expr,
      VectorAccessible incoming, FragmentContext context) throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();
    LogicalExpression mle = ExpressionTreeMaterializer.materialize(expr, incoming, collector,
            context.getFunctionRegistry());
    if (collector.hasErrors()) {
      throw new SchemaChangeException("Failure while materializing expression. "
          + collector.toErrorString());
    }
    return mle;
  }
}
