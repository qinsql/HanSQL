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
package org.lealone.hansql.exec.physical.impl.partitionsender;

import java.io.IOException;
import java.util.List;

import org.lealone.hansql.exec.compile.TemplateClassDefinition;
import org.lealone.hansql.exec.exception.SchemaChangeException;
import org.lealone.hansql.exec.expr.ClassGenerator;
import org.lealone.hansql.exec.ops.ExchangeFragmentContext;
import org.lealone.hansql.exec.ops.OperatorContext;
import org.lealone.hansql.exec.ops.OperatorStats;
import org.lealone.hansql.exec.physical.config.HashPartitionSender;
import org.lealone.hansql.exec.record.RecordBatch;

public interface Partitioner {
  // Keep the recordCount as (2^x) - 1 to better utilize the memory allocation in ValueVectors; however
  // other criteria such as batch sizing in terms of actual MBytes rather than record count could also be applied
  // by the operator.
  int DEFAULT_RECORD_BATCH_SIZE = (1 << 10) - 1;

  void setup(ExchangeFragmentContext context,
             RecordBatch incoming,
             HashPartitionSender popConfig,
             OperatorStats stats,
             OperatorContext oContext,
             ClassGenerator<?> cg,
             int start, int count) throws SchemaChangeException;

  void partitionBatch(RecordBatch incoming) throws IOException;
  void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) throws IOException;
  void initialize();
  void clear();
  List<? extends PartitionOutgoingBatch> getOutgoingBatches();
  /**
   * Method to get PartitionOutgoingBatch based on the fact that there can be > 1 Partitioner
   * @param index
   * @return PartitionOutgoingBatch that matches index within Partitioner. This method can
   * return null if index does not fall within boundary of this Partitioner
   */
  PartitionOutgoingBatch getOutgoingBatch(int index);
  OperatorStats getStats();

  TemplateClassDefinition<Partitioner> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(Partitioner.class, PartitionerTemplate.class);
}