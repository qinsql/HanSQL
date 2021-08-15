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
package org.lealone.hansql.exec.physical.impl.producer;

import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;
import org.lealone.hansql.common.exceptions.ExecutionSetupException;
import org.lealone.hansql.exec.ops.ExecutorFragmentContext;
import org.lealone.hansql.exec.physical.config.ProducerConsumer;
import org.lealone.hansql.exec.physical.impl.BatchCreator;
import org.lealone.hansql.exec.record.RecordBatch;

public class ProducerConsumerBatchCreator implements BatchCreator<ProducerConsumer> {
  @Override
  public ProducerConsumerBatch getBatch(ExecutorFragmentContext context, ProducerConsumer config, List<RecordBatch> children)
      throws ExecutionSetupException {
    return new ProducerConsumerBatch(config, context, Iterables.getOnlyElement(children));
  }
}
