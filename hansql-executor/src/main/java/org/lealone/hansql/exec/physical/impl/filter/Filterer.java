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
package org.lealone.hansql.exec.physical.impl.filter;

import org.lealone.hansql.exec.compile.TemplateClassDefinition;
import org.lealone.hansql.exec.exception.SchemaChangeException;
import org.lealone.hansql.exec.ops.FragmentContext;
import org.lealone.hansql.exec.record.RecordBatch;
import org.lealone.hansql.exec.record.TransferPair;

public interface Filterer {
  TemplateClassDefinition<Filterer> TEMPLATE_DEFINITION2 = new TemplateClassDefinition<Filterer>(Filterer.class, FilterTemplate2.class);
  TemplateClassDefinition<Filterer> TEMPLATE_DEFINITION4 = new TemplateClassDefinition<Filterer>(Filterer.class, FilterTemplate4.class);

  void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, TransferPair[] transfers) throws SchemaChangeException;
  void filterBatch(int recordCount) throws SchemaChangeException;
}
