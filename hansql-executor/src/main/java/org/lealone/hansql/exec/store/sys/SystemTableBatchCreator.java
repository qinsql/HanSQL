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
package org.lealone.hansql.exec.store.sys;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.lealone.hansql.common.exceptions.ExecutionSetupException;
import org.lealone.hansql.exec.ops.ExecutorFragmentContext;
import org.lealone.hansql.exec.physical.impl.BatchCreator;
import org.lealone.hansql.exec.physical.impl.ScanBatch;
import org.lealone.hansql.exec.record.RecordBatch;
import org.lealone.hansql.exec.store.RecordReader;
import org.lealone.hansql.exec.store.pojo.PojoRecordReader;

/**
 * This class creates batches based on the the type of {@link org.lealone.hansql.exec.store.sys.SystemTable}.
 * The distributed tables and the local tables use different record readers.
 * Local system tables do not require a full-fledged query because these records are present on every Drillbit.
 */
public class SystemTableBatchCreator implements BatchCreator<SystemTableScan> {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public ScanBatch getBatch(final ExecutorFragmentContext context, final SystemTableScan scan,
                            final List<RecordBatch> children)
    throws ExecutionSetupException {
    final SystemTable table = scan.getTable();
    final Iterator<Object> iterator = table.getIterator(context, scan.getMaxRecordsToRead());
    final RecordReader reader = new PojoRecordReader(table.getPojoClass(), ImmutableList.copyOf(iterator), scan.getMaxRecordsToRead());

    return new ScanBatch(scan, context, Collections.singletonList(reader));
  }
}
