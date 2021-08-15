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
package org.lealone.hansql.exec.planner.common;

import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.lealone.hansql.exec.planner.logical.CreateTableEntry;
import org.lealone.hansql.optimizer.plan.Convention;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.SingleRel;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.sql.type.SqlTypeName;

/** Base class for logical and physical Writer implemented in Drill. */
public abstract class DrillWriterRelBase extends SingleRel implements DrillRelNode {

  private static final List<String> FIELD_NAMES = ImmutableList.of("Fragment", "Number of records written");
  private final CreateTableEntry createTableEntry;

  protected void setRowType(){
    List<RelDataType> fields = Lists.newArrayList();
    fields.add(this.getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 255));
    fields.add(this.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    this.rowType = this.getCluster().getTypeFactory().createStructType(fields, FIELD_NAMES);
  }

  public DrillWriterRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      CreateTableEntry createTableEntry) {
    super(cluster, traitSet, input);
    assert input.getConvention() == convention;
    this.createTableEntry = createTableEntry;
  }

  public CreateTableEntry getCreateTableEntry() {
    return createTableEntry;
  }
}
