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
package org.lealone.hansql.optimizer.plan;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

import org.lealone.hansql.optimizer.rel.RelCollation;
import org.lealone.hansql.optimizer.rel.RelDistribution;
import org.lealone.hansql.optimizer.rel.RelDistributions;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelReferentialConstraint;
import org.lealone.hansql.optimizer.rel.logical.LogicalTableScan;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeField;
import org.lealone.hansql.optimizer.schema.ColumnStrategy;
import org.lealone.hansql.optimizer.schema.RelOptTableImpl;
import org.lealone.hansql.optimizer.util.ImmutableBitSet;

/**
 * Partial implementation of {@link RelOptTable}.
 */
public abstract class RelOptAbstractTable implements RelOptTable {
  //~ Instance fields --------------------------------------------------------

  protected final RelOptSchema schema;
  protected final RelDataType rowType;
  protected final String name;

  //~ Constructors -----------------------------------------------------------

  protected RelOptAbstractTable(
      RelOptSchema schema,
      String name,
      RelDataType rowType) {
    this.schema = schema;
    this.name = name;
    this.rowType = rowType;
  }

  //~ Methods ----------------------------------------------------------------

  public String getName() {
    return name;
  }

  public List<String> getQualifiedName() {
    return ImmutableList.of(name);
  }

  public double getRowCount() {
    return 100;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public RelOptSchema getRelOptSchema() {
    return schema;
  }

  // Override to define collations.
  public List<RelCollation> getCollationList() {
    return Collections.emptyList();
  }

  public RelDistribution getDistribution() {
    return RelDistributions.BROADCAST_DISTRIBUTED;
  }

  public <T> T unwrap(Class<T> clazz) {
    return clazz.isInstance(this)
        ? clazz.cast(this)
        : null;
  }

  // Override to define keys
  public boolean isKey(ImmutableBitSet columns) {
    return false;
  }

  // Override to define foreign keys
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return Collections.emptyList();
  }

  public RelNode toRel(ToRelContext context) {
    return LogicalTableScan.create(context.getCluster(), this);
  }

  public RelOptTable extend(List<RelDataTypeField> extendedFields) {
    throw new UnsupportedOperationException();
  }

  public List<ColumnStrategy> getColumnStrategies() {
    return RelOptTableImpl.columnStrategies(this);
  }

}

// End RelOptAbstractTable.java
