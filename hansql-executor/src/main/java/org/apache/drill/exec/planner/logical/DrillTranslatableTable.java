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
package org.apache.drill.exec.planner.logical;

import org.lealone.hansql.optimizer.config.CalciteConnectionConfig;
import org.lealone.hansql.optimizer.plan.RelOptTable;
import org.lealone.hansql.optimizer.plan.RelOptTable.ToRelContext;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeFactory;
import org.lealone.hansql.optimizer.schema.Statistic;
import org.lealone.hansql.optimizer.schema.TranslatableTable;
import org.lealone.hansql.optimizer.schema.Schema.TableType;
import org.lealone.hansql.optimizer.sql.SqlCall;
import org.lealone.hansql.optimizer.sql.SqlNode;

/**
 * TableMacros must return a TranslatableTable
 * This class adapts the existing DrillTable to a TranslatableTable
 */
public class DrillTranslatableTable implements TranslatableTable {

  /** all calls will be delegated to this field */
  private final DrillTable drillTable;

  public DrillTranslatableTable(DrillTable drillTable) {
    this.drillTable = drillTable;
  }

  public DrillTable getDrillTable() {
    return drillTable;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return drillTable.getRowType(typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    return drillTable.getStatistic();
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable table) {
    return drillTable.toRel(context, table);
  }

  @Override
  public TableType getJdbcTableType() {
    return drillTable.getJdbcTableType();
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String column,
      SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return true;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public int hashCode() {
    return drillTable.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return drillTable.equals(obj);
  }

  @Override
  public String toString() {
    return drillTable.toString();
  }
}