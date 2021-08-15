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
package org.lealone.hansql.exec.planner.sql.parser;

import java.util.List;

import org.lealone.hansql.optimizer.sql.SqlCall;
import org.lealone.hansql.optimizer.sql.SqlDescribeTable;
import org.lealone.hansql.optimizer.sql.SqlIdentifier;
import org.lealone.hansql.optimizer.sql.SqlKind;
import org.lealone.hansql.optimizer.sql.SqlLiteral;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.sql.SqlOperator;
import org.lealone.hansql.optimizer.sql.SqlSpecialOperator;
import org.lealone.hansql.optimizer.sql.SqlWriter;
import org.lealone.hansql.optimizer.sql.parser.SqlParserPos;
import org.lealone.hansql.optimizer.util.ImmutableNullableList;

/**
 * Sql parser tree node to represent statement:
 * { DESCRIBE | DESC } tblname [col_name | wildcard ]
 */
public class DrillSqlDescribeTable extends SqlDescribeTable {

  private final SqlNode columnQualifier;

  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("DESCRIBE_TABLE", SqlKind.DESCRIBE_TABLE) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new DrillSqlDescribeTable(pos, (SqlIdentifier) operands[0], (SqlIdentifier) operands[1], operands[2]);
    }
  };

  public DrillSqlDescribeTable(SqlParserPos pos, SqlIdentifier table, SqlIdentifier column, SqlNode columnQualifier) {
    super(pos, table, column);
    this.columnQualifier = columnQualifier;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.<SqlNode>of(getTable(), getColumn(), columnQualifier);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if (columnQualifier != null) {
      columnQualifier.unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlNode getColumnQualifier() { return columnQualifier; }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

}
