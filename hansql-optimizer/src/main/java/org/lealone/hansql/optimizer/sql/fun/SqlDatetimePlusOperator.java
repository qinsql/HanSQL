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
package org.lealone.hansql.optimizer.sql.fun;

import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeFactory;
import org.lealone.hansql.optimizer.sql.SqlCall;
import org.lealone.hansql.optimizer.sql.SqlKind;
import org.lealone.hansql.optimizer.sql.SqlOperatorBinding;
import org.lealone.hansql.optimizer.sql.SqlSpecialOperator;
import org.lealone.hansql.optimizer.sql.SqlSyntax;
import org.lealone.hansql.optimizer.sql.SqlWriter;
import org.lealone.hansql.optimizer.sql.type.InferTypes;
import org.lealone.hansql.optimizer.sql.type.IntervalSqlType;
import org.lealone.hansql.optimizer.sql.type.OperandTypes;
import org.lealone.hansql.optimizer.sql.type.ReturnTypes;
import org.lealone.hansql.optimizer.sql.validate.SqlMonotonicity;
import org.lealone.hansql.optimizer.util.TimeUnit;

/**
 * Operator that adds an INTERVAL to a DATETIME.
 */
public class SqlDatetimePlusOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  SqlDatetimePlusOperator() {
    super("+", SqlKind.PLUS, 40, true, ReturnTypes.ARG2_NULLABLE,
        InferTypes.FIRST_KNOWN, OperandTypes.MINUS_DATE_OPERATOR);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType leftType = opBinding.getOperandType(0);
    final IntervalSqlType unitType =
        (IntervalSqlType) opBinding.getOperandType(1);
    final TimeUnit timeUnit = unitType.getIntervalQualifier().getStartUnit();
    return SqlTimestampAddFunction.deduceType(typeFactory, timeUnit,
        unitType, leftType);
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseSqlDatetimeArithmetic(
        writer, call, SqlKind.PLUS, leftPrec, rightPrec);
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlStdOperatorTable.PLUS.getMonotonicity(call);
  }
}

// End SqlDatetimePlusOperator.java
