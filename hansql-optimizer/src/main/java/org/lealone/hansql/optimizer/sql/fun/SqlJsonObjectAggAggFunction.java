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

import java.util.Objects;

import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.sql.SqlAggFunction;
import org.lealone.hansql.optimizer.sql.SqlCall;
import org.lealone.hansql.optimizer.sql.SqlFunctionCategory;
import org.lealone.hansql.optimizer.sql.SqlJsonConstructorNullClause;
import org.lealone.hansql.optimizer.sql.SqlKind;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.sql.SqlWriter;
import org.lealone.hansql.optimizer.sql.type.OperandTypes;
import org.lealone.hansql.optimizer.sql.type.ReturnTypes;
import org.lealone.hansql.optimizer.sql.type.SqlTypeFamily;
import org.lealone.hansql.optimizer.sql.validate.SqlValidator;
import org.lealone.hansql.optimizer.sql.validate.SqlValidatorImpl;
import org.lealone.hansql.optimizer.sql.validate.SqlValidatorScope;
import org.lealone.hansql.optimizer.util.Optionality;

/**
 * The <code>JSON_OBJECTAGG</code> aggregate function.
 */
public class SqlJsonObjectAggAggFunction extends SqlAggFunction {
  private final SqlJsonConstructorNullClause nullClause;

  /** Creates a SqlJsonObjectAggAggFunction. */
  public SqlJsonObjectAggAggFunction(SqlKind kind,
      SqlJsonConstructorNullClause nullClause) {
    super(kind + "_" + nullClause.name(), null, kind, ReturnTypes.VARCHAR_2000, null,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY),
        SqlFunctionCategory.SYSTEM, false, false, Optionality.FORBIDDEN);
    this.nullClause = Objects.requireNonNull(nullClause);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame = writer.startFunCall("JSON_OBJECTAGG");
    writer.keyword("KEY");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.keyword("VALUE");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.keyword(nullClause.sql);
    writer.endFunCall(frame);
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    // To prevent operator rewriting by SqlFunction#deriveType.
    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand);
      ((SqlValidatorImpl) validator).setValidatedNodeType(operand, nodeType);
    }
    return validateOperands(validator, scope, call);
  }

  public SqlJsonObjectAggAggFunction with(SqlJsonConstructorNullClause nullClause) {
    return this.nullClause == nullClause ? this
        : new SqlJsonObjectAggAggFunction(getKind(), nullClause);
  }

  public SqlJsonConstructorNullClause getNullClause() {
    return nullClause;
  }
}

// End SqlJsonObjectAggAggFunction.java
