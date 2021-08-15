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

import com.google.common.collect.ImmutableList;

import java.util.List;

import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeFactory;
import org.lealone.hansql.optimizer.sql.SqlAggFunction;
import org.lealone.hansql.optimizer.sql.SqlFunctionCategory;
import org.lealone.hansql.optimizer.sql.SqlKind;
import org.lealone.hansql.optimizer.sql.type.OperandTypes;
import org.lealone.hansql.optimizer.sql.type.ReturnTypes;
import org.lealone.hansql.optimizer.util.Optionality;

/**
 * <code>SINGLE_VALUE</code> aggregate function returns the input value if there
 * is only one value in the input; Otherwise it triggers a run-time error.
 */
public class SqlSingleValueAggFunction extends SqlAggFunction {
  //~ Instance fields --------------------------------------------------------

  @Deprecated // to be removed before 2.0
  private final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  public SqlSingleValueAggFunction(
      RelDataType type) {
    super(
        "SINGLE_VALUE",
        null,
        SqlKind.SINGLE_VALUE,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM,
        false,
        false,
        Optionality.FORBIDDEN);
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  @SuppressWarnings("deprecation")
  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  @SuppressWarnings("deprecation")
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return type;
  }

  @Deprecated // to be removed before 2.0
  public RelDataType getType() {
    return type;
  }
}

// End SqlSingleValueAggFunction.java
