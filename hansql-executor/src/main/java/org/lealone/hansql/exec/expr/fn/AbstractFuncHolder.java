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
package org.lealone.hansql.exec.expr.fn;

import java.util.List;

import org.lealone.hansql.common.expression.ExpressionPosition;
import org.lealone.hansql.common.expression.FieldReference;
import org.lealone.hansql.common.expression.FunctionHolderExpression;
import org.lealone.hansql.common.expression.LogicalExpression;
import org.lealone.hansql.common.expression.fn.FuncHolder;
import org.lealone.hansql.common.types.TypeProtos.MajorType;
import org.lealone.hansql.exec.expr.ClassGenerator;
import org.lealone.hansql.exec.expr.ClassGenerator.HoldingContainer;

import com.sun.codemodel.JVar;

public abstract class AbstractFuncHolder implements FuncHolder {

  public abstract JVar[] renderStart(ClassGenerator<?> g, HoldingContainer[] inputVariables, FieldReference fieldReference);

  public void renderMiddle(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[] workspaceJVars) {
    // default implementation is add no code
  }

  /**
   * Generate methods body and complete the code generation.
   *
   * @param classGenerator the class responsible for code generation
   * @param inputVariables the source of the vector holders
   * @param workspaceJVars class fields
   * @param fieldReference reference of the output field
   * @return HoldingContainer for return value
   */
  public abstract HoldingContainer renderEnd(ClassGenerator<?> classGenerator, HoldingContainer[] inputVariables,
                                             JVar[] workspaceJVars, FieldReference fieldReference);

  public boolean isNested() {
    return false;
  }

  public abstract FunctionHolderExpression getExpr(String name, List<LogicalExpression> args, ExpressionPosition pos);

  public abstract MajorType getParmMajorType(int i);

  public abstract int getParamCount();

  /**
   * Checks that the current function holder stores output value
   * using field writer instead of vector holder.
   *
   * @return true if current function holder uses field writer to store the output value
   */
  public boolean isComplexWriterFuncHolder() {
    return false;
  }
}
