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
package org.lealone.hansql.exec.expr.fn.impl;

import org.lealone.hansql.exec.expr.holders.BigIntHolder;
import org.lealone.hansql.exec.expr.holders.VarCharHolder;
import org.lealone.hansql.exec.expr.DrillFunc;
import org.lealone.hansql.exec.expr.DrillSimpleFunc;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate;
import org.lealone.hansql.exec.expr.annotations.Output;
import org.lealone.hansql.exec.expr.annotations.Param;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate.FunctionScope;

public class ExceptionFunction {

    @FunctionTemplate(name = DrillFunc.EXCEPTION_FUNCTION_NAME, isRandom = true, //
            scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class ThrowException implements DrillSimpleFunc {

        @Param
        VarCharHolder message;
        @Output
        BigIntHolder out;

        @Override
        public void setup() {
        }

        @Override
        public void eval() {
            String msg = org.lealone.hansql.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(message.start,
                    message.end, message.buffer);
            org.lealone.hansql.exec.expr.fn.impl.ExceptionFunction.throwException(msg);
        }
    }

    public static void throwException(String message) {
        throw new org.lealone.hansql.common.exceptions.DrillRuntimeException(message);
    }
}
