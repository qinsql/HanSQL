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
package org.lealone.hansql.exec.expr.fn.impl.conv;

import org.lealone.hansql.exec.expr.holders.TimeHolder;
import org.lealone.hansql.exec.expr.holders.VarBinaryHolder;
import org.lealone.hansql.exec.expr.DrillSimpleFunc;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate;
import org.lealone.hansql.exec.expr.annotations.Output;
import org.lealone.hansql.exec.expr.annotations.Param;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate.NullHandling;

@FunctionTemplate(name = "convert_fromTIME_EPOCH", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class TimeEpochConvertFrom implements DrillSimpleFunc {

  @Param VarBinaryHolder in;
  @Output TimeHolder out;

  @Override
  public void setup() { }

  @Override
  public void eval() {
    org.lealone.hansql.exec.util.ByteBufUtil.checkBufferLength(in.buffer, in.start, in.end, 8);

    in.buffer.readerIndex(in.start);
    long epochMillis = in.buffer.readLong();
    out.value = (int) (epochMillis % (24*3600*1000));
  }
}
