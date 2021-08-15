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
import org.lealone.hansql.exec.expr.annotations.Workspace;

<@pp.dropOutputFile />

<#list numericTypes.numeric as numerics>

<@pp.changeOutputFile name="/org/lealone/hansql/exec/expr/fn/impl/G${numerics}ToTimeStamp.java" />

<#include "/@includes/license.ftl" />

package org.lealone.hansql.exec.expr.fn.impl;

import org.lealone.hansql.exec.expr.DrillSimpleFunc;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.lealone.hansql.exec.expr.annotations.Output;
import org.lealone.hansql.exec.expr.annotations.Workspace;
import org.lealone.hansql.exec.expr.annotations.Param;
import org.lealone.hansql.exec.expr.holders.*;
import org.lealone.hansql.exec.record.RecordBatch;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@FunctionTemplate(name = "to_timestamp",
                  scope = FunctionTemplate.FunctionScope.SIMPLE,
                  nulls = NullHandling.NULL_IF_NULL)
public class G${numerics}ToTimeStamp implements DrillSimpleFunc {

  @Param  ${numerics}Holder left;
  @Output TimeStampHolder out;

  public void setup() {
  }

  public void eval() {
    long inputMillis = 0;

    <#if (numerics == "VarDecimal")>
    java.math.BigDecimal input = org.lealone.hansql.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(left.buffer, left.start, left.end - left.start, left.scale);
    inputMillis = input.multiply(new java.math.BigDecimal(1000)).longValue();
    <#else>
    inputMillis = (long) (left.value * 1000L);
    </#if>
    out.value = new org.joda.time.DateTime(inputMillis).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();
  }
}
</#list>