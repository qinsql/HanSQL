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


import io.netty.buffer.DrillBuf;

import javax.inject.Inject;

import org.lealone.hansql.exec.expr.holders.NullableVarBinaryHolder;
import org.lealone.hansql.exec.expr.holders.NullableVarCharHolder;
import org.lealone.hansql.exec.expr.holders.VarBinaryHolder;
import org.lealone.hansql.exec.expr.holders.VarCharHolder;
import org.lealone.hansql.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.lealone.hansql.exec.expr.DrillSimpleFunc;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate;
import org.lealone.hansql.exec.expr.annotations.Output;
import org.lealone.hansql.exec.expr.annotations.Param;
import org.lealone.hansql.exec.expr.annotations.Workspace;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate.FunctionScope;

public class JsonConvertFrom {

 static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonConvertFrom.class);

  private JsonConvertFrom() {
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJson implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Inject DrillBuf buffer;
    @Workspace org.lealone.hansql.exec.vector.complex.fn.JsonReader jsonReader;

    @Output ComplexWriter writer;

    @Override
    public void setup() {
      jsonReader = new org.lealone.hansql.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .build();
    }

    @Override
    public void eval() {
      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarchar implements DrillSimpleFunc {

    @Param VarCharHolder in;
    @Inject DrillBuf buffer;
    @Workspace org.lealone.hansql.exec.vector.complex.fn.JsonReader jsonReader;

    @Output ComplexWriter writer;

    @Override
    public void setup() {
      jsonReader = new org.lealone.hansql.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .build();
    }

    @Override
    public void eval() {
      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonNullableInput implements DrillSimpleFunc {

    @Param NullableVarBinaryHolder in;
    @Inject DrillBuf buffer;
    @Workspace org.lealone.hansql.exec.vector.complex.fn.JsonReader jsonReader;

    @Output ComplexWriter writer;

    @Override
    public void setup() {
      jsonReader = new org.lealone.hansql.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .build();
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        // Return empty map
        org.lealone.hansql.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", scope = FunctionScope.SIMPLE, isRandom = true)
  public static class ConvertFromJsonVarcharNullableInput implements DrillSimpleFunc {

    @Param NullableVarCharHolder in;
    @Inject DrillBuf buffer;
    @Workspace org.lealone.hansql.exec.vector.complex.fn.JsonReader jsonReader;

    @Output ComplexWriter writer;

    @Override
    public void setup() {
      jsonReader = new org.lealone.hansql.exec.vector.complex.fn.JsonReader.Builder(buffer)
          .defaultSchemaPathColumns()
          .build();
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        // Return empty map
        org.lealone.hansql.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
        mapWriter.start();
        mapWriter.end();
        return;
      }

      try {
        jsonReader.setSource(in.start, in.end, in.buffer);
        jsonReader.write(writer);
        buffer = jsonReader.getWorkBuf();
      } catch (Exception e) {
        throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
      }
    }
  }
}
