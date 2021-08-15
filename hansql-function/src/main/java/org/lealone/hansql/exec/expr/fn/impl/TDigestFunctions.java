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

import io.netty.buffer.DrillBuf;

import org.lealone.hansql.exec.expr.holders.BigIntHolder;
import org.lealone.hansql.exec.expr.holders.NullableBigIntHolder;
import org.lealone.hansql.exec.expr.holders.BitHolder;
import org.lealone.hansql.exec.expr.holders.NullableBitHolder;
import org.lealone.hansql.exec.expr.holders.NullableIntHolder;
import org.lealone.hansql.exec.expr.holders.NullableFloat8Holder;
import org.lealone.hansql.exec.expr.holders.NullableFloat4Holder;
import org.lealone.hansql.exec.expr.holders.Float8Holder;
import org.lealone.hansql.exec.expr.holders.Float4Holder;
import org.lealone.hansql.exec.expr.holders.DateHolder;
import org.lealone.hansql.exec.expr.holders.TimeHolder;
import org.lealone.hansql.exec.expr.holders.TimeStampHolder;
import org.lealone.hansql.exec.expr.holders.NullableDateHolder;
import org.lealone.hansql.exec.expr.holders.NullableTimeHolder;
import org.lealone.hansql.exec.expr.holders.NullableTimeStampHolder;
import org.lealone.hansql.exec.expr.holders.IntHolder;
import org.lealone.hansql.exec.expr.holders.NullableVarBinaryHolder;
import org.lealone.hansql.exec.expr.holders.VarCharHolder;
import org.lealone.hansql.exec.context.options.OptionManager;
import org.lealone.hansql.exec.expr.DrillAggFunc;
import org.lealone.hansql.exec.expr.annotations.FunctionTemplate;
import org.lealone.hansql.exec.expr.annotations.Output;
import org.lealone.hansql.exec.expr.annotations.Param;
import org.lealone.hansql.exec.expr.annotations.Workspace;
import org.lealone.hansql.exec.expr.holders.ObjectHolder;
import org.lealone.hansql.exec.expr.holders.VarBinaryHolder;
import org.lealone.hansql.exec.expr.holders.NullableVarCharHolder;

import javax.inject.Inject;

@SuppressWarnings("unused")
public class TDigestFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TDigestFunctions.class);

  private TDigestFunctions(){}

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntTDigestFunction implements DrillAggFunc {
    @Param BigIntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntTDigestFunction implements DrillAggFunc {
    @Param NullableBigIntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntTDigestFunction implements DrillAggFunc {
    @Param IntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntTDigestFunction implements DrillAggFunc {
    @Param NullableIntHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8TDigestFunction implements DrillAggFunc {
    @Param Float8Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8TDigestFunction implements DrillAggFunc {
    @Param NullableFloat8Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float4TDigestFunction implements DrillAggFunc {
    @Param Float4Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4TDigestFunction implements DrillAggFunc {
    @Param NullableFloat4Holder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitTDigestFunction implements DrillAggFunc {
    @Param BitHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitTDigestFunction implements DrillAggFunc {
    @Param NullableBitHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class DateTDigestFunction implements DrillAggFunc {
    @Param DateHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateTDigestFunction implements DrillAggFunc {
    @Param NullableDateHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeTDigestFunction implements DrillAggFunc {
    @Param TimeHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeTDigestFunction implements DrillAggFunc {
    @Param NullableTimeHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeStampTDigestFunction implements DrillAggFunc {
    @Param TimeStampHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        tdigest.add(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampTDigestFunction implements DrillAggFunc {
    @Param NullableTimeStampHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        if (in.isSet == 1) {
          tdigest.add(in.value);
        } else {
          // do nothing since we track nulls outside the scope of the histogram
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (tdigest.size() > 0) {
            int size = tdigest.smallByteSize();
            java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
            tdigest.asSmallBytes(byteBuf);
            out.buffer = buffer.reallocIfNeeded(size);
            out.start = 0;
            out.end = size;
            out.buffer.setBytes(0, byteBuf.array());
            out.isSet = 1;
          } else {
            out.isSet = 0;
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarCharTDigestFunction implements DrillAggFunc {
    @Param VarCharHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {

    }

    @Override
    public void add() {

    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {

    }
  }


  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharTDigestFunction implements DrillAggFunc {
    @Param NullableVarCharHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {

    }

    @Override
    public void add() {

    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {

    }
  }

  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarBinaryTDigestFunction implements DrillAggFunc {
    @Param VarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {

    }

    @Override
    public void add() {

    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {

    }
  }


  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryTDigestFunction implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {

    }

    @Override
    public void add() {

    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {

    }
  }

  @FunctionTemplate(name = "tdigest_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TDigestMergeFunction implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;
    @Inject OptionManager options;
    @Workspace IntHolder compression;

    @Override
    public void setup() {
      work = new ObjectHolder();
      compression.value = (int) options.getLong(org.lealone.hansql.exec.ExecConstants.TDIGEST_COMPRESSION);
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          if (in.isSet != 0) {
            byte[] buf = org.lealone.hansql.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer).getBytes();
            com.clearspring.analytics.stream.quantile.TDigest other =
              com.clearspring.analytics.stream.quantile.TDigest.fromBytes(java.nio.ByteBuffer.wrap(buf));
            tdigest.add(other);
          }
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to merge TDigest output", e);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.quantile.TDigest tdigest = (com.clearspring.analytics.stream.quantile.TDigest) work.obj;
        try {
          int size = tdigest.smallByteSize();
          java.nio.ByteBuffer byteBuf = java.nio.ByteBuffer.allocate(size);
          tdigest.asSmallBytes(byteBuf);
          out.buffer = buffer.reallocIfNeeded(size);
          out.start = 0;
          out.end = size;
          out.buffer.setBytes(0, byteBuf.array());
          out.isSet = 1;
        } catch (Exception e) {
          throw new org.lealone.hansql.common.exceptions.DrillRuntimeException("Failed to get TDigest output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.quantile.TDigest(compression.value);
    }
  }
}
