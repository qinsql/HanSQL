<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
import static org.apache.drill.shaded.guava.com.google.common.base.Preconditions.checkArgument;
import static org.apache.drill.shaded.guava.com.google.common.base.Preconditions.checkState;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.ObjectArrays;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.ObjectArrays;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import io.netty.buffer.*;

import org.apache.commons.lang3.ArrayUtils;

import org.lealone.hansql.common.exceptions.UserException;
import org.lealone.hansql.exec.expr.fn.impl.StringFunctionUtil;
import org.lealone.hansql.exec.memory.*;
import org.lealone.hansql.exec.proto.SchemaDefProtos;
import org.lealone.hansql.exec.proto.UserBitShared;
import org.lealone.hansql.exec.proto.UserBitShared.DrillPBError;
import org.lealone.hansql.exec.proto.UserBitShared.SerializedField;
import org.lealone.hansql.exec.record.*;
import org.lealone.hansql.exec.vector.*;
import org.lealone.hansql.common.exceptions.*;
import org.lealone.hansql.exec.exception.*;
import org.lealone.hansql.exec.expr.holders.*;
import org.lealone.hansql.common.expression.FieldReference;
import org.lealone.hansql.common.types.TypeProtos.*;
import org.lealone.hansql.common.types.Types;
import org.lealone.hansql.common.util.DrillStringUtils;
import org.lealone.hansql.exec.vector.complex.*;
import org.lealone.hansql.exec.vector.complex.reader.*;
import org.lealone.hansql.exec.vector.complex.impl.*;
import org.lealone.hansql.exec.vector.complex.writer.*;
import org.lealone.hansql.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.lealone.hansql.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.lealone.hansql.exec.util.JsonStringArrayList;

import org.lealone.hansql.exec.exception.OutOfMemoryException;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.DateTime;
import org.joda.time.Period;

import org.lealone.hansql.exec.util.Text;

import org.lealone.hansql.exec.vector.accessor.sql.TimePrintMillis;
import javax.inject.Inject;





