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
package org.lealone.hansql.exec.planner.sql.parser.impl;

import java.io.Reader;

import org.lealone.hansql.exec.planner.sql.parser.impl.DrillParserImpl;
import org.lealone.hansql.exec.planner.physical.PlannerSettings;
import org.lealone.hansql.exec.planner.sql.parser.CompoundIdentifierConverter;
import org.lealone.hansql.exec.planner.sql.parser.SqlAnalyzeTable;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.sql.parser.SqlAbstractParserImpl;
import org.lealone.hansql.optimizer.sql.parser.SqlParserImplFactory;
import org.lealone.hansql.optimizer.sql.util.SqlVisitor;

public class DrillParserWithCompoundIdConverter extends DrillParserImpl {

  /**
   * {@link org.lealone.hansql.optimizer.sql.parser.SqlParserImplFactory} implementation for creating parser.
   */

  public static final SqlParserImplFactory FACTORY = new SqlParserImplFactory() {
    public SqlAbstractParserImpl getParser(Reader stream) {
      SqlAbstractParserImpl parserImpl = new DrillParserWithCompoundIdConverter(stream);
      parserImpl.setIdentifierMaxLength(PlannerSettings.DEFAULT_IDENTIFIER_MAX_LENGTH);
      return parserImpl;
    }
  };

  public DrillParserWithCompoundIdConverter(Reader stream) {
    super(stream);
  }

  protected SqlVisitor<SqlNode> createConverter() {
    return createConverter(false);
  }

  private SqlVisitor<SqlNode> createConverter(boolean allowNoTableRefCompoundIdentifier) {
    return new CompoundIdentifierConverter(allowNoTableRefCompoundIdentifier);
  }

  @Override
  public SqlNode parseSqlExpressionEof() throws Exception {
    SqlNode originalSqlNode = super.parseSqlExpressionEof();
    return originalSqlNode.accept(createConverter());
  }

  @Override
  public SqlNode parseSqlStmtEof() throws Exception {
    SqlNode originalSqlNode = super.parseSqlStmtEof();
    return originalSqlNode.accept(createConverter(originalSqlNode instanceof SqlAnalyzeTable));
  }
}
