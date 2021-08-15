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
package org.lealone.hansql.exec.planner.sql.handlers;

import static org.lealone.hansql.exec.store.ischema.InfoSchemaConstants.IS_SCHEMA_NAME;
import static org.lealone.hansql.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_NAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.lealone.hansql.exec.planner.sql.parser.DrillParserUtil;
import org.lealone.hansql.exec.planner.sql.parser.SqlShowSchemas;
import org.lealone.hansql.exec.store.ischema.InfoSchemaTableType;
import org.lealone.hansql.exec.work.exception.SqlExecutorSetupException;
import org.lealone.hansql.optimizer.sql.SqlCharStringLiteral;
import org.lealone.hansql.optimizer.sql.SqlIdentifier;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.sql.SqlNodeList;
import org.lealone.hansql.optimizer.sql.SqlSelect;
import org.lealone.hansql.optimizer.sql.fun.SqlStdOperatorTable;
import org.lealone.hansql.optimizer.sql.parser.SqlParserPos;
import org.lealone.hansql.optimizer.util.NlsString;

public class ShowSchemasHandler extends DefaultSqlHandler {

  public ShowSchemasHandler(SqlHandlerConfig config) { super(config); }

  /** Rewrite the parse tree as SELECT ... FROM INFORMATION_SCHEMA.SCHEMATA ... */
  @Override
  public SqlNode rewrite(SqlNode sqlNode) throws SqlExecutorSetupException {
    SqlShowSchemas node = unwrap(sqlNode, SqlShowSchemas.class);
    List<SqlNode> selectList = Collections.singletonList(new SqlIdentifier(SCHS_COL_SCHEMA_NAME, SqlParserPos.ZERO));

    SqlNode fromClause = new SqlIdentifier(Arrays.asList(IS_SCHEMA_NAME, InfoSchemaTableType.SCHEMATA.name()), SqlParserPos.ZERO);

    SqlNode where = null;
    SqlNode likePattern = node.getLikePattern();
    if (likePattern != null) {
      SqlNode column = new SqlIdentifier(SCHS_COL_SCHEMA_NAME, SqlParserPos.ZERO);
      // schema names are case insensitive, wrap column in lower function, pattern to lower case
      if (likePattern instanceof SqlCharStringLiteral) {
        NlsString conditionString = ((SqlCharStringLiteral) likePattern).getNlsString();
        likePattern = SqlCharStringLiteral.createCharString(
            conditionString.getValue().toLowerCase(),
            conditionString.getCharsetName(),
            likePattern.getParserPosition());
        column = SqlStdOperatorTable.LOWER.createCall(SqlParserPos.ZERO, column);
      }
      where = DrillParserUtil.createCondition(column, SqlStdOperatorTable.LIKE, likePattern);
    } else if (node.getWhereClause() != null) {
      where = node.getWhereClause();
    }

    return new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(selectList, SqlParserPos.ZERO),
        fromClause, where, null, null, null, null, null, null);
  }
}
