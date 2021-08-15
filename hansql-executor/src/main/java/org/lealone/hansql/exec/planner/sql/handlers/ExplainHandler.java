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

import java.io.IOException;

import org.lealone.hansql.common.logical.LogicalPlan;
import org.lealone.hansql.common.logical.PlanProperties.Generator.ResultMode;
import org.lealone.hansql.exec.ops.QueryContext;
import org.lealone.hansql.exec.physical.PhysicalPlan;
import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.planner.SqlPlanner;
import org.lealone.hansql.exec.planner.logical.DrillImplementor;
import org.lealone.hansql.exec.planner.logical.DrillParseContext;
import org.lealone.hansql.exec.planner.logical.DrillRel;
import org.lealone.hansql.exec.planner.physical.Prel;
import org.lealone.hansql.exec.planner.physical.explain.PrelSequencer;
import org.lealone.hansql.exec.util.Pointer;
import org.lealone.hansql.exec.work.exception.SqlExecutorSetupException;
import org.lealone.hansql.optimizer.plan.RelOptUtil;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.sql.SqlExplain;
import org.lealone.hansql.optimizer.sql.SqlExplainLevel;
import org.lealone.hansql.optimizer.sql.SqlLiteral;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.tools.RelConversionException;
import org.lealone.hansql.optimizer.tools.ValidationException;

public class ExplainHandler extends DefaultSqlHandler {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplainHandler.class);

    private ResultMode mode;
    private SqlExplainLevel level = SqlExplainLevel.ALL_ATTRIBUTES;

    public ExplainHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
        super(config, textPlan);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode)
            throws ValidationException, RelConversionException, IOException, SqlExecutorSetupException {
        final ConvertedRelNode convertedRelNode = validateAndConvert(sqlNode);
        final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
        final RelNode queryRelNode = convertedRelNode.getConvertedNode();

        log("Calcite", queryRelNode, logger, null);
        DrillRel drel = convertToDrel(queryRelNode);

        if (mode == ResultMode.LOGICAL) {
            LogicalExplain logicalResult = new LogicalExplain(drel, level, context);
            return SqlPlanner.createDirectPlan(context, logicalResult);
        }

        Prel prel = convertToPrel(drel, validatedRowType);
        logAndSetTextPlan("Drill Physical", prel, logger);
        PhysicalOperator pop = convertToPop(prel);
        PhysicalPlan plan = convertToPlan(pop);
        log("Drill Plan", plan, logger);
        PhysicalExplain physicalResult = new PhysicalExplain(prel, plan, level, context);
        return SqlPlanner.createDirectPlan(context, physicalResult);
    }

    @Override
    public SqlNode rewrite(SqlNode sqlNode) throws RelConversionException, SqlExecutorSetupException {
        SqlExplain node = unwrap(sqlNode, SqlExplain.class);
        SqlLiteral op = node.operand(2);
        SqlExplain.Depth depth = (SqlExplain.Depth) op.getValue();
        if (node.getDetailLevel() != null) {
            level = node.getDetailLevel();
        }
        switch (depth) {
        case LOGICAL:
            mode = ResultMode.LOGICAL;
            break;
        case PHYSICAL:
            mode = ResultMode.PHYSICAL;
            break;
        default:
            throw new UnsupportedOperationException("Unknown depth " + depth);
        }

        return node.operand(0);
    }

    public static class LogicalExplain {
        public String text;
        public String json;

        public LogicalExplain(RelNode node, SqlExplainLevel level, QueryContext context) {
            this.text = RelOptUtil.toString(node, level);
            DrillImplementor implementor = new DrillImplementor(new DrillParseContext(context.getPlannerSettings()),
                    ResultMode.LOGICAL);
            implementor.go((DrillRel) node);
            LogicalPlan plan = implementor.getPlan();
            this.json = plan.unparse(context.getLpPersistence());
        }
    }

    public static class PhysicalExplain {
        public String text;
        public String json;

        public PhysicalExplain(RelNode node, PhysicalPlan plan, SqlExplainLevel level, QueryContext context) {
            this.text = PrelSequencer.printWithIds((Prel) node, level);
            this.json = plan.unparse(context.getLpPersistence().getMapper().writer());
        }
    }

}
