/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.test.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.lealone.hansql.optimizer.config.CalciteConnectionConfig;
import org.lealone.hansql.optimizer.config.CalciteConnectionConfigImpl;
import org.lealone.hansql.optimizer.plan.Context;
import org.lealone.hansql.optimizer.plan.ConventionTraitDef;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCostFactory;
import org.lealone.hansql.optimizer.plan.RelOptCostImpl;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelOptTable;
import org.lealone.hansql.optimizer.plan.RelOptUtil;
import org.lealone.hansql.optimizer.plan.ViewExpanders;
import org.lealone.hansql.optimizer.plan.hep.HepPlanner;
import org.lealone.hansql.optimizer.plan.hep.HepProgramBuilder;
import org.lealone.hansql.optimizer.plan.volcano.VolcanoPlanner;
import org.lealone.hansql.optimizer.rel.RelCollationTraitDef;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelRoot;
import org.lealone.hansql.optimizer.rel.rules.ReduceExpressionsRule;
import org.lealone.hansql.optimizer.rel.type.RelDataType;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeFactory;
import org.lealone.hansql.optimizer.rel.type.RelDataTypeSystem;
import org.lealone.hansql.optimizer.rel.type.java.JavaTypeFactory;
import org.lealone.hansql.optimizer.rel.type.java.JavaTypeFactoryImpl;
import org.lealone.hansql.optimizer.rex.RexBuilder;
import org.lealone.hansql.optimizer.schema.CalciteCatalogReader;
import org.lealone.hansql.optimizer.schema.CalciteSchema;
import org.lealone.hansql.optimizer.schema.CatalogReader;
import org.lealone.hansql.optimizer.schema.SchemaPlus;
import org.lealone.hansql.optimizer.schema.Table;
import org.lealone.hansql.optimizer.schema.impl.AbstractTable;
import org.lealone.hansql.optimizer.sql.SqlNode;
import org.lealone.hansql.optimizer.sql.SqlOperatorTable;
import org.lealone.hansql.optimizer.sql.fun.SqlStdOperatorTable;
import org.lealone.hansql.optimizer.sql.parser.SqlParser;
import org.lealone.hansql.optimizer.sql.type.SqlTypeName;
import org.lealone.hansql.optimizer.sql.validate.SqlConformance;
import org.lealone.hansql.optimizer.sql.validate.SqlConformanceEnum;
import org.lealone.hansql.optimizer.sql.validate.SqlValidator;
import org.lealone.hansql.optimizer.sql.validate.SqlValidatorCatalogReader;
import org.lealone.hansql.optimizer.sql.validate.SqlValidatorUtil;
import org.lealone.hansql.optimizer.sql2rel.SqlRexConvertletTable;
import org.lealone.hansql.optimizer.sql2rel.SqlToRelConverter;
import org.lealone.hansql.optimizer.sql2rel.StandardConvertletTable;
import org.lealone.hansql.optimizer.tools.Program;
import org.lealone.hansql.optimizer.tools.Programs;
import org.lealone.hansql.optimizer.util.Pair;

public class HanSQLOptimizerTest {

    public static void main(String[] args) throws Exception {
        // 词法语法分析
        testSqlParser();

        // 语义分析
        testSqlValidator();

        // SqlNode转RelNode/RexNode
        testSqlToRelConverter();

        // 使用HepPlanner优化RelNode/RexNode
        testHepPlanner();

        // 使用VolcanoPlanner优化RelNode/RexNode
        // testVolcanoPlanner(); //还有错
    }

    static void testSqlParser() throws Exception {
        String sql = "select * from test where f1=1 or f2=2 order by f3 limit 2";
        SqlParser sqlParser = createSqlParser(sql);
        SqlNode sqlNode = sqlParser.parseQuery();
        System.out.println(sqlNode);
        System.out.println();

        // drill的parser不支持
        // sql = "insert into test(f1,f2,f3) values(1,2,3)";
        // sqlParser = createSqlParser(sql);
        // sqlNode = sqlParser.parseStmt();
        // System.out.println(sqlNode);
        // System.out.println();

        sql = "delete from test where f1=1";
        sqlNode = sqlParser.parseQuery(sql);
        System.out.println(sqlNode);
        System.out.println();

        sql = "call LTRIM('abc')";
        sqlNode = sqlParser.parseQuery(sql);
        System.out.println(sqlNode);
        System.out.println();
    }

    static Pair<SqlNode, SqlValidator> testSqlValidator() throws Exception {
        String sql = "select * from my_schema.test where f1=1 or f2=2 order by f3 limit 2";
        sql = "select * from test";
        sql = "select * from my_schema2.test2";
        sql = "select sum(f1),max(f2) from test";

        sql = "select t1.f1,sum(Distinct f1) as sumf1 from test as t1 "
                + "where f2>20 group by f1 having f1>10 order by f1 limit 2";
        // sql = "insert into test(f1,f2,f3) values(1,2,3)";
        // sql = "update test set f1=100 where f2>10";
        // sql = "delete from test where f2>10";
        SqlNode sqlNode = parse(sql);

        SqlOperatorTable opTab = SqlStdOperatorTable.instance();
        RelDataTypeFactory typeFactory = createJavaTypeFactory();
        SqlValidatorCatalogReader catalogReader = createCalciteCatalogReader(typeFactory);
        SqlConformance conformance = SqlConformanceEnum.DEFAULT;

        List<String> names = new ArrayList<>();
        names.add("my_schema");
        names.add("test");
        catalogReader.getTable(names);

        SqlValidator sqlValidator = SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory, conformance);
        sqlNode = sqlValidator.validate(sqlNode);
        // System.out.println(sqlNode);

        sql = "insert into test(f1,f2,f3) values(1,2,3)";
        // sqlNode = parse(sql);
        // sqlNode = sqlValidator.validate(sqlNode);

        return new Pair<>(sqlNode, sqlValidator);
    }

    static RelNode testSqlToRelConverter() throws Exception {
        RelOptPlanner planner = createHepPlanner();
        return testSqlToRelConverter(planner);
    }

    static RelNode testSqlToRelConverter(RelOptPlanner planner) throws Exception {
        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelOptTable.ViewExpander viewExpander = ViewExpanders.simpleContext(cluster);

        Pair<SqlNode, SqlValidator> pair = testSqlValidator();
        SqlNode sqlNode = pair.left;
        SqlValidator validator = pair.right;
        CatalogReader catalogReader = createCalciteCatalogReader();
        SqlRexConvertletTable convertletTable = StandardConvertletTable.INSTANCE;
        SqlToRelConverter.Config config = SqlToRelConverter.Config.DEFAULT;
        // 不转换成EnumerableTableScan，而是LogicalTableScan
        config = SqlToRelConverter.configBuilder().withConvertTableAccess(false).build();

        SqlToRelConverter converter = new SqlToRelConverter(viewExpander, validator, catalogReader, cluster,
                convertletTable, config);

        boolean needsValidation = false;
        boolean top = false;
        RelRoot root = converter.convertQuery(sqlNode, needsValidation, top);
        RelNode relNode = root.rel;

        String plan = RelOptUtil.toString(relNode);
        System.out.println("Logical Plan:");
        System.out.println("------------------------------------------------------------------");
        System.out.println(plan);
        System.out.println();

        // testPrograms(root.rel);

        return relNode;
    }

    static void testHepPlanner() throws Exception {
        RelOptPlanner hepPlanner = createHepPlanner();
        RelNode relNode = testSqlToRelConverter(hepPlanner);
        hepPlanner = relNode.getCluster().getPlanner();
        // relNode.getCluster().getPlanner().setExecutor(RexUtil.EXECUTOR);
        hepPlanner.setRoot(relNode);
        relNode = hepPlanner.findBestExp();

        String plan = RelOptUtil.toString(relNode);
        System.out.println("Hep Plan:");
        System.out.println("------------------------------------------------------------------");
        System.out.println(plan);
    }

    static void testVolcanoPlanner() throws Exception {
        VolcanoPlanner volcanoPlanner = createVolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        // addRules(volcanoPlanner);
        volcanoPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
        // volcanoPlanner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);

        RelNode relNode = testSqlToRelConverter(volcanoPlanner);
        volcanoPlanner.setRoot(relNode);
        relNode = volcanoPlanner.findBestExp(); // 在这一步出错

        String plan = RelOptUtil.toString(relNode);
        System.out.println("Volcano Plan:");
        System.out.println("------------------------------------------------------------------");
        System.out.println(plan);
    }

    static SqlNode parse(String sql) throws Exception {
        SqlParser sqlParser = createSqlParser(sql);
        return sqlParser.parseQuery();
    }

    static SqlParser createSqlParser(String sql) throws Exception {
        SqlParser.Config config = SqlParser.configBuilder().setUnquotedCasing(org.lealone.hansql.optimizer.util.Casing.TO_LOWER)
                .setParserFactory(org.apache.drill.exec.planner.sql.parser.impl.DrillParserImpl.FACTORY).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        return sqlParser;
    }

    static Table createTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER),
                        true);
                RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER),
                        true);
                RelDataType t3 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER),
                        true);

                builder.add("f1", t1);
                builder.add("f2", t2);
                builder.add("f3", t3);
                return builder.build();
            }
        };
    }

    static CalciteCatalogReader createCalciteCatalogReader() {
        return createCalciteCatalogReader(createJavaTypeFactory());
    }

    static CalciteCatalogReader createCalciteCatalogReader(RelDataTypeFactory typeFactory) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();

        String schemaName = "my_schema";
        List<String> defaultSchema = new ArrayList<>();
        defaultSchema.add(schemaName);

        CalciteSchema subSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus subSchemaPlus = subSchema.plus();
        subSchemaPlus.add("test", createTable());
        schemaPlus.add(schemaName, subSchemaPlus);

        CalciteSchema subSchema2 = CalciteSchema.createRootSchema(true);
        SchemaPlus subSchemaPlus2 = subSchema2.plus();
        schemaPlus.add("my_schema2", subSchemaPlus2);
        subSchemaPlus2.add("test2", createTable());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(new Properties());
        return new CalciteCatalogReader(rootSchema, defaultSchema, typeFactory, config);
    }

    static void addRules(VolcanoPlanner volcanoPlanner) {
        volcanoPlanner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRels(volcanoPlanner);
        // for (RelOptRule rule : Bindables.RULES) {
        // volcanoPlanner.addRule(rule);
        // }
        // volcanoPlanner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        // volcanoPlanner.addRule(ProjectTableScanRule.INSTANCE);
        // volcanoPlanner.addRule(ProjectTableScanRule.INTERPRETER);
        // for (RelOptRule rule : org.apache.calcite.prepare.CalcitePrepareImpl.ENUMERABLE_RULES)
        // volcanoPlanner.addRule(rule);
        // volcanoPlanner.addRule(org.apache.calcite.interpreter.NoneToBindableConverterRule.INSTANCE);
    }

    static HepPlanner createHepPlanner() {
        HepProgramBuilder builder = new HepProgramBuilder();
        // builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
        // builder.addRuleInstance(FilterJoinRule.JOIN);
        builder.addRuleCollection(Programs.CALC_RULES);
        // builder.addRuleCollection(Programs.RULE_SET);
        // builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE); // 加上这个可以把100+100变成200，常量折叠
        // builder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
        // builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);

        // HepMatchOrder order = HepMatchOrder.TOP_DOWN;
        // builder.addMatchOrder(order);
        // builder.addConverters(true);

        HepPlanner hepPlanner = new HepPlanner(builder.build());

        hepPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        hepPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        return hepPlanner;
    }

    static void testPrograms(RelNode relNode) {
        final RelOptPlanner planner = relNode.getCluster().getPlanner();
        final Program program = Programs.ofRules(ReduceExpressionsRule.PROJECT_INSTANCE);
        relNode = program.run(planner, relNode, relNode.getTraitSet());
        String plan = RelOptUtil.toString(relNode);
        System.out.println(plan);
    }

    static VolcanoPlanner createVolcanoPlanner() {
        RelOptCostFactory costFactory = RelOptCostImpl.FACTORY;
        Context externalContext = null;
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner(costFactory, externalContext);
        // RexExecutor rexExecutor = null;
        return volcanoPlanner;
    }

    static RelOptCluster createRelOptCluster(RelOptPlanner planner, RexBuilder rexBuilder) {
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        return cluster;
    }

    static RexBuilder createRexBuilder() {
        JavaTypeFactory javaTypeFactory = createJavaTypeFactory();
        RexBuilder rexBuilder = new RexBuilder(javaTypeFactory);
        return rexBuilder;
    }

    static JavaTypeFactory createJavaTypeFactory() {
        RelDataTypeSystem relDataTypeSystem = RelDataTypeSystem.DEFAULT;
        JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl(relDataTypeSystem);
        return javaTypeFactory;
    }
}
