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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.timecho.iotdb.fuzzy;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.DataNodeSqlFormatter;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Random;

import static org.apache.iotdb.db.it.utils.TestUtils.tableFuzzyTestVerify;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBGrammarBasedTableIT {

  private static final String DATABASE_NAME = "test";

  private static final String[] sql =
      new String[] {
        "create database test",
        "use test",
        "create table table1(device string tag, s1 int32 field, s2 float field, s3 boolean field)",
        "insert into table1(time,device,s1,s2,s3) values(2020-01-01 00:00:01.000,'d1',1, 1.0, true)",
        "insert into table1(time,device,s1,s2,s3) values(2020-01-01 00:00:03.000,'d1',3, 3.0, false)",
        "insert into table1(time,device,s1,s2,s3) values(2020-01-01 00:00:05.000,'d1',5, 5.0, true)",
        "insert into table1(time,device,s1,s2,s3) values(2020-01-01 00:00:08.000,'d2',8, 8.0, false)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sql) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // seed sql: select s1 from table1 where time = 2020-01-01 00:00:01.000
  @Test
  public void rawDataTimeEqualFilterTest() {
    Node root = buildAST("select s1 from table1 where time = 2020-01-01 00:00:01.000");
    RawDataTimeEqualFilterFuzzier fuzzier = new RawDataTimeEqualFilterFuzzier();

    for (int i = 0; i < 10; i++) {
      fuzzier.process(root);
      tableFuzzyTestVerify(constructSql(root), DATABASE_NAME);
    }
  }

  private static class RawDataTimeEqualFilterFuzzier extends DefaultTraversalVisitor<Void> {

    private final Random random = new Random(System.currentTimeMillis());

    @Override
    public Void visitComparisonExpression(ComparisonExpression node, Void context) {
      if (node.getOperator() == ComparisonExpression.Operator.EQUAL) {
        if (node.getRight() instanceof LongLiteral) {
          node.setRight(new LongLiteral(String.valueOf(random.nextLong())));
        }
      }
      return null;
    }
  }

  // seed sql: select s1 from table1 where time >= 2020-01-01 00:00:01.000 and time <= 2020-01-01
  // 00:00:08.000
  @Test
  public void rawDataTimeRangeFilterTest() {
    Node root =
        buildAST(
            "select s1 from table1 where time >= 2020-01-01 00:00:01.000 and time <= 2020-01-01 00:00:08.000");

    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 10; i++) {
      long left = random.nextLong();
      RawDataTimeRangeFilterFuzzier fuzzier =
          new RawDataTimeRangeFilterFuzzier(left, left + 8_000L);
      fuzzier.process(root);
      tableFuzzyTestVerify(constructSql(root), DATABASE_NAME);
    }
  }

  private static class RawDataTimeRangeFilterFuzzier extends DefaultTraversalVisitor<Void> {

    private final long left;
    private final long right;

    public RawDataTimeRangeFilterFuzzier(long left, long right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public Void visitComparisonExpression(ComparisonExpression node, Void context) {
      if (node.getOperator() == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL) {
        if (node.getRight() instanceof LongLiteral) {
          node.setRight(new LongLiteral(String.valueOf(right)));
        }
      } else if (node.getOperator() == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL) {
        if (node.getRight() instanceof LongLiteral) {
          node.setRight(new LongLiteral(String.valueOf(left)));
        }
      }
      return null;
    }
  }

  // seed sql: select s1 from table1 where s1 = 1
  @Test
  public void rawDataValueFilterTest() {
    Node root = buildAST("select s1 from table1 where s1 = 1");
    RawDataValueFilterFuzzier fuzzier = new RawDataValueFilterFuzzier();

    for (int i = 0; i < 10; i++) {
      fuzzier.process(root);
      tableFuzzyTestVerify(constructSql(root), DATABASE_NAME);
    }
  }

  private static class RawDataValueFilterFuzzier extends DefaultTraversalVisitor<Void> {

    private final Random random = new Random(System.currentTimeMillis());

    @Override
    public Void visitComparisonExpression(ComparisonExpression node, Void context) {
      if (node.getOperator() == ComparisonExpression.Operator.EQUAL) {
        if (node.getRight() instanceof LongLiteral) {
          node.setRight(new LongLiteral(String.valueOf(random.nextInt(9))));
        }
      }
      return null;
    }
  }

  // seed sql: select device, date_bin(1s, time), max(s1) from table1 group by 1, 2
  @Test
  public void downSamplingTest() {
    Node root = buildAST("select device, date_bin(1s, time), max(s1) from table1 group by 1, 2");

    for (int i = 0; i < 10; i++) {
      DownSamplingFuzzier fuzzier = new DownSamplingFuzzier();
      fuzzier.process(root);
      tableFuzzyTestVerify(constructSql(root), DATABASE_NAME);
    }
  }

  private static class DownSamplingFuzzier extends DefaultTraversalVisitor<Void> {

    private final Random random = new Random(System.currentTimeMillis());

    @Override
    public Void visitFunctionCall(FunctionCall node, Void context) {
      if (TableBuiltinScalarFunction.DATE_BIN
          .getFunctionName()
          .equalsIgnoreCase(node.getName().toString())) {
        node.getArguments().set(1, new LongLiteral(String.valueOf(random.nextInt(10000))));
      }
      return null;
    }
  }

  // seed sql: select device, last(time), last_by(s1, time)  from table1 group by device
  @Test
  public void latestPointTest() {
    Node root =
        buildAST("select device, last(time), last_by(s1, time)  from table1 group by device");

    for (int i = 0; i < 10; i++) {
      LatestPointFuzzier fuzzier = new LatestPointFuzzier();
      fuzzier.process(root);
      tableFuzzyTestVerify(constructSql(root), DATABASE_NAME);
    }
  }

  private static class LatestPointFuzzier extends DefaultTraversalVisitor<Void> {

    private static final String COLUMN_NAME = "s%d";

    private final Random random = new Random(System.currentTimeMillis());

    @Override
    public Void visitFunctionCall(FunctionCall node, Void context) {
      if (TableBuiltinAggregationFunction.LAST_BY
          .getFunctionName()
          .equalsIgnoreCase(node.getName().toString())) {
        node.getArguments()
            .set(0, new Identifier(String.format(COLUMN_NAME, random.nextInt(3) + 1)));
      }
      return null;
    }
  }

  private Node buildAST(String sql) {
    SqlParser sqlParser = new SqlParser();
    return sqlParser.createStatement(
        sql, ZoneId.systemDefault(), new InternalClientSession("testClient"));
  }

  private String constructSql(Node node) {
    String sql = DataNodeSqlFormatter.formatDataNodeSql(node);
    System.out.println(sql);
    return sql;
  }
}
