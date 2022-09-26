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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.ratis.thirdparty.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AnalyzeTest {

  @Test
  public void testRawDataQuery() {
    String sql = "select s1, status, s1 + 1 as t from root.sg.d1 where s2 > 10;";
    try {
      Analysis actualAnalysis = analyzeSQL(sql);

      Analysis expectedAnalysis = new Analysis();
      expectedAnalysis.setSelectExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
              new AdditionExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setWhereExpression(
          new GreaterThanExpression(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
              new ConstantOperand(TSDataType.INT64, "10")));
      expectedAnalysis.setSourceTransformExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
              new AdditionExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setSourceExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2"))));
      expectedAnalysis.setRespDatasetHeader(
          new DatasetHeader(
              Arrays.asList(
                  new ColumnHeader("root.sg.d1.s1", TSDataType.INT32),
                  new ColumnHeader("root.sg.d1.s1", TSDataType.INT32, "status"),
                  new ColumnHeader("root.sg.d1.s1 + 1", TSDataType.INT32, "t")),
              false));
      alignByTimeAnalysisEqualTest(actualAnalysis, expectedAnalysis);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTimeFilterRewrite() throws IllegalPathException {
    // Test whether time filter has been removed after extracting
    String[] sqls =
        new String[] {
          "select s1 from root.sg.d1 where time>1 and time<3",
          "select s1 from root.sg.d1 where time>1 and time<3 or time>100",
          "select s1 from root.sg.d1 where time>1 or time<3",
          "select s1 from root.sg.d1 where time>1 and time<3 and s1>1",
          "select s1 from root.sg.d1 where time>1 and time<3 or s1>1",
          "select s1 from root.sg.d1 where time>1 or time<3 or time >5",
          "select s1 from root.sg.d1 where time>1 and time<3 and s1>1 and time>4",
          "select s1 from root.sg.d1 where time>1 and time<3 or time >4 and time>5",
          "select s1 from root.sg.d1 where time>1 or time<3 and s1>1",
          "select s1 from root.sg.d1 where time>1 or time<3 or s1>1",
          "select s1 from root.sg.d1 where time>1 or s1>1 and time<3",
          "select s1 from root.sg.d1 where time>1 or s1>1 or time<3",
        };

    Expression[] predicates =
        new Expression[] {
          null,
          null,
          null,
          new GreaterThanExpression(
              new TimeSeriesOperand(new PartialPath("s1")),
              new ConstantOperand(TSDataType.INT32, "1")),
          new LogicOrExpression(
              new LogicAndExpression(
                  new GreaterThanExpression(
                      new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "1")),
                  new LessThanExpression(
                      new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "3"))),
              new GreaterThanExpression(
                  new TimeSeriesOperand(new PartialPath("s1")),
                  new ConstantOperand(TSDataType.INT32, "1"))),
          null,
          new GreaterThanExpression(
              new TimeSeriesOperand(new PartialPath("s1")),
              new ConstantOperand(TSDataType.INT32, "1")),
          null,
          new LogicOrExpression(
              new GreaterThanExpression(
                  new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "1")),
              new LogicAndExpression(
                  new LessThanExpression(
                      new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "3")),
                  new GreaterThanExpression(
                      new TimeSeriesOperand(new PartialPath("s1")),
                      new ConstantOperand(TSDataType.INT32, "1")))),
          new LogicOrExpression(
              new GreaterThanExpression(
                  new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "1")),
              new LogicOrExpression(
                  new LessThanExpression(
                      new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "3")),
                  new GreaterThanExpression(
                      new TimeSeriesOperand(new PartialPath("s1")),
                      new ConstantOperand(TSDataType.INT32, "1")))),
          new LogicOrExpression(
              new GreaterThanExpression(
                  new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "1")),
              new LogicAndExpression(
                  new GreaterThanExpression(
                      new TimeSeriesOperand(new PartialPath("s1")),
                      new ConstantOperand(TSDataType.INT32, "1")),
                  new LessThanExpression(
                      new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "3")))),
          new LogicOrExpression(
              new GreaterThanExpression(
                  new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "1")),
              new LogicOrExpression(
                  new GreaterThanExpression(
                      new TimeSeriesOperand(new PartialPath("s1")),
                      new ConstantOperand(TSDataType.INT32, "1")),
                  new LessThanExpression(
                      new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "3")))),
        };

    for (int i = 0; i < sqls.length; i++) {
      Analysis analysis = analyzeSQL(sqls[i]);
      QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
      if (predicates[i] == null) {
        Assert.assertNull(queryStatement.getWhereCondition());
      } else {
        assertEquals(predicates[i], queryStatement.getWhereCondition().getPredicate());
      }
    }
  }

  private Analysis analyzeSQL(String sql) {
    try {
      Statement statement =
          StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
      MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
      Analyzer analyzer =
          new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
      return analyzer.analyze(statement);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    fail();
    return null;
  }

  private void alignByTimeAnalysisEqualTest(Analysis actualAnalysis, Analysis expectedAnalysis) {
    commonAnalysisEqualTest(actualAnalysis, expectedAnalysis);
    assertEquals(actualAnalysis.getSourceExpressions(), expectedAnalysis.getSourceExpressions());
    assertEquals(
        actualAnalysis.getSourceTransformExpressions(),
        expectedAnalysis.getSourceTransformExpressions());
    assertEquals(actualAnalysis.getWhereExpression(), expectedAnalysis.getWhereExpression());
    assertEquals(
        actualAnalysis.getAggregationExpressions(), expectedAnalysis.getAggregationExpressions());
    assertEquals(
        actualAnalysis.getGroupByLevelExpressions(), expectedAnalysis.getGroupByLevelExpressions());
  }

  private void alignByDeviceAnalysisEqualTest(Analysis actualAnalysis, Analysis expectedAnalysis) {
    commonAnalysisEqualTest(actualAnalysis, expectedAnalysis);
    assertEquals(
        actualAnalysis.getDeviceToSourceExpressions(),
        expectedAnalysis.getDeviceToSourceExpressions());
    assertEquals(
        actualAnalysis.getDeviceToSourceTransformExpressions(),
        expectedAnalysis.getDeviceToSourceTransformExpressions());
    assertEquals(
        actualAnalysis.getDeviceToWhereExpression(), expectedAnalysis.getDeviceToWhereExpression());
    assertEquals(
        actualAnalysis.getDeviceToAggregationExpressions(),
        expectedAnalysis.getDeviceToWhereExpression());
    assertEquals(
        actualAnalysis.getDeviceToSelectExpressions(),
        expectedAnalysis.getDeviceToSelectExpressions());
    assertEquals(
        actualAnalysis.getDeviceViewInputIndexesMap(),
        expectedAnalysis.getDeviceViewInputIndexesMap());
    assertEquals(
        actualAnalysis.getDeviceViewOutputExpressions(),
        actualAnalysis.getDeviceViewOutputExpressions());
  }

  private void commonAnalysisEqualTest(Analysis actualAnalysis, Analysis expectedAnalysis) {
    assertEquals(expectedAnalysis.getSelectExpressions(), actualAnalysis.getSelectExpressions());
    assertEquals(expectedAnalysis.getHavingExpression(), actualAnalysis.getHavingExpression());
    assertEquals(expectedAnalysis.getRespDatasetHeader(), actualAnalysis.getRespDatasetHeader());
    assertEquals(expectedAnalysis.getGlobalTimeFilter(), actualAnalysis.getGlobalTimeFilter());
  }
}
