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

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
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

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

import static org.junit.Assert.fail;

public class AnalyzeTest {

  @Test
  public void testRawDataQuery() {
    // TODO: @lmh add UTs
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
        Assert.assertEquals(predicates[i], queryStatement.getWhereCondition().getPredicate());
      }
    }
  }

  @Test
  public void testDataPartitionAnalyze() {
    Analysis analysis = analyzeSQL("insert into root.sg.d1(timestamp,s) values(1,10),(86401,11)");
    Assert.assertEquals(
        analysis
            .getDataPartitionInfo()
            .getDataPartitionMap()
            .get("root.sg")
            .get(new TSeriesPartitionSlot(8923))
            .size(),
        1);
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
}
