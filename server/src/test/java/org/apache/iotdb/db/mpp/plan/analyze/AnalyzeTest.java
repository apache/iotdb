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
import org.apache.iotdb.commons.path.MeasurementPath;
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
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;

import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.ratis.thirdparty.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant.COLUMN_DEVICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AnalyzeTest {

  @Test
  public void testRawDataQuery() {
    String sql = "select s1, status, s1 + 1 as t from root.sg.d1 where time > 100 and s2 > 10;";
    try {
      Analysis actualAnalysis = analyzeSQL(sql);

      Analysis expectedAnalysis = new Analysis();
      expectedAnalysis.setGlobalTimeFilter(TimeFilter.gt(100));
      expectedAnalysis.setSelectExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
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
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
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
                  new ColumnHeader("root.sg.d1.s2", TSDataType.DOUBLE, "root.sg.d1.status"),
                  new ColumnHeader("root.sg.d1.s1 + 1", TSDataType.DOUBLE, "t")),
              false));

      alignByTimeAnalysisEqualTest(actualAnalysis, expectedAnalysis);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAggregationQuery() {
    String sql =
        "select count(s1 + 1) + 1 as t from root.sg.d1 where time > 100 and s2 > 10 "
            + "group by ([0, 1000), 10ms) having sum(s2 + 1) + count(s1) > 100;";
    try {
      Analysis actualAnalysis = analyzeSQL(sql);

      Analysis expectedAnalysis = new Analysis();
      expectedAnalysis.setGlobalTimeFilter(
          new AndFilter(TimeFilter.gt(100), new GroupByFilter(10, 10, 0, 1000)));
      expectedAnalysis.setSelectExpressions(
          Sets.newHashSet(
              new AdditionExpression(
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setHavingExpression(
          new GreaterThanExpression(
              new AdditionExpression(
                  new FunctionExpression(
                      "sum",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
              new ConstantOperand(TSDataType.INT64, "100")));
      expectedAnalysis.setAggregationExpressions(
          Sets.newHashSet(
              new FunctionExpression(
                  "count",
                  new LinkedHashMap<>(),
                  Collections.singletonList(
                      new AdditionExpression(
                          new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                          new ConstantOperand(TSDataType.INT64, "1")))),
              new FunctionExpression(
                  "sum",
                  new LinkedHashMap<>(),
                  Collections.singletonList(
                      new AdditionExpression(
                          new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                          new ConstantOperand(TSDataType.INT64, "1")))),
              new FunctionExpression(
                  "count",
                  new LinkedHashMap<>(),
                  Collections.singletonList(
                      new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))));
      expectedAnalysis.setWhereExpression(
          new GreaterThanExpression(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
              new ConstantOperand(TSDataType.INT64, "10")));
      expectedAnalysis.setSourceTransformExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
              new AdditionExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                  new ConstantOperand(TSDataType.INT64, "1")),
              new AdditionExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setSourceExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2"))));
      expectedAnalysis.setRespDatasetHeader(
          new DatasetHeader(
              Collections.singletonList(
                  new ColumnHeader("count(root.sg.d1.s1 + 1) + 1", TSDataType.DOUBLE, "t")),
              false));

      alignByTimeAnalysisEqualTest(actualAnalysis, expectedAnalysis);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawDataQueryAlignByDevice() {
    String sql =
        "select s1, status, s2 + 1 from root.sg.* where time > 100 and s2 > 10 align by device;";
    try {
      Analysis actualAnalysis = analyzeSQL(sql);

      Analysis expectedAnalysis = new Analysis();
      expectedAnalysis.setGlobalTimeFilter(TimeFilter.gt(100));
      expectedAnalysis.setSelectExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(
                  new MeasurementPath(new PartialPath(COLUMN_DEVICE, false), TSDataType.TEXT)),
              new TimeSeriesOperand(new PartialPath("s1")),
              new TimeSeriesOperand(new PartialPath("s2")),
              new AdditionExpression(
                  new TimeSeriesOperand(new PartialPath("s2")),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setDeviceToSelectExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                      new ConstantOperand(TSDataType.INT64, "1"))),
              "root.sg.d2",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                      new ConstantOperand(TSDataType.INT64, "1")))));
      expectedAnalysis.setDeviceToWhereExpression(
          ImmutableMap.of(
              "root.sg.d1",
              new GreaterThanExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                  new ConstantOperand(TSDataType.INT64, "10")),
              "root.sg.d2",
              new GreaterThanExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                  new ConstantOperand(TSDataType.INT64, "10"))));
      expectedAnalysis.setDeviceToAggregationExpressions(ImmutableMap.of());
      expectedAnalysis.setDeviceToSourceTransformExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                      new ConstantOperand(TSDataType.INT64, "1"))),
              "root.sg.d2",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                      new ConstantOperand(TSDataType.INT64, "1")))));
      expectedAnalysis.setDeviceToSourceExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2"))),
              "root.sg.d2",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")))));
      expectedAnalysis.setDeviceViewInputIndexesMap(
          ImmutableMap.of(
              "root.sg.d1", Arrays.asList(1, 2, 3), "root.sg.d2", Arrays.asList(1, 2, 3)));
      expectedAnalysis.setDeviceViewOutputExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(
                  new MeasurementPath(new PartialPath(COLUMN_DEVICE, false), TSDataType.TEXT)),
              new TimeSeriesOperand(new PartialPath("s1")),
              new TimeSeriesOperand(new PartialPath("s2")),
              new AdditionExpression(
                  new TimeSeriesOperand(new PartialPath("s2")),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setRespDatasetHeader(
          new DatasetHeader(
              Arrays.asList(
                  new ColumnHeader("Device", TSDataType.TEXT),
                  new ColumnHeader("s1", TSDataType.INT32),
                  new ColumnHeader("s2", TSDataType.DOUBLE, "status"),
                  new ColumnHeader("s2 + 1", TSDataType.DOUBLE)),
              false));

      alignByDeviceAnalysisEqualTest(actualAnalysis, expectedAnalysis);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAggregationQueryAlignByDevice() {
    String sql =
        "select count(s1 + 1) + 1 from root.sg.* where time > 100 and s2 > 10 "
            + "group by ([0, 1000), 10ms) having sum(s2 + 1) + count(s1) > 100 align by device;";

    try {
      Analysis actualAnalysis = analyzeSQL(sql);

      Analysis expectedAnalysis = new Analysis();
      expectedAnalysis.setGlobalTimeFilter(
          new AndFilter(TimeFilter.gt(100), new GroupByFilter(10, 10, 0, 1000)));
      expectedAnalysis.setSelectExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(
                  new MeasurementPath(new PartialPath(COLUMN_DEVICE, false), TSDataType.TEXT)),
              new AdditionExpression(
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("s1")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new ConstantOperand(TSDataType.INT64, "1"))));
      expectedAnalysis.setHavingExpression(
          new GreaterThanExpression(
              new AdditionExpression(
                  new FunctionExpression(
                      "sum",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("s2")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(new TimeSeriesOperand(new PartialPath("s1"))))),
              new ConstantOperand(TSDataType.INT64, "100")));
      expectedAnalysis.setDeviceToSelectExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new AdditionExpression(
                      new FunctionExpression(
                          "count",
                          new LinkedHashMap<>(),
                          Collections.singletonList(
                              new AdditionExpression(
                                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                                  new ConstantOperand(TSDataType.INT64, "1")))),
                      new ConstantOperand(TSDataType.INT64, "1"))),
              "root.sg.d2",
              Sets.newHashSet(
                  new AdditionExpression(
                      new FunctionExpression(
                          "count",
                          new LinkedHashMap<>(),
                          Collections.singletonList(
                              new AdditionExpression(
                                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                                  new ConstantOperand(TSDataType.INT64, "1")))),
                      new ConstantOperand(TSDataType.INT64, "1")))));
      expectedAnalysis.setDeviceToWhereExpression(
          ImmutableMap.of(
              "root.sg.d1",
              new GreaterThanExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                  new ConstantOperand(TSDataType.INT64, "10")),
              "root.sg.d2",
              new GreaterThanExpression(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                  new ConstantOperand(TSDataType.INT64, "10"))));
      expectedAnalysis.setDeviceToAggregationExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new FunctionExpression(
                      "sum",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
              "root.sg.d2",
              Sets.newHashSet(
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new FunctionExpression(
                      "sum",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new AdditionExpression(
                              new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                              new ConstantOperand(TSDataType.INT64, "1")))),
                  new FunctionExpression(
                      "count",
                      new LinkedHashMap<>(),
                      Collections.singletonList(
                          new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")))))));
      expectedAnalysis.setDeviceToSourceTransformExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                      new ConstantOperand(TSDataType.INT64, "1")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")),
                      new ConstantOperand(TSDataType.INT64, "1"))),
              "root.sg.d2",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                      new ConstantOperand(TSDataType.INT64, "1")),
                  new AdditionExpression(
                      new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")),
                      new ConstantOperand(TSDataType.INT64, "1")))));
      expectedAnalysis.setDeviceToSourceExpressions(
          ImmutableMap.of(
              "root.sg.d1",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d1.s2"))),
              "root.sg.d2",
              Sets.newHashSet(
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s1")),
                  new TimeSeriesOperand(new PartialPath("root.sg.d2.s2")))));
      expectedAnalysis.setDeviceViewOutputExpressions(
          Sets.newHashSet(
              new TimeSeriesOperand(
                  new MeasurementPath(new PartialPath(COLUMN_DEVICE, false), TSDataType.TEXT)),
              new FunctionExpression(
                  "sum",
                  new LinkedHashMap<>(),
                  Collections.singletonList(
                      new AdditionExpression(
                          new TimeSeriesOperand(new PartialPath("s2")),
                          new ConstantOperand(TSDataType.INT64, "1")))),
              new FunctionExpression(
                  "count",
                  new LinkedHashMap<>(),
                  Collections.singletonList(new TimeSeriesOperand(new PartialPath("s1")))),
              new FunctionExpression(
                  "count",
                  new LinkedHashMap<>(),
                  Collections.singletonList(
                      new AdditionExpression(
                          new TimeSeriesOperand(new PartialPath("s1")),
                          new ConstantOperand(TSDataType.INT64, "1"))))));
      expectedAnalysis.setDeviceViewInputIndexesMap(
          ImmutableMap.of(
              "root.sg.d1", Arrays.asList(2, 3, 1), "root.sg.d2", Arrays.asList(2, 3, 1)));

      expectedAnalysis.setRespDatasetHeader(
          new DatasetHeader(
              Arrays.asList(
                  new ColumnHeader("Device", TSDataType.TEXT),
                  new ColumnHeader("count(s1 + 1) + 1", TSDataType.DOUBLE)),
              false));

      alignByDeviceAnalysisEqualTest(actualAnalysis, expectedAnalysis);
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

  private void alignByTimeAnalysisEqualTest(Analysis actualAnalysis, Analysis expectedAnalysis) {
    commonAnalysisEqualTest(expectedAnalysis, actualAnalysis);
    assertEquals(expectedAnalysis.getSourceExpressions(), actualAnalysis.getSourceExpressions());
    assertEquals(
        expectedAnalysis.getSourceTransformExpressions(),
        actualAnalysis.getSourceTransformExpressions());
    assertEquals(expectedAnalysis.getWhereExpression(), actualAnalysis.getWhereExpression());
    assertEquals(
        expectedAnalysis.getAggregationExpressions(), actualAnalysis.getAggregationExpressions());
    assertEquals(
        expectedAnalysis.getGroupByLevelExpressions(), actualAnalysis.getGroupByLevelExpressions());
  }

  private void alignByDeviceAnalysisEqualTest(Analysis actualAnalysis, Analysis expectedAnalysis) {
    commonAnalysisEqualTest(expectedAnalysis, actualAnalysis);
    assertEquals(
        expectedAnalysis.getDeviceToSourceExpressions(),
        actualAnalysis.getDeviceToSourceExpressions());
    assertEquals(
        expectedAnalysis.getDeviceToSourceTransformExpressions(),
        actualAnalysis.getDeviceToSourceTransformExpressions());
    assertEquals(
        expectedAnalysis.getDeviceToWhereExpression(), actualAnalysis.getDeviceToWhereExpression());
    assertEquals(
        expectedAnalysis.getDeviceToAggregationExpressions(),
        actualAnalysis.getDeviceToAggregationExpressions());
    assertEquals(
        expectedAnalysis.getDeviceToSelectExpressions(),
        actualAnalysis.getDeviceToSelectExpressions());
    assertEquals(
        expectedAnalysis.getDeviceViewOutputExpressions(),
        actualAnalysis.getDeviceViewOutputExpressions());
    assertEquals(
        expectedAnalysis.getDeviceViewInputIndexesMap(),
        actualAnalysis.getDeviceViewInputIndexesMap());
  }

  private void commonAnalysisEqualTest(Analysis actualAnalysis, Analysis expectedAnalysis) {
    assertEquals(expectedAnalysis.getSelectExpressions(), actualAnalysis.getSelectExpressions());
    assertEquals(expectedAnalysis.getHavingExpression(), actualAnalysis.getHavingExpression());
    assertEquals(expectedAnalysis.getRespDatasetHeader(), actualAnalysis.getRespDatasetHeader());
    assertEquals(expectedAnalysis.getGlobalTimeFilter(), actualAnalysis.getGlobalTimeFilter());
  }
}
