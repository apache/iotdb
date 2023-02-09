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
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.ratis.thirdparty.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant.DEVICE;
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
                  new MeasurementPath(new PartialPath(DEVICE, false), TSDataType.TEXT)),
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
                  new MeasurementPath(new PartialPath(DEVICE, false), TSDataType.TEXT)),
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
                  new MeasurementPath(new PartialPath(DEVICE, false), TSDataType.TEXT)),
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
                  new MeasurementPath(new PartialPath(DEVICE, false), TSDataType.TEXT)),
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
        1,
        analysis
            .getDataPartitionInfo()
            .getDataPartitionMap()
            .get("root.sg")
            .get(new TSeriesPartitionSlot(1107))
            .size());
  }

  @Test
  public void testSelectIntoPath() throws IllegalPathException {
    List<String> sqls =
        Arrays.asList(
            "SELECT s1, s2 INTO root.sg_copy.d1(t1, t2), root.sg_copy.d2(t1, t2) FROM root.sg.d1, root.sg.d2;",
            "SELECT s1, s2 INTO root.sg_copy.d1(t1, t2, t3, t4) FROM root.sg.d1, root.sg.d2;",
            "SELECT s1, s2 INTO root.sg_copy.d1(t1), root.sg_copy.d2(t1, t2), root.sg_copy.d3(t1) FROM root.sg.d1, root.sg.d2;",
            "select count(s1 + s2), last_value(s2) into root.agg.count(s1_add_s2), root.agg.last_value(s2) from root.sg.d1 group by ([0, 100), 10ms);",
            "select s1 + s2 into root.expr.add(d1s1_d1s2, d1s1_d2s2, d2s1_d1s2, d2s1_d2s2) from root.sg.d1, root.sg.d2;",
            "select s1, s1 into root.sg_copy.d1(s1, s2)  from root.sg.d1",
            "select s1, s1 into root.sg_copy.d1(s1), root.sg_copy.d2(s1)  from root.sg.d1",
            "select s1, s2 into ::(t1, t1, t2, t2) from root.sg.*;",
            "select s1, s2 into root.sg_copy.::(::) from root.sg.*;",
            "select s1, s2 into root.sg_copy.d1_copy(${2}_${3}), root.sg_copy.d1_copy(${2}_${3}), root.sg_copy.d2_copy(${2}_${3}), root.sg_copy.d2_copy(${2}_${3}) from root.sg.d1, root.sg.d2;",
            "select d1.s1, d1.s2, d2.s1, d2.s2 into ::(s1_1, s2_2), root.sg.d2_2(s3_3), root.backup_${1}.::(s4) from root.sg",
            "select s1, s2 into root.sg_bk.new_d1(::) from root.sg.d1;");
    List<List<Pair<String, PartialPath>>> results =
        Arrays.asList(
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.t1")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg_copy.d1.t2")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg_copy.d2.t1")),
                new Pair("root.sg.d2.s2", new PartialPath("root.sg_copy.d2.t2"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.t1")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg_copy.d1.t2")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg_copy.d1.t3")),
                new Pair("root.sg.d2.s2", new PartialPath("root.sg_copy.d1.t4"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.t1")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg_copy.d2.t1")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg_copy.d2.t2")),
                new Pair("root.sg.d2.s2", new PartialPath("root.sg_copy.d3.t1"))),
            Arrays.asList(
                new Pair<>(
                    "count(root.sg.d1.s1 + root.sg.d1.s2)",
                    new PartialPath("root.agg.count.s1_add_s2")),
                new Pair<>("last_value(root.sg.d1.s2)", new PartialPath("root.agg.last_value.s2"))),
            Arrays.asList(
                new Pair(
                    "root.sg.d1.s1 + root.sg.d1.s2", new PartialPath("root.expr.add.d1s1_d1s2")),
                new Pair(
                    "root.sg.d1.s1 + root.sg.d2.s2", new PartialPath("root.expr.add.d1s1_d2s2")),
                new Pair(
                    "root.sg.d2.s1 + root.sg.d1.s2", new PartialPath("root.expr.add.d2s1_d1s2")),
                new Pair(
                    "root.sg.d2.s1 + root.sg.d2.s2", new PartialPath("root.expr.add.d2s1_d2s2"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.s1")),
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.s2"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.s1")),
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d2.s1"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg.d1.t1")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg.d2.t1")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg.d1.t2")),
                new Pair("root.sg.d2.s2", new PartialPath("root.sg.d2.t2"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1.s1")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg_copy.d2.s1")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg_copy.d1.s2")),
                new Pair("root.sg.d2.s2", new PartialPath("root.sg_copy.d2.s2"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_copy.d1_copy.d1_s1")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg_copy.d1_copy.d2_s1")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg_copy.d2_copy.d1_s2")),
                new Pair("root.sg.d2.s2", new PartialPath("root.sg_copy.d2_copy.d2_s2"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg.d1.s1_1")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg.d1.s2_2")),
                new Pair("root.sg.d2.s1", new PartialPath("root.sg.d2_2.s3_3")),
                new Pair("root.sg.d2.s2", new PartialPath("root.backup_sg.d2.s4"))),
            Arrays.asList(
                new Pair("root.sg.d1.s1", new PartialPath("root.sg_bk.new_d1.s1")),
                new Pair("root.sg.d1.s2", new PartialPath("root.sg_bk.new_d1.s2"))));

    for (int i = 0; i < sqls.size(); i++) {
      Analysis analysis = analyzeSQL(sqls.get(i));
      assert analysis != null;
      Assert.assertEquals(
          results.get(i), analysis.getIntoPathDescriptor().getSourceTargetPathPairList());
    }
  }

  @Test
  public void testSelectIntoPathAlignByDevice() throws IllegalPathException {
    List<String> sqls =
        Arrays.asList(
            "select s1, s2 into root.sg_copy.::(t1, t2) from root.sg.d1, root.sg.d2 align by device;",
            "select s1 + s2 into root.expr.add(d1s1_d1s2), root.expr.add(d2s1_d2s2) from root.sg.d1, root.sg.d2 align by device;",
            "select count(s1), last_value(s2) into root.agg.::(count_s1, last_value_s2) from root.sg.d1, root.sg.d2 group by ([0, 100), 10ms) align by device;",
            "select s1, s2 into root.sg1.new_d1(::), root.sg2.new_d2(::) from root.sg.d1, root.sg.d2 align by device;",
            "select s1, s2 into root.sg1.new_${2}(::) from root.sg.d1, root.sg.d2 align by device;");

    List<Map<String, List<Pair<String, PartialPath>>>> results = new ArrayList<>();
    Map<String, List<Pair<String, PartialPath>>> resultMap1 = new HashMap<>();
    resultMap1.put(
        "root.sg.d1",
        Arrays.asList(
            new Pair<>("s1", new PartialPath("root.sg_copy.d1.t1")),
            new Pair<>("s2", new PartialPath("root.sg_copy.d1.t2"))));
    resultMap1.put(
        "root.sg.d2",
        Arrays.asList(
            new Pair<>("s1", new PartialPath("root.sg_copy.d2.t1")),
            new Pair<>("s2", new PartialPath("root.sg_copy.d2.t2"))));
    results.add(resultMap1);

    Map<String, List<Pair<String, PartialPath>>> resultMap2 = new HashMap<>();
    resultMap2.put(
        "root.sg.d1",
        Collections.singletonList(
            new Pair<>("s1 + s2", new PartialPath("root.expr.add.d1s1_d1s2"))));
    resultMap2.put(
        "root.sg.d2",
        Collections.singletonList(
            new Pair<>("s1 + s2", new PartialPath("root.expr.add.d2s1_d2s2"))));
    results.add(resultMap2);

    Map<String, List<Pair<String, PartialPath>>> resultMap3 = new HashMap<>();
    resultMap3.put(
        "root.sg.d1",
        Arrays.asList(
            new Pair<>("count(s1)", new PartialPath("root.agg.d1.count_s1")),
            new Pair<>("last_value(s2)", new PartialPath("root.agg.d1.last_value_s2"))));
    resultMap3.put(
        "root.sg.d2",
        Arrays.asList(
            new Pair<>("count(s1)", new PartialPath("root.agg.d2.count_s1")),
            new Pair<>("last_value(s2)", new PartialPath("root.agg.d2.last_value_s2"))));
    results.add(resultMap3);

    Map<String, List<Pair<String, PartialPath>>> resultMap4 = new HashMap<>();
    resultMap4.put(
        "root.sg.d1",
        Arrays.asList(
            new Pair<>("s1", new PartialPath("root.sg1.new_d1.s1")),
            new Pair<>("s2", new PartialPath("root.sg1.new_d1.s2"))));
    resultMap4.put(
        "root.sg.d2",
        Arrays.asList(
            new Pair<>("s1", new PartialPath("root.sg2.new_d2.s1")),
            new Pair<>("s2", new PartialPath("root.sg2.new_d2.s2"))));
    results.add(resultMap4);

    Map<String, List<Pair<String, PartialPath>>> resultMap5 = new HashMap<>();
    resultMap5.put(
        "root.sg.d1",
        Arrays.asList(
            new Pair<>("s1", new PartialPath("root.sg1.new_d1.s1")),
            new Pair<>("s2", new PartialPath("root.sg1.new_d1.s2"))));
    resultMap5.put(
        "root.sg.d2",
        Arrays.asList(
            new Pair<>("s1", new PartialPath("root.sg1.new_d2.s1")),
            new Pair<>("s2", new PartialPath("root.sg1.new_d2.s2"))));
    results.add(resultMap5);

    for (int i = 0; i < sqls.size(); i++) {
      Analysis analysis = analyzeSQL(sqls.get(i));
      assert analysis != null;
      Assert.assertEquals(
          results.get(i),
          analysis.getDeviceViewIntoPathDescriptor().getDeviceToSourceTargetPathPairListMap());
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
      fail(sql + ", " + e.getMessage());
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
        expectedAnalysis.getCrossGroupByExpressions(), actualAnalysis.getCrossGroupByExpressions());
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
