/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.Accumulator;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.RawDataAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.window.SessionWindowParameter;
import org.apache.iotdb.db.mpp.execution.operator.window.TimeWindowParameter;
import org.apache.iotdb.db.mpp.execution.operator.window.VariationWindowParameter;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RawDataAggregationOperatorTest {

  private static final String AGGREGATION_OPERATOR_TEST_SG = "root.RawDataAggregationOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();
  private ExecutorService instanceNotificationExecutor;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, AGGREGATION_OPERATOR_TEST_SG);
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
    instanceNotificationExecutor.shutdown();
  }

  /**
   * Test aggregating raw data without group by interval.
   *
   * <p>Example SQL: select count(s0), sum(s0), min_time(s0), max_time(s0), min_value(s0),
   * max_value(s0), count(s1), sum(s1), min_time(s1), max_time(s1), min_value(s1), max_value(s1)
   * from root.sg.d0 where s1 > 10 and s2 < 15
   *
   * <p>For convenience, we don't use FilterOperator in test though rawDataAggregateOperator is
   * always used with value filter.
   */
  @Test
  public void aggregateRawDataTest1() throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      aggregationTypes.add(TAggregationType.SUM);
      aggregationTypes.add(TAggregationType.MIN_TIME);
      aggregationTypes.add(TAggregationType.MAX_TIME);
      aggregationTypes.add(TAggregationType.MAX_VALUE);
      aggregationTypes.add(TAggregationType.MIN_VALUE);
      for (int j = 0; j < 6; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }

    WindowParameter windowParameter = new TimeWindowParameter(false);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int i = 0; i < 2; i++) {
        assertEquals(500, resultTsBlock.getColumn(6 * i).getLong(0));
        assertEquals(6524750.0, resultTsBlock.getColumn(6 * i + 1).getDouble(0), 0.0001);
        assertEquals(0, resultTsBlock.getColumn(6 * i + 2).getLong(0));
        assertEquals(499, resultTsBlock.getColumn(6 * i + 3).getLong(0));
        assertEquals(20199, resultTsBlock.getColumn(6 * i + 4).getInt(0));
        assertEquals(260, resultTsBlock.getColumn(6 * i + 5).getInt(0));
      }
      count++;
    }
    assertEquals(1, count);
  }

  /**
   * Test aggregating raw data without group by interval.
   *
   * <p>Example SQL: select avg(s0), avg(s1), first_value(s0), first_value(s1), last_value(s0),
   * last_value(s1) from root.sg.d0 where s1 > 10 and s2 < 15
   *
   * <p>For convenience, we don't use FilterOperator in test though rawDataAggregateOperator is
   * always used with value filter.
   */
  @Test
  public void aggregateRawDataTest2() throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.AVG);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    WindowParameter windowParameter = new TimeWindowParameter(false);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int i = 0; i < 2; i++) {
        assertEquals(13049.5, resultTsBlock.getColumn(i).getDouble(0), 0.001);
      }
      for (int i = 2; i < 4; i++) {
        assertEquals(20000, resultTsBlock.getColumn(i).getInt(0));
      }
      for (int i = 4; i < 6; i++) {
        assertEquals(10499, resultTsBlock.getColumn(i).getInt(0));
      }
      count++;
    }
    assertEquals(1, count);
  }

  /** Test aggregating raw data by time interval. */
  @Test
  public void groupByTimeRawDataTest1() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {100, 100, 100, 99},
          {2004950, 2014950, 624950, 834551},
          {0, 100, 200, 300},
          {99, 199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      aggregationTypes.add(TAggregationType.SUM);
      aggregationTypes.add(TAggregationType.MIN_TIME);
      aggregationTypes.add(TAggregationType.MAX_TIME);
      aggregationTypes.add(TAggregationType.MAX_VALUE);
      aggregationTypes.add(TAggregationType.MIN_VALUE);
      for (int j = 0; j < 6; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);

    WindowParameter windowParameter = new TimeWindowParameter(false);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(
            aggregationTypes, groupByTimeParameter, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(row));
        for (int i = 0; i < 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(6 * i).getLong(row));
          assertEquals(result[1][count], resultTsBlock.getColumn(6 * i + 1).getDouble(row), 0.0001);
          assertEquals(result[2][count], resultTsBlock.getColumn(6 * i + 2).getLong(row));
          assertEquals(result[3][count], resultTsBlock.getColumn(6 * i + 3).getLong(row));
          assertEquals(result[4][count], resultTsBlock.getColumn(6 * i + 4).getInt(row));
          assertEquals(result[5][count], resultTsBlock.getColumn(6 * i + 5).getInt(row));
        }
      }
    }
    assertEquals(4, count);
  }

  /** Test aggregating raw data by time interval. */
  @Test
  public void groupByTimeRawDataTest2() throws IllegalPathException {
    double[][] result =
        new double[][] {
          {20049.5, 20149.5, 6249.5, 8429.808},
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398},
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.AVG);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);

    WindowParameter windowParameter = new TimeWindowParameter(false);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(
            aggregationTypes, groupByTimeParameter, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }

      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(row));
        for (int i = 0; i < 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(i).getDouble(row), 0.001);
        }
        for (int i = 2; i < 4; i++) {
          assertEquals((int) result[1][count], resultTsBlock.getColumn(i).getInt(row));
        }
        for (int i = 4; i < 6; i++) {
          assertEquals((int) result[2][count], resultTsBlock.getColumn(i).getInt(row));
        }
      }
    }
    assertEquals(4, count);
  }

  /** Test by time interval with EndTime */
  @Test
  public void groupByTimeRawDataTest3() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {100, 100, 100, 99},
          {2004950, 2014950, 624950, 834551},
          {0, 100, 200, 300},
          {99, 199, 299, 398},
          {20099, 20199, 10259, 10379},
          {20000, 20100, 260, 380},
          {20000, 20100, 10200, 10300},
          {20099, 20199, 299, 398}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      aggregationTypes.add(TAggregationType.SUM);
      aggregationTypes.add(TAggregationType.MIN_TIME);
      aggregationTypes.add(TAggregationType.MAX_TIME);
      aggregationTypes.add(TAggregationType.MAX_VALUE);
      aggregationTypes.add(TAggregationType.MIN_VALUE);
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      for (int j = 0; j < 8; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 399, 100, 100, true);

    WindowParameter windowParameter = new TimeWindowParameter(true);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(
            aggregationTypes, groupByTimeParameter, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }

      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(row));
        // endTime
        long endTime = 100L * count + 99;
        if (count == 3) {
          endTime = 398;
        }
        assertEquals(endTime, resultTsBlock.getColumn(0).getLong(row));
        for (int i = 0; i < 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(8 * i + 1).getLong(row));
          assertEquals(result[1][count], resultTsBlock.getColumn(8 * i + 2).getDouble(row), 0.0001);
          assertEquals(result[2][count], resultTsBlock.getColumn(8 * i + 3).getLong(row));
          assertEquals(result[3][count], resultTsBlock.getColumn(8 * i + 4).getLong(row));
          assertEquals(result[4][count], resultTsBlock.getColumn(8 * i + 5).getInt(row));
          assertEquals(result[5][count], resultTsBlock.getColumn(8 * i + 6).getInt(row));
          assertEquals(result[6][count], resultTsBlock.getColumn(8 * i + 7).getInt(row));
          assertEquals(result[7][count], resultTsBlock.getColumn(8 * i + 8).getInt(row));
        }
      }
    }
    assertEquals(4, count);
  }

  /** 0 - 99 100 - 199 200 - 299 300 - 399 400 - 499 500 - 599 */
  @Test
  public void groupByTimeRawDataTest4() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {100, 100, 100, 100, 100, 0},
          {20099, 20199, 10259, 10379, 10499},
          {20000, 20100, 260, 380, 10400},
          {20000, 20100, 10200, 10300, 10400},
          {20099, 20199, 299, 399, 10499}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      aggregationTypes.add(TAggregationType.MAX_VALUE);
      aggregationTypes.add(TAggregationType.MIN_VALUE);
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      for (int j = 0; j < 8; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 600, 100, 100, true);

    WindowParameter windowParameter = new TimeWindowParameter(true);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(
            aggregationTypes, groupByTimeParameter, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(100 * count, resultTsBlock.getTimeColumn().getLong(row));
        // endTime
        assertEquals(100L * count + 99, resultTsBlock.getColumn(0).getLong(row));
        for (int i = 0; i < 2; i++) {
          if (count == 5) {
            assertEquals(result[0][count], resultTsBlock.getColumn(5 * i + 1).getLong(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 2).isNull(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 3).isNull(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 4).isNull(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 5).isNull(row));
            continue;
          }
          assertEquals(result[0][count], resultTsBlock.getColumn(5 * i + 1).getLong(row));
          assertEquals(result[1][count], resultTsBlock.getColumn(5 * i + 2).getInt(row));
          assertEquals(result[2][count], resultTsBlock.getColumn(5 * i + 3).getInt(row));
          assertEquals(result[3][count], resultTsBlock.getColumn(5 * i + 4).getInt(row));
          assertEquals(result[4][count], resultTsBlock.getColumn(5 * i + 5).getInt(row));
        }
      }
    }
    assertEquals(6, count);
  }

  /**
   * test the situation when leftCRightO is false. 1 - 100 101 - 200 201 - 300 301 - 400 401 - 500
   * 501 - 600
   */
  @Test
  public void groupByTimeRawDataTest5() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {100, 100, 100, 100, 99, 0},
          {20100, 20199, 10300, 10400, 10499}, // max
          {20001, 10200, 260, 380, 10401}, // min
          {20001, 20101, 10201, 10301, 10401}, // first
          {20100, 10200, 10300, 10400, 10499} // last
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      aggregationTypes.add(TAggregationType.MAX_VALUE);
      aggregationTypes.add(TAggregationType.MIN_VALUE);
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      for (int j = 0; j < 8; j++) {
        List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
        inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
        inputLocations.add(inputLocationForOneAggregator);
      }
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(0, 600, 100, 100, false);

    WindowParameter windowParameter = new TimeWindowParameter(false);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(
            aggregationTypes, groupByTimeParameter, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(100 * (count + 1), resultTsBlock.getTimeColumn().getLong(row));
        for (int i = 0; i < 2; i++) {
          if (count == 5) {
            assertEquals(result[0][count], resultTsBlock.getColumn(5 * i).getLong(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 1).isNull(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 2).isNull(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 3).isNull(row));
            assertTrue(resultTsBlock.getColumn(5 * i + 4).isNull(row));
            continue;
          }
          assertEquals(result[0][count], resultTsBlock.getColumn(5 * i).getLong(row));
          assertEquals(result[1][count], resultTsBlock.getColumn(5 * i + 1).getInt(row));
          assertEquals(result[2][count], resultTsBlock.getColumn(5 * i + 2).getInt(row));
          assertEquals(result[3][count], resultTsBlock.getColumn(5 * i + 3).getInt(row));
          assertEquals(result[4][count], resultTsBlock.getColumn(5 * i + 4).getInt(row));
        }
      }
    }
    assertEquals(6, count);
  }

  /** 0 - 259 260 - 299 `300 - 499 */
  @Test
  public void groupByEventRawDataTest1() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {0, 260, 300},
          {259, 299, 499},
          {20000, 260, 10300},
          {10259, 299, 10499}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.MIN_TIME);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.MAX_TIME);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    WindowParameter windowParameter =
        new VariationWindowParameter(TSDataType.INT32, 0, false, true, 10000);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        for (int i = 0; i < 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(i).getLong(row));
        }
        for (int i = 2; i < 4; i++) {
          assertEquals(result[1][count], resultTsBlock.getColumn(i).getLong(row));
        }
        for (int i = 4; i < 6; i++) {
          assertEquals(result[2][count], resultTsBlock.getColumn(i).getInt(row));
        }
        for (int i = 6; i < 8; i++) {
          assertEquals(result[3][count], resultTsBlock.getColumn(i).getInt(row));
        }
      }
    }
    assertEquals(3, count);
  }

  @Test
  public void groupByEventRawDataTest2() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {4019900, 613770, 11180, 827160, 7790, 1044950},
          {200, 60, 40, 80, 20, 100},
          {20000, 10200, 260, 10300, 380, 10400}
        };
    long[][] resultTime =
        new long[][] {
          {0, 200, 260, 300, 380, 400},
          {199, 259, 299, 379, 399, 499}
        };
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.SUM);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    WindowParameter windowParameter =
        new VariationWindowParameter(TSDataType.INT32, 0, true, true, 5000);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(resultTime[0][count], resultTsBlock.getTimeByIndex(row));
        assertEquals(resultTime[1][count], resultTsBlock.getColumn(0).getLong(row));
        for (int i = 1; i <= 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(i).getDouble(row), 0.01);
        }
        for (int i = 3; i <= 4; i++) {
          assertEquals(result[1][count], resultTsBlock.getColumn(i).getLong(row));
        }
        for (int i = 5; i <= 6; i++) {
          assertEquals(result[2][count], resultTsBlock.getColumn(i).getInt(row));
        }
      }
    }
    assertEquals(6, count);
  }

  @Test
  public void groupByEventRawDataTest3() throws IllegalPathException {
    int[][] result =
        new int[][] {
          {4019900, 613770, 11180, 827160, 7790, 1044950},
          {200, 60, 40, 80, 20, 100},
          {20000, 10200, 260, 10300, 380, 10400}
        };
    long[] resultTime = new long[] {0, 200, 260, 300, 380, 400};
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.SUM);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    WindowParameter windowParameter =
        new VariationWindowParameter(TSDataType.INT32, 0, false, true, 5000);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(resultTime[count], resultTsBlock.getTimeByIndex(row));
        for (int i = 0; i < 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(i).getDouble(row), 0.01);
        }
        for (int i = 2; i < 4; i++) {
          assertEquals(result[1][count], resultTsBlock.getColumn(i).getLong(row));
        }
        for (int i = 4; i < 6; i++) {
          assertEquals(result[2][count], resultTsBlock.getColumn(i).getInt(row));
        }
      }
    }
    assertEquals(6, count);
  }
  /** 0 - 199 200 - 259 260 - 299 300 - 379 380 - 399 400 - 499 */
  @Test
  public void groupByEventRawDataTest4() throws IllegalPathException {
    int[] result =
        new int[] {
          20000, 10200, 260, 10300, 380, 10400,
        };
    long[][] resultTime =
        new long[][] {
          {0, 200, 260, 300, 380, 400},
          {199, 259, 299, 379, 399, 499}
        };

    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    WindowParameter windowParameter =
        new VariationWindowParameter(TSDataType.INT32, 0, true, true, 5000);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        assertEquals(resultTime[0][count], resultTsBlock.getTimeByIndex(row));
        assertEquals(resultTime[1][count], resultTsBlock.getColumn(0).getLong(row));
        for (int i = 1; i <= 2; i++) {
          assertEquals(result[count], resultTsBlock.getColumn(i).getInt(row));
        }
      }
    }
    assertEquals(6, count);
  }

  @Test
  public void onePointInOneEqualEventWindowTest() throws IllegalPathException {
    WindowParameter windowParameter =
        new VariationWindowParameter(TSDataType.INT32, 0, false, true, 0);
    onePointInOneWindowTest(windowParameter);
  }

  @Test
  public void onePointInOneVariationEventWindowTest() throws IllegalPathException {
    WindowParameter windowParameter =
        new VariationWindowParameter(TSDataType.INT32, 0, false, true, 0.5);
    onePointInOneWindowTest(windowParameter);
  }

  private void onePointInOneWindowTest(WindowParameter windowParameter)
      throws IllegalPathException {
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.COUNT);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.MIN_TIME);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);

    int resultMinTime1 = -1, resultMinTime2 = -1;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int i = 0; i < 2; i++) {
        for (int j = 0; j < resultTsBlock.getColumn(i).getPositionCount(); j++) {
          assertEquals(1, resultTsBlock.getColumn(i).getLong(j));
        }
      }
      // Here, a resultTsBlock has many aggregation results instead of one.
      for (int i = 2; i < 4; i++) {
        if (i == 2) {
          for (int j = 0; j < resultTsBlock.getColumn(i).getPositionCount(); j++) {
            assertEquals(++resultMinTime1, resultTsBlock.getColumn(i).getLong(j));
          }
        } else {
          for (int j = 0; j < resultTsBlock.getColumn(i).getPositionCount(); j++) {
            assertEquals(++resultMinTime2, resultTsBlock.getColumn(i).getLong(j));
          }
        }
      }
    }
    assertEquals(resultMinTime1, 499);
    assertEquals(resultMinTime2, 499);
  }

  @Test
  public void groupBySessionRawDataTest1() throws IllegalPathException {
    int[][] result = new int[][] {{0}, {499}, {20000}, {10499}};
    List<TAggregationType> aggregationTypes = new ArrayList<>();
    List<List<InputLocation[]>> inputLocations = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.MIN_TIME);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.MAX_TIME);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.FIRST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }
    for (int i = 0; i < 2; i++) {
      aggregationTypes.add(TAggregationType.LAST_VALUE);
      List<InputLocation[]> inputLocationForOneAggregator = new ArrayList<>();
      inputLocationForOneAggregator.add(new InputLocation[] {new InputLocation(0, i)});
      inputLocations.add(inputLocationForOneAggregator);
    }

    WindowParameter windowParameter = new SessionWindowParameter(2, false);

    RawDataAggregationOperator rawDataAggregationOperator =
        initRawDataAggregationOperator(aggregationTypes, null, inputLocations, windowParameter);
    int count = 0;
    while (rawDataAggregationOperator.isBlocked().isDone()
        && rawDataAggregationOperator.hasNext()) {
      TsBlock resultTsBlock = rawDataAggregationOperator.next();
      if (resultTsBlock == null) {
        continue;
      }
      for (int row = 0; row < resultTsBlock.getPositionCount(); row++, count++) {
        for (int i = 0; i < 2; i++) {
          assertEquals(result[0][count], resultTsBlock.getColumn(i).getLong(row));
        }
        for (int i = 2; i < 4; i++) {
          assertEquals(result[1][count], resultTsBlock.getColumn(i).getLong(row));
        }
        for (int i = 4; i < 6; i++) {
          assertEquals(result[2][count], resultTsBlock.getColumn(i).getInt(row));
        }
        for (int i = 6; i < 8; i++) {
          assertEquals(result[3][count], resultTsBlock.getColumn(i).getInt(row));
        }
      }
    }
    assertEquals(1, count);
  }

  private RawDataAggregationOperator initRawDataAggregationOperator(
      List<TAggregationType> aggregationTypes,
      GroupByTimeParameter groupByTimeParameter,
      List<List<InputLocation[]>> inputLocations,
      WindowParameter windowParameter)
      throws IllegalPathException {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

    MeasurementPath measurementPath1 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
    Set<String> allSensors = new HashSet<>();
    allSensors.add("sensor0");
    allSensors.add("sensor1");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        3, new PlanNodeId("3"), RowBasedTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        4, new PlanNodeId("4"), RawDataAggregationOperatorTest.class.getSimpleName());
    driverContext
        .getOperatorContexts()
        .forEach(
            operatorContext -> {
              operatorContext.setMaxRunTime(TEST_TIME_SLICE);
            });

    Filter timeFilter = null;
    if (groupByTimeParameter != null && !groupByTimeParameter.isLeftCRightO()) {
      timeFilter = new Gt<>(0L, FilterType.TIME_FILTER);
    }

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(allSensors);
    scanOptionsBuilder.withGlobalTimeFilter(timeFilter);
    SeriesScanOperator seriesScanOperator1 =
        new SeriesScanOperator(
            driverContext.getOperatorContexts().get(0),
            planNodeId1,
            measurementPath1,
            Ordering.ASC,
            scanOptionsBuilder.build());
    seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

    MeasurementPath measurementPath2 =
        new MeasurementPath(AGGREGATION_OPERATOR_TEST_SG + ".device0.sensor1", TSDataType.INT32);
    SeriesScanOperator seriesScanOperator2 =
        new SeriesScanOperator(
            driverContext.getOperatorContexts().get(1),
            planNodeId2,
            measurementPath2,
            Ordering.ASC,
            scanOptionsBuilder.build());
    seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

    RowBasedTimeJoinOperator timeJoinOperator =
        new RowBasedTimeJoinOperator(
            driverContext.getOperatorContexts().get(2),
            Arrays.asList(seriesScanOperator1, seriesScanOperator2),
            Ordering.ASC,
            Arrays.asList(TSDataType.INT32, TSDataType.INT32),
            Arrays.asList(
                new SingleColumnMerger(new InputLocation(0, 0), new AscTimeComparator()),
                new SingleColumnMerger(new InputLocation(1, 0), new AscTimeComparator())),
            new AscTimeComparator());

    List<Aggregator> aggregators = new ArrayList<>();
    List<Accumulator> accumulators =
        AccumulatorFactory.createAccumulators(
            aggregationTypes,
            TSDataType.INT32,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    for (int i = 0; i < accumulators.size(); i++) {
      aggregators.add(
          new Aggregator(accumulators.get(i), AggregationStep.SINGLE, inputLocations.get(i)));
    }
    return new RawDataAggregationOperator(
        driverContext.getOperatorContexts().get(3),
        aggregators,
        initTimeRangeIterator(groupByTimeParameter, true, true),
        timeJoinOperator,
        true,
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        windowParameter);
  }
}
