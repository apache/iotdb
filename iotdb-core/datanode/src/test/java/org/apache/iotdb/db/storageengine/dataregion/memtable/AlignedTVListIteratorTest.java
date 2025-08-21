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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.commons.path.AlignedPath.VECTOR_PLACEHOLDER;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class AlignedTVListIteratorTest {
  private static FragmentInstanceId instanceId;
  private static ExecutorService instanceNotificationExecutor;
  private static FragmentInstanceStateMachine stateMachine;
  private static FragmentInstanceContext fragmentInstanceContext;

  private static Map<TVList, Integer> largeSingleTvListMap;
  private static Map<TVList, Integer> largeOrderedMultiTvListMap;
  private static Map<TVList, Integer> largeMergeSortMultiTvListMap;

  @BeforeClass
  public static void setup() {
    instanceId = new FragmentInstanceId(new PlanFragmentId("1", 0), "stub-instance");
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    stateMachine = new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    fragmentInstanceContext = createFragmentInstanceContext(instanceId, stateMachine);

    largeSingleTvListMap =
        buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 400000)));
    largeOrderedMultiTvListMap =
        buildAlignedMultiTvListMap(
            Arrays.asList(
                new TimeRange(1, 100000),
                new TimeRange(100001, 200000),
                new TimeRange(200001, 300000),
                new TimeRange(300001, 400000)));
    largeMergeSortMultiTvListMap =
        buildAlignedMultiTvListMap(
            Arrays.asList(
                new TimeRange(1, 20000),
                new TimeRange(1, 100000),
                new TimeRange(100001, 200000),
                new TimeRange(200001, 300000),
                new TimeRange(250001, 300000),
                new TimeRange(300001, 400000)));
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void testAlignedAsc() throws QueryProcessException, IOException {
    testAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList()),
        400000);
  }

  private void testAligned(
      Ordering scanOrder,
      Filter globalTimeFilter,
      Filter pushDownFilter,
      PaginationController paginationController,
      List<TimeRange> timeDeletions,
      List<List<TimeRange>> valueDeletions,
      int expectedCount)
      throws QueryProcessException, IOException {

    testAligned(
        largeSingleTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        timeDeletions,
        valueDeletions,
        expectedCount);
    testAligned(
        largeOrderedMultiTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        timeDeletions,
        valueDeletions,
        expectedCount);
    testAligned(
        largeMergeSortMultiTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        timeDeletions,
        valueDeletions,
        expectedCount);
  }

  private void testAligned(
      Map<TVList, Integer> tvListMap,
      Ordering scanOrder,
      Filter globalTimeFilter,
      Filter pushDownFilter,
      PaginationController paginationController,
      List<TimeRange> timeDeletions,
      List<List<TimeRange>> valueDeletions,
      int expectedCount)
      throws IOException, QueryProcessException {
    List<Integer> columnIdxList = Arrays.asList(0, 1);
    IMeasurementSchema measurementSchema = getMeasurementSchema();
    AlignedReadOnlyMemChunk chunk =
        new AlignedReadOnlyMemChunk(
            fragmentInstanceContext,
            columnIdxList,
            measurementSchema,
            tvListMap,
            timeDeletions,
            valueDeletions);

    chunk.sortTvLists();
    chunk.initChunkMetaFromTVListsWithFakeStatistics(scanOrder, globalTimeFilter);

    MemPointIterator memPointIterator = chunk.getMemPointIterator();
    memPointIterator.setLimitAndOffset(paginationController);
    memPointIterator.setPushDownFilter(pushDownFilter);
    List<Statistics<? extends Serializable>> pageStatisticsList = chunk.getTimeStatisticsList();
    int count = 0;
    long offset = paginationController.getCurOffset();
    for (Statistics<? extends Serializable> statistics : pageStatisticsList) {
      memPointIterator.setCurrentPageTimeRange(
          new TimeRange(statistics.getStartTime(), statistics.getEndTime()));
      while (memPointIterator.hasNextBatch()) {
        TsBlock tsBlock = memPointIterator.nextBatch();
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTimestamp;
          if (scanOrder.isAscending()) {
            count++;
            expectedTimestamp = count + offset;
          } else {
            expectedTimestamp = 400000 - count - offset;
            count++;
          }
          long currentTimestamp = tsBlock.getTimeByIndex(i);
          long int64Value = tsBlock.getColumn(0).getLong(i);
          boolean boolValue = tsBlock.getColumn(1).getBoolean(i);

          Assert.assertEquals(currentTimestamp, int64Value);
          Assert.assertEquals(expectedTimestamp % 2 == 0, boolValue);
          if (globalTimeFilter != null) {
            Assert.assertTrue(globalTimeFilter.satisfyRow(currentTimestamp, null));
          }
          if (pushDownFilter != null) {
            Assert.assertTrue(
                pushDownFilter.satisfyRow(currentTimestamp, new Object[] {int64Value, boolValue}));
          }
          if (!timeDeletions.isEmpty()) {
            timeDeletions.stream()
                .map(deletion -> deletion.contains(currentTimestamp))
                .forEach(Assert::assertFalse);
          }
          if (pushDownFilter == null
              && globalTimeFilter == null
              && paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER
              && timeDeletions.isEmpty()) {
            Assert.assertEquals(expectedTimestamp, currentTimestamp);
          }
        }
      }
    }
    Assert.assertEquals(expectedCount, count);

    if (pushDownFilter != null) {
      return;
    }

    chunk.initChunkMetaFromTVListsWithFakeStatistics(scanOrder, globalTimeFilter);
    memPointIterator = chunk.getMemPointIterator();
    count = 0;
    while (memPointIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = memPointIterator.nextTimeValuePair();
      long currentTimestamp = timeValuePair.getTimestamp();
      Long value = (Long) timeValuePair.getValues()[0];
      Boolean boolValue = (Boolean) timeValuePair.getValues()[1];

      if (value != null) {
        Assert.assertEquals(currentTimestamp, value.longValue());
      }
      if (boolValue != null) {
        Assert.assertEquals(currentTimestamp % 2 == 0, boolValue.booleanValue());
      }
      long expectedTimestamp;
      if (scanOrder.isAscending()) {
        count++;
        expectedTimestamp = count + offset;
      } else {
        expectedTimestamp = 400000 - count - offset;
        count++;
      }

      if (globalTimeFilter != null) {
        Assert.assertTrue(globalTimeFilter.satisfyRow(currentTimestamp, null));
      }
      if (!timeDeletions.isEmpty()) {
        timeDeletions.stream()
            .map(deletion -> deletion.contains(currentTimestamp))
            .forEach(Assert::assertFalse);
      }
      if (globalTimeFilter == null
          && paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER
          && timeDeletions.isEmpty()) {
        Assert.assertEquals(expectedTimestamp, currentTimestamp);
      }
    }
  }

  public VectorMeasurementSchema getMeasurementSchema() {
    String[] array = new String[] {"s1", "s2"};
    TSDataType[] types = new TSDataType[] {TSDataType.INT64, TSDataType.BOOLEAN};
    TSEncoding[] encodings = new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.PLAIN};
    return new VectorMeasurementSchema(
        VECTOR_PLACEHOLDER, array, types, encodings, CompressionType.UNCOMPRESSED);
  }

  private static Map<TVList, Integer> buildAlignedSingleTvListMap(List<TimeRange> timeRanges) {
    AlignedTVList alignedTVList =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.BOOLEAN));
    int rowCount = 0;
    for (TimeRange timeRange : timeRanges) {
      long start = timeRange.getMin();
      long end = timeRange.getMax();
      List<Long> timestamps = new ArrayList<>((int) (end - start + 1));
      for (long timestamp = start; timestamp <= end; timestamp++) {
        timestamps.add(timestamp);
      }
      Collections.shuffle(timestamps);
      for (Long timestamp : timestamps) {
        alignedTVList.putAlignedValue(timestamp, new Object[] {timestamp, timestamp % 2 == 0});
        rowCount++;
      }
    }
    Map<TVList, Integer> tvListMap = new HashMap<>();
    tvListMap.put(alignedTVList, rowCount);
    return tvListMap;
  }

  private static Map<TVList, Integer> buildAlignedMultiTvListMap(List<TimeRange> timeRanges) {
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    for (TimeRange timeRange : timeRanges) {
      AlignedTVList alignedTVList =
          AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT64, TSDataType.BOOLEAN));
      int rowCount = 0;
      long start = timeRange.getMin();
      long end = timeRange.getMax();
      List<Long> timestamps = new ArrayList<>((int) (end - start + 1));
      for (long timestamp = start; timestamp <= end; timestamp++) {
        timestamps.add(timestamp);
      }
      Collections.shuffle(timestamps);
      for (Long timestamp : timestamps) {
        alignedTVList.putAlignedValue(timestamp, new Object[] {timestamp, timestamp % 2 == 0});
        rowCount++;
      }
      tvListMap.put(alignedTVList, rowCount);
    }
    return tvListMap;
  }

  private PaginationController duplicatePaginationController(
      PaginationController paginationController) {
    return paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER
        ? paginationController
        : new PaginationController(
            paginationController.getCurLimit(), paginationController.getCurOffset());
  }
}
