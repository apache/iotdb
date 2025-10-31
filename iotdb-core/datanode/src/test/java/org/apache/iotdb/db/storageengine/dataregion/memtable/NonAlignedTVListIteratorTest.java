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
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.read.reader.series.PaginationController;
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

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class NonAlignedTVListIteratorTest {

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
        buildNonAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 400000)));
    largeOrderedMultiTvListMap =
        buildNonAlignedMultiTvListMap(
            Arrays.asList(
                new TimeRange(1, 100000),
                new TimeRange(100001, 200000),
                new TimeRange(200001, 300000),
                new TimeRange(300001, 400000)));
    largeMergeSortMultiTvListMap =
        buildNonAlignedMultiTvListMap(
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
  public void otherTest() throws QueryProcessException, IOException {
    Map<TVList, Integer> tvListMap =
        buildNonAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1)));
    testNonAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        false,
        1);
    testNonAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        false,
        1);

    tvListMap = buildNonAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 10000)));
    TVList tvList = tvListMap.keySet().iterator().next();
    tvList.delete(1, 10);
    testNonAligned(
        tvListMap,
        Ordering.ASC,
        new TimeFilterOperators.TimeBetweenAnd(110L, 200L),
        new LongFilterOperators.ValueBetweenAnd(0, 100, 200),
        new PaginationController(10, 10),
        Collections.singletonList(new TimeRange(1, 10)),
        false,
        10);
    testNonAligned(
        tvListMap,
        Ordering.DESC,
        new TimeFilterOperators.TimeBetweenAnd(110L, 200L),
        new LongFilterOperators.ValueBetweenAnd(0, 100, 200),
        new PaginationController(10, 10),
        Collections.singletonList(new TimeRange(1, 10)),
        false,
        10);
  }

  @Test
  public void testNonAlignedWithDeletionInTVList() throws QueryProcessException, IOException {
    Map<TVList, Integer> tvListMap =
        buildNonAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1000)));
    TVList tvList = tvListMap.keySet().iterator().next();
    tvList.delete(1, 1000);
    testNonAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 1000)),
        false,
        0);
    testNonAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 1000)),
        false,
        0);

    tvListMap = buildNonAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1000)));
    tvList = tvListMap.keySet().iterator().next();
    tvList.delete(1, 10);
    testNonAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 10)),
        false,
        990);
    testNonAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 10)),
        false,
        990);
  }

  @Test
  public void testNonAlignedAsc() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        400000);
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Arrays.asList(new TimeRange(10001, 20000), new TimeRange(50001, 60000)),
        380000);
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(0, 1000000)),
        0);
  }

  @Test
  public void testNonAlignedAscWithGlobalTimeFilter() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        370000);
    testNonAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1001L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        371000);
    testNonAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 3000000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        0);
  }

  @Test
  public void testNonAlignedAscWithPushDownFilter() throws QueryProcessException, IOException {
    testNonAligned(
        Ordering.ASC,
        null,
        new LongFilterOperators.ValueEq(0, 10000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        1);
    testNonAligned(
        Ordering.ASC,
        null,
        new LongFilterOperators.ValueEq(0, -1),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        0);
    testNonAligned(
        Ordering.ASC,
        null,
        new LongFilterOperators.ValueBetweenAnd(0, 10001, 20000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        10000);
  }

  @Test
  public void testNonAlignedAscWithLimitAndOffset() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(10000, 0),
        Collections.emptyList(),
        10000);
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(10000, 10),
        Collections.emptyList(),
        10000);
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(10000, 1000000),
        Collections.emptyList(),
        0);
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(100000, 0),
        Collections.emptyList(),
        100000);
    testNonAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(200000, 0),
        Collections.emptyList(),
        200000);
  }

  @Test
  public void testNonAlignedDesc() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        400000);
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Arrays.asList(new TimeRange(10001, 20000), new TimeRange(50001, 60000)),
        380000);
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(0, 1000000)),
        0);
  }

  @Test
  public void testNonAlignedDescWithGlobalTimeFilter() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        370000);
    testNonAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1001L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        371000);
    testNonAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 3000000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        0);
  }

  @Test
  public void testNonAlignedDescWithPushDownFilter() throws QueryProcessException, IOException {
    testNonAligned(
        Ordering.DESC,
        null,
        new LongFilterOperators.ValueEq(0, 10000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        1);
    testNonAligned(
        Ordering.DESC,
        null,
        new LongFilterOperators.ValueEq(0, -1),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        0);
    testNonAligned(
        Ordering.DESC,
        null,
        new LongFilterOperators.ValueBetweenAnd(0, 10001, 20000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        10000);
  }

  @Test
  public void testNonAlignedDescWithLimitAndOffset() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(10000, 0),
        Collections.emptyList(),
        10000);
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(10000, 10),
        Collections.emptyList(),
        10000);
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(10000, 1000000),
        Collections.emptyList(),
        0);
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(100000, 0),
        Collections.emptyList(),
        100000);
    testNonAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(200000, 0),
        Collections.emptyList(),
        200000);
  }

  private void testNonAligned(
      Ordering scanOrder,
      Filter globalTimeFilter,
      Filter pushDownFilter,
      PaginationController paginationController,
      List<TimeRange> deletions,
      int expectedCount)
      throws QueryProcessException, IOException {
    testNonAligned(
        largeSingleTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        deletions,
        true,
        expectedCount);
    testNonAligned(
        largeOrderedMultiTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        deletions,
        true,
        expectedCount);
    testNonAligned(
        largeMergeSortMultiTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        deletions,
        true,
        expectedCount);
  }

  private PaginationController duplicatePaginationController(
      PaginationController paginationController) {
    return paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER
        ? paginationController
        : new PaginationController(
            paginationController.getCurLimit(), paginationController.getCurOffset());
  }

  private void testNonAligned(
      Map<TVList, Integer> tvListMap,
      Ordering scanOrder,
      Filter globalTimeFilter,
      Filter pushDownFilter,
      PaginationController paginationController,
      List<TimeRange> deletions,
      boolean isDeletedAsModification,
      int expectedCount)
      throws IOException, QueryProcessException {
    long endTime = tvListMap.keySet().stream().mapToLong(TVList::getMaxTime).max().getAsLong();
    ReadOnlyMemChunk chunk =
        new ReadOnlyMemChunk(
            fragmentInstanceContext,
            "s1",
            TSDataType.INT64,
            TSEncoding.PLAIN,
            tvListMap,
            null,
            isDeletedAsModification ? deletions : Collections.emptyList());
    chunk.sortTvLists();
    chunk.initChunkMetaFromTVListsWithFakeStatistics();

    MemPointIterator memPointIterator = chunk.createMemPointIterator(scanOrder, globalTimeFilter);
    memPointIterator.setLimitAndOffset(paginationController);
    memPointIterator.setPushDownFilter(pushDownFilter);
    List<Statistics<? extends Serializable>> pageStatisticsList = chunk.getPageStatisticsList();
    int count = 0;
    long offset = paginationController.getCurOffset();
    if (!scanOrder.isAscending()) {
      Collections.reverse(pageStatisticsList);
    }
    for (Statistics<? extends Serializable> statistics : pageStatisticsList) {
      TimeRange currentTimeRange =
          (statistics.getStartTime() <= statistics.getEndTime())
              ? new TimeRange(statistics.getStartTime(), statistics.getEndTime())
              : null;
      memPointIterator.setCurrentPageTimeRange(currentTimeRange);
      while (memPointIterator.hasNextBatch()) {
        TsBlock tsBlock = memPointIterator.nextBatch();
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long expectedTimestamp;
          if (scanOrder.isAscending()) {
            count++;
            expectedTimestamp = count + offset;
          } else {
            expectedTimestamp = endTime - count - offset;
            count++;
          }
          long currentTimestamp = tsBlock.getTimeByIndex(i);
          Assert.assertTrue(currentTimeRange.contains(currentTimestamp));
          long value = tsBlock.getColumn(0).getLong(i);
          Assert.assertEquals(currentTimestamp, value);
          if (globalTimeFilter != null) {
            Assert.assertTrue(globalTimeFilter.satisfyRow(currentTimestamp, null));
          }
          if (pushDownFilter != null) {
            Assert.assertTrue(pushDownFilter.satisfyLong(currentTimestamp, value));
          }
          if (!deletions.isEmpty()) {
            deletions.stream()
                .map(deletion -> deletion.contains(currentTimestamp))
                .forEach(Assert::assertFalse);
          }
          if (pushDownFilter == null
              && globalTimeFilter == null
              && paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER
              && deletions.isEmpty()) {
            Assert.assertEquals(expectedTimestamp, currentTimestamp);
          }
        }
      }
    }
    Assert.assertEquals(expectedCount, count);

    if (pushDownFilter != null) {
      return;
    }

    chunk.initChunkMetaFromTVListsWithFakeStatistics();
    memPointIterator = chunk.createMemPointIterator(scanOrder, globalTimeFilter);
    count = 0;
    while (memPointIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = memPointIterator.nextTimeValuePair();
      long currentTimestamp = timeValuePair.getTimestamp();
      if (currentTimestamp == 60001) {
        System.out.println();
      }
      long value = timeValuePair.getValue().getLong();
      Assert.assertEquals(currentTimestamp, value);
      long expectedTimestamp;
      if (scanOrder.isAscending()) {
        count++;
        expectedTimestamp = count + offset;
      } else {
        expectedTimestamp = endTime - count - offset;
        count++;
      }

      if (globalTimeFilter != null) {
        Assert.assertTrue(globalTimeFilter.satisfyRow(currentTimestamp, null));
      }
      if (!deletions.isEmpty()) {
        try {
          deletions.stream()
              .map(deletion -> deletion.contains(currentTimestamp))
              .forEach(Assert::assertFalse);
        } catch (AssertionError e) {
          System.out.println();
        }
      }
      if (globalTimeFilter == null
          && paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER
          && deletions.isEmpty()) {
        Assert.assertEquals(expectedTimestamp, currentTimestamp);
      }
    }
  }

  private static Map<TVList, Integer> buildNonAlignedSingleTvListMap(List<TimeRange> timeRanges) {
    TVList tvList = TVList.newList(TSDataType.INT64);
    for (TimeRange timeRange : timeRanges) {
      long start = timeRange.getMin();
      long end = timeRange.getMax();
      List<Long> timestamps = new ArrayList<>((int) (end - start + 1));
      for (long timestamp = start; timestamp <= end; timestamp++) {
        timestamps.add(timestamp);
      }
      Collections.shuffle(timestamps);
      for (Long timestamp : timestamps) {
        if (timestamp % 5000 == 1) {
          // add some duplicated timestamp
          for (int i = 0; i < 4; i++) {
            tvList.putLong(timestamp, timestamp - i);
          }
        }
        tvList.putLong(timestamp, timestamp);
      }
    }
    Map<TVList, Integer> tvListMap = new HashMap<>();
    tvListMap.put(tvList, tvList.rowCount());
    return tvListMap;
  }

  private static Map<TVList, Integer> buildNonAlignedMultiTvListMap(List<TimeRange> timeRanges) {
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    for (TimeRange timeRange : timeRanges) {
      TVList tvList = TVList.newList(TSDataType.INT64);
      long start = timeRange.getMin();
      long end = timeRange.getMax();
      List<Long> timestamps = new ArrayList<>((int) (end - start + 1));
      for (long timestamp = start; timestamp <= end; timestamp++) {
        timestamps.add(timestamp);
      }
      Collections.shuffle(timestamps);
      for (Long timestamp : timestamps) {
        tvList.putLong(timestamp, timestamp);
      }
      tvListMap.put(tvList, tvList.rowCount());
    }
    return tvListMap;
  }

  @Test
  public void testSkipTimeRange() throws QueryProcessException, IOException {
    List<Map<TVList, Integer>> list =
        Arrays.asList(
            buildNonAlignedSingleTvListMap(
                Arrays.asList(new TimeRange(10, 20), new TimeRange(22, 40))),
            buildNonAlignedMultiTvListMap(
                Arrays.asList(new TimeRange(10, 20), new TimeRange(22, 40))),
            buildNonAlignedMultiTvListMap(
                Arrays.asList(
                    new TimeRange(10, 20),
                    new TimeRange(10, 20),
                    new TimeRange(24, 30),
                    new TimeRange(22, 40))));
    for (Map<TVList, Integer> tvListMap : list) {
      testSkipTimeRange(
          tvListMap,
          Ordering.ASC,
          Arrays.asList(new TimeRange(11, 13), new TimeRange(21, 21), new TimeRange(33, 34)),
          Arrays.asList(new TimeRange(11, 13), new TimeRange(33, 34)));
      testSkipTimeRange(
          tvListMap,
          Ordering.DESC,
          Arrays.asList(new TimeRange(33, 34), new TimeRange(21, 21), new TimeRange(11, 13)),
          Arrays.asList(new TimeRange(33, 34), new TimeRange(11, 13)));
    }
  }

  private void testSkipTimeRange(
      Map<TVList, Integer> tvListMap,
      Ordering scanOrder,
      List<TimeRange> statisticsTimeRanges,
      List<TimeRange> expectedResultTimeRange)
      throws QueryProcessException, IOException {
    ReadOnlyMemChunk chunk =
        new ReadOnlyMemChunk(
            fragmentInstanceContext,
            "s1",
            TSDataType.INT64,
            TSEncoding.PLAIN,
            tvListMap,
            null,
            Collections.emptyList());
    chunk.sortTvLists();
    chunk.initChunkMetaFromTVListsWithFakeStatistics();
    MemPointIterator memPointIterator = chunk.createMemPointIterator(scanOrder, null);
    List<Long> expectedTimestamps = new ArrayList<>();
    for (TimeRange timeRange : expectedResultTimeRange) {
      if (scanOrder == Ordering.ASC) {
        for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
          expectedTimestamps.add(i);
        }
      } else {
        for (long i = timeRange.getMax(); i >= timeRange.getMin(); i--) {
          expectedTimestamps.add(i);
        }
      }
    }
    List<Long> resultTimestamps = new ArrayList<>(expectedTimestamps.size());
    for (TimeRange timeRange : statisticsTimeRanges) {
      memPointIterator.setCurrentPageTimeRange(timeRange);
      while (memPointIterator.hasNextBatch()) {
        TsBlock tsBlock = memPointIterator.nextBatch();
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long currentTimestamp = tsBlock.getTimeByIndex(i);
          long value = tsBlock.getColumn(0).getLong(i);
          Assert.assertEquals(currentTimestamp, value);
          resultTimestamps.add(currentTimestamp);
        }
      }
    }
    Assert.assertEquals(expectedTimestamps, resultTimestamps);

    memPointIterator = chunk.createMemPointIterator(scanOrder, null);

    resultTimestamps.clear();
    for (TimeRange timeRange : statisticsTimeRanges) {
      memPointIterator.setCurrentPageTimeRange(timeRange);
      while (memPointIterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = memPointIterator.nextTimeValuePair();
        Assert.assertEquals(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        resultTimestamps.add(timeValuePair.getTimestamp());
      }
    }
    Assert.assertEquals(expectedTimestamps, resultTimestamps);
  }
}
