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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
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
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
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
                new TimeRange(250001, 300000),
                new TimeRange(300001, 400000)));
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void otherTest() throws IOException {
    Map<TVList, Integer> tvListMap =
        buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1)));
    testAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        false,
        1);
    testAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        false,
        1);

    tvListMap = buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 10000)));
    AlignedTVList tvList = (AlignedTVList) tvListMap.keySet().iterator().next();
    tvList.delete(1, 10, 0);
    testAligned(
        tvListMap,
        Ordering.ASC,
        new TimeFilterOperators.TimeBetweenAnd(110L, 200L),
        new LongFilterOperators.ValueBetweenAnd(0, 100, 200),
        new PaginationController(10, 10),
        Collections.emptyList(),
        Arrays.asList(
            Collections.singletonList(new TimeRange(1, 10)),
            Collections.emptyList(),
            Collections.emptyList()),
        false,
        10);
    testAligned(
        tvListMap,
        Ordering.DESC,
        new TimeFilterOperators.TimeBetweenAnd(110L, 200L),
        new LongFilterOperators.ValueBetweenAnd(0, 100, 200),
        new PaginationController(10, 10),
        Collections.emptyList(),
        Arrays.asList(
            Collections.singletonList(new TimeRange(1, 10)),
            Collections.emptyList(),
            Collections.emptyList()),
        false,
        10);
    tvListMap = buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 10)));
    testAligned(
        tvListMap,
        Ordering.ASC,
        new TimeFilterOperators.TimeBetweenAnd(1L, 10L),
        new LongFilterOperators.ValueBetweenAnd(0, 1, 10),
        new PaginationController(10, 1),
        Collections.singletonList(new TimeRange(4, 4)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        true,
        8);
    testAligned(
        tvListMap,
        Ordering.DESC,
        new TimeFilterOperators.TimeBetweenAnd(1L, 10L),
        new LongFilterOperators.ValueBetweenAnd(0, 1, 10),
        new PaginationController(10, 1),
        Collections.singletonList(new TimeRange(4, 4)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        true,
        8);
  }

  @Test
  public void testAlignedWithDeletionsInTVList() throws IOException {
    Map<TVList, Integer> tvListMap =
        buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1000)));
    AlignedTVList tvList = (AlignedTVList) tvListMap.keySet().iterator().next();
    tvList.deleteTime(1, 1000);
    // deletion in time column
    testAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 1000)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        false,
        0);
    testAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 1000)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        false,
        0);

    // deletion in time column
    tvListMap = buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1000)));
    tvList = (AlignedTVList) tvListMap.keySet().iterator().next();
    tvList.deleteTime(1, 10);
    testAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 10)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        false,
        990);
    testAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 10)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        false,
        990);

    // deletion in one value column
    tvListMap = buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1000)));
    tvList = (AlignedTVList) tvListMap.keySet().iterator().next();
    tvList.delete(1, 10, 0);
    testAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(
            Collections.singletonList(new TimeRange(1, 10)),
            Collections.emptyList(),
            Collections.emptyList()),
        false,
        1000);
    testAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(
            Collections.singletonList(new TimeRange(1, 10)),
            Collections.emptyList(),
            Collections.emptyList()),
        false,
        1000);

    // deletion in all value column
    tvListMap = buildAlignedSingleTvListMap(Collections.singletonList(new TimeRange(1, 1000)));
    tvList = (AlignedTVList) tvListMap.keySet().iterator().next();
    tvList.delete(1, 10);
    testAligned(
        tvListMap,
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 10)),
        Arrays.asList(
            Collections.singletonList(new TimeRange(1, 10)),
            Collections.singletonList(new TimeRange(0, 10)),
            Collections.singletonList(new TimeRange(0, 10))),
        false,
        990);
    testAligned(
        tvListMap,
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(1, 10)),
        Arrays.asList(
            Collections.singletonList(new TimeRange(1, 10)),
            Collections.singletonList(new TimeRange(0, 10)),
            Collections.singletonList(new TimeRange(0, 10))),
        false,
        990);
  }

  @Test
  public void testAlignedAsc() throws IOException {
    testAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        400000);
    testAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Arrays.asList(new TimeRange(10001, 20000), new TimeRange(50001, 60000)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        380000);
    testAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(0, 1000000)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
    testAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(
            Collections.singletonList(new TimeRange(0, 1000000)),
            Collections.emptyList(),
            Collections.emptyList()),
        400000);
    testAligned(
        Ordering.ASC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(
            Arrays.asList(new TimeRange(10001, 20000), new TimeRange(100001, 120000)),
            Collections.singletonList(new TimeRange(50001, 60000)),
            Collections.emptyList()),
        400000);
  }

  @Test
  public void testAlignedAscWithGlobalTimeFilter() throws IOException {
    testAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        370000);
    testAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1001L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        371000);
    testAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 3000000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
  }

  @Test
  public void testAlignedAscWithPushDownFilter() throws IOException {
    testAligned(
        Ordering.ASC,
        null,
        new LongFilterOperators.ValueEq(0, 10000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        1);
    testAligned(
        Ordering.ASC,
        null,
        new LongFilterOperators.ValueEq(0, -1),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
    testAligned(
        Ordering.ASC,
        null,
        new LongFilterOperators.ValueBetweenAnd(0, 10001, 20000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        10000);
  }

  @Test
  public void testAlignedAscWithLimitAndOffset() throws IOException {
    testAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(10000, 0),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        10000);
    testAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(10000, 10),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        10000);
    testAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(10000, 1000000),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
    testAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(100000, 0),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        100000);
    testAligned(
        Ordering.ASC,
        null,
        null,
        new PaginationController(200000, 0),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        200000);
  }

  @Test
  public void testAlignedDesc() throws IOException {
    testAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        400000);
    testAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Arrays.asList(new TimeRange(10001, 20000), new TimeRange(50001, 60000)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        380000);
    testAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.singletonList(new TimeRange(0, 1000000)),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
    testAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(
            Collections.singletonList(new TimeRange(0, 1000000)),
            Collections.emptyList(),
            Collections.emptyList()),
        400000);
    testAligned(
        Ordering.DESC,
        null,
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(
            Arrays.asList(new TimeRange(10001, 20000), new TimeRange(100001, 120000)),
            Collections.singletonList(new TimeRange(50001, 60000)),
            Collections.emptyList()),
        400000);
  }

  @Test
  public void testAlignedDescWithGlobalTimeFilter() throws IOException {
    testAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        370000);
    testAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1001L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        371000);
    testAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 3000000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
  }

  @Test
  public void testAlignedDescWithPushDownFilter() throws IOException {
    testAligned(
        Ordering.DESC,
        null,
        new LongFilterOperators.ValueEq(0, 10000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        1);
    testAligned(
        Ordering.DESC,
        null,
        new LongFilterOperators.ValueEq(0, -1),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
    testAligned(
        Ordering.DESC,
        null,
        new LongFilterOperators.ValueBetweenAnd(0, 10001, 20000),
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        10000);
  }

  @Test
  public void testAlignedDescWithLimitAndOffset() throws IOException {
    testAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(10000, 0),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        10000);
    testAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(10000, 10),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        10000);
    testAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(10000, 1000000),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        0);
    testAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(100000, 0),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        100000);
    testAligned(
        Ordering.DESC,
        null,
        null,
        new PaginationController(200000, 0),
        Collections.emptyList(),
        Arrays.asList(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
        200000);
  }

  private void testAligned(
      Ordering scanOrder,
      Filter globalTimeFilter,
      Filter pushDownFilter,
      PaginationController paginationController,
      List<TimeRange> timeDeletions,
      List<List<TimeRange>> valueDeletions,
      int expectedCount)
      throws IOException {

    testAligned(
        largeSingleTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        timeDeletions,
        valueDeletions,
        true,
        expectedCount);
    testAligned(
        largeOrderedMultiTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        timeDeletions,
        valueDeletions,
        true,
        expectedCount);
    testAligned(
        largeMergeSortMultiTvListMap,
        scanOrder,
        globalTimeFilter,
        pushDownFilter,
        duplicatePaginationController(paginationController),
        timeDeletions,
        valueDeletions,
        true,
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
      boolean isDeletedAsModification,
      int expectedCount)
      throws IOException {
    long endTime = tvListMap.keySet().stream().mapToLong(TVList::getMaxTime).max().getAsLong();
    List<Integer> columnIdxList = Arrays.asList(0, 1, 2);
    IMeasurementSchema measurementSchema = getMeasurementSchema();
    AlignedReadOnlyMemChunk chunk =
        new AlignedReadOnlyMemChunk(
            fragmentInstanceContext,
            columnIdxList,
            measurementSchema,
            tvListMap,
            isDeletedAsModification ? timeDeletions : Collections.emptyList(),
            isDeletedAsModification
                ? valueDeletions
                : Arrays.asList(
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));

    chunk.sortTvLists();
    chunk.initChunkMetaFromTVListsWithFakeStatistics();

    MemPointIterator memPointIterator = chunk.createMemPointIterator(scanOrder, globalTimeFilter);
    memPointIterator.setLimitAndOffset(paginationController);
    memPointIterator.setPushDownFilter(pushDownFilter);
    List<Statistics<? extends Serializable>> pageStatisticsList = chunk.getTimeStatisticsList();
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
          Assert.assertTrue(
              currentTimeRange == null || currentTimeRange.contains(currentTimestamp));
          Long int64Value = tsBlock.getColumn(0).isNull(i) ? null : tsBlock.getColumn(0).getLong(i);
          Boolean boolValue =
              tsBlock.getColumn(1).isNull(i) ? null : tsBlock.getColumn(1).getBoolean(i);
          boolean isLast = tsBlock.getColumn(2).getBoolean(i);
          Assert.assertTrue(isLast);
          List<TimeRange> int64Deletions = valueDeletions.get(0);
          List<TimeRange> boolDeletions = valueDeletions.get(1);

          if (int64Value != null) {
            Assert.assertEquals(currentTimestamp, int64Value.longValue());
          } else {
            Assert.assertTrue(
                int64Deletions.stream().anyMatch(range -> range.contains(currentTimestamp)));
          }
          if (boolValue != null) {
            Assert.assertEquals(currentTimestamp % 2 == 0, boolValue.booleanValue());
          } else {
            Assert.assertTrue(
                boolDeletions.stream().anyMatch(range -> range.contains(currentTimestamp)));
          }
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

    chunk.initChunkMetaFromTVListsWithFakeStatistics();
    memPointIterator = chunk.createMemPointIterator(scanOrder, globalTimeFilter);
    count = 0;
    while (memPointIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = memPointIterator.nextTimeValuePair();
      long currentTimestamp = timeValuePair.getTimestamp();
      Long value = (Long) timeValuePair.getValues()[0];
      Boolean boolValue = (Boolean) timeValuePair.getValues()[1];
      boolean isLast = (boolean) timeValuePair.getValues()[2];
      Assert.assertTrue(isLast);

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
        expectedTimestamp = endTime - count - offset;
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
    String[] array = new String[] {"s1", "s2", "s3"};
    TSDataType[] types =
        new TSDataType[] {TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.BOOLEAN};
    TSEncoding[] encodings =
        new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN};
    return new VectorMeasurementSchema(
        VECTOR_PLACEHOLDER, array, types, encodings, CompressionType.UNCOMPRESSED);
  }

  private static Map<TVList, Integer> buildAlignedSingleTvListMap(List<TimeRange> timeRanges) {
    AlignedTVList alignedTVList =
        AlignedTVList.newAlignedList(
            Arrays.asList(TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.BOOLEAN));
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
            alignedTVList.putAlignedValue(
                timestamp, new Object[] {timestamp, timestamp % 2 == 0, false});
          }
        }
        alignedTVList.putAlignedValue(
            timestamp, new Object[] {timestamp, timestamp % 2 == 0, true});
      }
    }
    Map<TVList, Integer> tvListMap = new HashMap<>();
    tvListMap.put(alignedTVList, alignedTVList.rowCount());
    return tvListMap;
  }

  private static Map<TVList, Integer> buildAlignedMultiTvListMap(List<TimeRange> timeRanges) {
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    for (int i = 0; i < timeRanges.size(); i++) {
      TimeRange timeRange = timeRanges.get(i);
      AlignedTVList alignedTVList =
          AlignedTVList.newAlignedList(
              Arrays.asList(TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.BOOLEAN));
      long start = timeRange.getMin();
      long end = timeRange.getMax();
      List<Long> timestamps = new ArrayList<>((int) (end - start + 1));
      for (long timestamp = start; timestamp <= end; timestamp++) {
        timestamps.add(timestamp);
      }
      Collections.shuffle(timestamps);
      for (Long timestamp : timestamps) {
        boolean isLast = true;
        for (int j = i + 1; j < timeRanges.size(); j++) {
          if (timeRanges.get(j).contains(timestamp)) {
            isLast = false;
            break;
          }
        }
        alignedTVList.putAlignedValue(
            timestamp, new Object[] {timestamp, timestamp % 2 == 0, isLast});
      }
      tvListMap.put(alignedTVList, alignedTVList.rowCount());
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

  @Test
  public void testSkipTimeRange() throws QueryProcessException, IOException {
    List<Map<TVList, Integer>> list =
        Arrays.asList(
            buildAlignedSingleTvListMap(
                Arrays.asList(new TimeRange(10, 20), new TimeRange(22, 40))),
            buildAlignedMultiTvListMap(Arrays.asList(new TimeRange(10, 20), new TimeRange(22, 40))),
            buildAlignedMultiTvListMap(
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
    List<Integer> columnIdxList = Arrays.asList(0, 1, 2);
    IMeasurementSchema measurementSchema = getMeasurementSchema();
    AlignedReadOnlyMemChunk chunk =
        new AlignedReadOnlyMemChunk(
            fragmentInstanceContext,
            columnIdxList,
            measurementSchema,
            tvListMap,
            Collections.emptyList(),
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
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
        Assert.assertEquals(timeValuePair.getTimestamp(), timeValuePair.getValues()[0]);
        resultTimestamps.add(timeValuePair.getTimestamp());
      }
    }
    Assert.assertEquals(expectedTimestamps, resultTimestamps);
  }

  @Test
  public void testEncodeBatch() {
    testEncodeBatch(largeSingleTvListMap, 400000);
    testEncodeBatch(largeOrderedMultiTvListMap, 400000);
    testEncodeBatch(largeMergeSortMultiTvListMap, 400000);
  }

  private void testEncodeBatch(Map<TVList, Integer> tvListMap, long expectedCount) {
    AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(getMeasurementSchema());
    List<Integer> columnIdxList = Arrays.asList(0, 1, 2);
    IMeasurementSchema measurementSchema = getMeasurementSchema();
    AlignedReadOnlyMemChunk chunk =
        new AlignedReadOnlyMemChunk(
            fragmentInstanceContext,
            columnIdxList,
            measurementSchema,
            tvListMap,
            Collections.emptyList(),
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    chunk.sortTvLists();
    chunk.initChunkMetaFromTVListsWithFakeStatistics();
    MemPointIterator memPointIterator = chunk.createMemPointIterator(Ordering.ASC, null);
    BatchEncodeInfo encodeInfo =
        new BatchEncodeInfo(
            0, 0, 0, 10000, 100000, IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize());
    long[] times = new long[10000];
    long count = 0;
    while (memPointIterator.hasNextBatch()) {
      memPointIterator.encodeBatch(alignedChunkWriter, encodeInfo, times);
      if (encodeInfo.pointNumInPage >= encodeInfo.maxNumberOfPointsInPage) {
        alignedChunkWriter.write(times, encodeInfo.pointNumInPage, 0);
        encodeInfo.pointNumInPage = 0;
      }

      if (encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk) {
        alignedChunkWriter.sealCurrentPage();
        alignedChunkWriter.clearPageWriter();
        count += alignedChunkWriter.getTimeChunkWriter().getStatistics().getCount();
        alignedChunkWriter = new AlignedChunkWriterImpl(getMeasurementSchema());
        encodeInfo.reset();
      }
    }
    // Handle remaining data in the final unsealed chunk
    if (encodeInfo.pointNumInChunk > 0 || encodeInfo.pointNumInPage > 0) {
      if (encodeInfo.pointNumInPage > 0) {
        alignedChunkWriter.write(times, encodeInfo.pointNumInPage, 0);
      }
      alignedChunkWriter.sealCurrentPage();
      count += alignedChunkWriter.getTimeChunkWriter().getStatistics().getCount();
    }
    Assert.assertEquals(expectedCount, count);
  }
}
