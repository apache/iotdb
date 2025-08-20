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
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class MemPointIteratorTest {
  private static final int maxNumberOfPointsInPage = 1000;

  private static FragmentInstanceId instanceId;
  private static ExecutorService instanceNotificationExecutor;
  private static FragmentInstanceStateMachine stateMachine;
  private static FragmentInstanceContext fragmentInstanceContext;

  @BeforeClass
  public static void setup() {
    instanceId = new FragmentInstanceId(new PlanFragmentId("1", 0), "stub-instance");
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    stateMachine = new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    fragmentInstanceContext = createFragmentInstanceContext(instanceId, stateMachine);
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void testNonAlignedAsc() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.ASC, null, null, PaginationController.UNLIMITED_PAGINATION_CONTROLLER, 400000);
  }

  @Test
  public void testNonAlignedAscWithGlobalTimeFilter() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        370000);
    testNonAligned(
        Ordering.ASC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 3000000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        0);
  }

  @Test
  public void testNonAlignedAscWithLimitAndOffset() throws IOException, QueryProcessException {
    testNonAligned(Ordering.ASC, null, null, new PaginationController(10000, 0), 10000);
    testNonAligned(Ordering.ASC, null, null, new PaginationController(100000, 0), 100000);
    testNonAligned(Ordering.ASC, null, null, new PaginationController(200000, 0), 200000);
  }

  @Test
  public void testNonAlignedDesc() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.DESC, null, null, PaginationController.UNLIMITED_PAGINATION_CONTROLLER, 400000);
  }

  @Test
  public void testNonAlignedDescWithGlobalTimeFilter() throws IOException, QueryProcessException {
    testNonAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 30000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        370000);
    testNonAligned(
        Ordering.DESC,
        new TimeFilterOperators.TimeNotBetweenAnd(1L, 3000000L),
        null,
        PaginationController.UNLIMITED_PAGINATION_CONTROLLER,
        0);
  }

  @Test
  public void testNonAlignedDescWithLimitAndOffset() throws IOException, QueryProcessException {
    testNonAligned(Ordering.ASC, null, null, new PaginationController(10000, 0), 10000);
    testNonAligned(Ordering.ASC, null, null, new PaginationController(100000, 0), 100000);
    testNonAligned(Ordering.ASC, null, null, new PaginationController(200000, 0), 200000);
  }

  private void testNonAligned(
      Ordering scanOrder,
      Filter globalTimeFilter,
      Filter pushDownFilter,
      PaginationController paginationController,
      int expectedCount)
      throws IOException, QueryProcessException {
    Map<TVList, Integer> tvListMap =
        buildTvListMap(
            Arrays.asList(
                new TimeRange(1, 100000),
                new TimeRange(100001, 200000),
                new TimeRange(200001, 300000),
                new TimeRange(300001, 400000)));
    List<TimeRange> deletions = Collections.emptyList();
    ReadOnlyMemChunk chunk =
        new ReadOnlyMemChunk(
            fragmentInstanceContext,
            "s1",
            TSDataType.INT64,
            TSEncoding.PLAIN,
            tvListMap,
            null,
            deletions);
    chunk.sortTvLists();
    chunk.initChunkMetaFromTVListsWithFakeStatistics(scanOrder, globalTimeFilter);

    MemPointIterator memPointIterator = chunk.getMemPointIterator();
    memPointIterator.setLimitAndOffset(paginationController);
    memPointIterator.setPushDownFilter(pushDownFilter);
    List<Statistics<? extends Serializable>> pageStatisticsList = chunk.getPageStatisticsList();
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
          long value = tsBlock.getColumn(0).getLong(i);
          Assert.assertEquals(currentTimestamp, value);
          if (globalTimeFilter != null) {
            Assert.assertTrue(globalTimeFilter.satisfyRow(currentTimestamp, null));
          }
          if (pushDownFilter != null) {
            Assert.assertTrue(pushDownFilter.satisfyLong(currentTimestamp, value));
          }
          if (pushDownFilter == null
              && globalTimeFilter == null
              && paginationController == PaginationController.UNLIMITED_PAGINATION_CONTROLLER) {
            Assert.assertEquals(expectedTimestamp, currentTimestamp);
          }
        }
      }
    }
    Assert.assertEquals(expectedCount, count);
  }

  private Map<TVList, Integer> buildTvListMap(List<TimeRange> timeRanges) {
    TVList tvList = TVList.newList(TSDataType.INT64);
    int rowCount = 0;
    for (TimeRange timeRange : timeRanges) {
      long start = timeRange.getMin();
      long end = timeRange.getMax();
      for (long timestamp = start; timestamp <= end; timestamp++) {
        tvList.putLong(timestamp, timestamp);
        rowCount++;
      }
    }
    Map<TVList, Integer> tvListMap = new HashMap<>();
    tvListMap.put(tvList, rowCount);
    return tvListMap;
  }
}
