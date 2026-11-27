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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iotdb.db.service.metrics.WritingMetrics.AVG_SERIES_POINT_NUM;
import static org.junit.Assert.assertEquals;

public class WritingMetricsTest {

  private static final DataRegionId FIRST_REGION = new DataRegionId(1);
  private static final DataRegionId SECOND_REGION = new DataRegionId(2);
  private static final MetricService METRIC_SERVICE = MetricService.getInstance();
  private static final WritingMetrics WRITING_METRICS = WritingMetrics.getInstance();

  @BeforeClass
  public static void setUp() {
    MetricConfigDescriptor.getInstance().getMetricConfig().setMetricLevel(MetricLevel.IMPORTANT);
    METRIC_SERVICE.startService();
    WRITING_METRICS.createFlushingMemTableStatusMetrics(FIRST_REGION);
    WRITING_METRICS.createFlushingMemTableStatusMetrics(SECOND_REGION);
  }

  @AfterClass
  public static void tearDown() {
    WRITING_METRICS.removeFlushingMemTableStatusMetrics(FIRST_REGION);
    WRITING_METRICS.removeFlushingMemTableStatusMetrics(SECOND_REGION);
    METRIC_SERVICE.stopService();
  }

  /**
   * Verifies that flushing two DataRegions records each average series point sample in the
   * histogram carrying that Region's tag, instead of routing every sample to the last-created
   * histogram.
   */
  @Test
  public void testRecordAverageSeriesPointNumByDataRegion() {
    WRITING_METRICS.recordFlushingMemTableStatus("root.db-1", 100, 2, 20, 10);
    WRITING_METRICS.recordFlushingMemTableStatus("root.db-2", 200, 4, 80, 20);

    Histogram firstRegionHistogram = getAverageSeriesPointHistogram(FIRST_REGION);
    Histogram secondRegionHistogram = getAverageSeriesPointHistogram(SECOND_REGION);

    assertEquals(1, firstRegionHistogram.getCount());
    assertEquals(10, firstRegionHistogram.takeSnapshot().getSum(), 0.001);
    assertEquals(1, secondRegionHistogram.getCount());
    assertEquals(20, secondRegionHistogram.takeSnapshot().getSum(), 0.001);
  }

  /**
   * The decaying mean used as the pre-allocation hint is kept per DataRegion, so a dense region
   * does not inflate the containers of a sparse one; the global mean stays available for
   * observability.
   */
  @Test
  public void testRecentAverageSeriesPointNumIsScopedToTheDataRegion() {
    DataRegionId sparseRegion = new DataRegionId(31);
    DataRegionId denseRegion = new DataRegionId(32);
    WRITING_METRICS.createFlushingMemTableStatusMetrics(sparseRegion);
    WRITING_METRICS.createFlushingMemTableStatusMetrics(denseRegion);
    boolean originalSwitch =
        IoTDBDescriptor.getInstance().getConfig().isEnableTVListBlockPreallocation();
    int originalMax = IoTDBDescriptor.getInstance().getConfig().getTvListMaxPreallocatedBlocks();
    try {
      for (int sample = 0; sample < 5; sample++) {
        WRITING_METRICS.recordFlushingMemTableStatus(sparseRegion, 100, 2, 20, 10);
        WRITING_METRICS.recordFlushingMemTableStatus(denseRegion, 200, 4, 4_000, 1_000);
      }

      assertEquals(10, WRITING_METRICS.getRecentAvgSeriesPointNum(sparseRegion.getId()), 0.001);
      assertEquals(1_000, WRITING_METRICS.getRecentAvgSeriesPointNum(denseRegion.getId()), 0.001);
      // A region without a sample falls back to the node-wide mean.
      assertEquals(
          WRITING_METRICS.getRecentAvgSeriesPointNum(),
          WRITING_METRICS.getRecentAvgSeriesPointNum(999),
          0.001);

      IoTDBDescriptor.getInstance().getConfig().setEnableTVListBlockPreallocation(true);
      // 10 points per series is less than one primitive array, 1000 points is about 16 arrays.
      assertEquals(1, TVList.getDefaultArrayNum(String.valueOf(sparseRegion.getId())));
      assertEquals(16, TVList.getDefaultArrayNum(String.valueOf(denseRegion.getId())));

      // Removing the region drops its decaying mean and falls back to the node-wide one.
      WRITING_METRICS.removeFlushingMemTableStatusMetrics(denseRegion);
      assertEquals(
          WRITING_METRICS.getRecentAvgSeriesPointNum(),
          WRITING_METRICS.getRecentAvgSeriesPointNum(denseRegion.getId()),
          0.001);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTVListBlockPreallocation(originalSwitch);
      IoTDBDescriptor.getInstance().getConfig().setTvListMaxPreallocatedBlocks(originalMax);
      WRITING_METRICS.removeFlushingMemTableStatusMetrics(sparseRegion);
      WRITING_METRICS.removeFlushingMemTableStatusMetrics(denseRegion);
    }
  }

  private Histogram getAverageSeriesPointHistogram(DataRegionId dataRegionId) {
    return METRIC_SERVICE.getOrCreateHistogram(
        Metric.FLUSHING_MEM_TABLE_STATUS.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        AVG_SERIES_POINT_NUM,
        Tag.REGION.toString(),
        dataRegionId.toString());
  }
}
