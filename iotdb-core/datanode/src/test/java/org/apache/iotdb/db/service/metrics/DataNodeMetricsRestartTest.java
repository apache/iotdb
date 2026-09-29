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
import org.apache.iotdb.db.protocol.thrift.handler.RPCServiceThriftHandlerMetrics;
import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileMemMetricSet;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** DataNode metrics must survive a metric service restart, which drops and rebinds all metrics. */
public class DataNodeMetricsRestartTest {

  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final List<IMetricSet> boundMetricSets = new ArrayList<>();
  private MetricLevel originalLevel;
  private String originalReporters;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service.startService();
  }

  @After
  public void tearDown() {
    boundMetricSets.forEach(service::removeMetricSet);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testWritingMetricsRecordedValues() {
    final WritingMetrics metrics = WritingMetrics.getInstance();
    bind(metrics);
    metrics.recordFlushThreshold(100);
    metrics.recordRejectThreshold(200);
    metrics.recordWALQueueMaxMemorySize(300);
    metrics.recordActiveMemTableCount("1", 3);
    metrics.recordActiveTimePartitionCount(2);
    // A region that does not exist any more
    metrics.recordActiveMemTableCount("2", 1);
    metrics.recordActiveMemTableCount("2", -1);
    try {
      service.restartService();
      assertEquals(
          100,
          ((Gauge)
                  get(
                      Metric.MEMTABLE_THRESHOLD,
                      Tag.TYPE.toString(),
                      WritingMetrics.FLUSH_THRESHOLD))
              .getValue());
      assertEquals(
          200,
          ((Gauge)
                  get(
                      Metric.MEMTABLE_THRESHOLD,
                      Tag.TYPE.toString(),
                      WritingMetrics.REJECT_THRESHOLD))
              .getValue());
      assertEquals(
          300,
          ((Gauge)
                  get(
                      Metric.WAL_QUEUE_MEM_COST,
                      Tag.NAME.toString(),
                      WritingMetrics.WAL_QUEUE_MAX_MEM_COST))
              .getValue());
      assertEquals(
          3,
          ((Counter)
                  get(
                      Metric.ACTIVE_MEMTABLE_COUNT,
                      Tag.REGION.toString(),
                      new DataRegionId(1).toString()))
              .getCount());
      assertEquals(2, ((Counter) get(Metric.ACTIVE_TIME_PARTITION_COUNT)).getCount());
      assertEquals(
          0,
          count(
              Metric.ACTIVE_MEMTABLE_COUNT, Tag.REGION.toString(), new DataRegionId(2).toString()));
    } finally {
      metrics.recordActiveMemTableCount("1", -3);
      metrics.recordActiveTimePartitionCount(-2);
    }
  }

  @Test
  public void testActiveMemTableCountIsRecordedToTheCounterOfTheRegion() {
    final WritingMetrics metrics = WritingMetrics.getInstance();
    final DataRegionId dataRegionId = new DataRegionId(3);
    metrics.createActiveMemtableCounterMetrics(dataRegionId);
    metrics.recordActiveMemTableCount(String.valueOf(dataRegionId.getId()), 2);
    try {
      // The counter created with the region is the one recorded to and removed with the region
      assertEquals(1, countActiveMemTableCounters(dataRegionId));
      assertEquals(
          2,
          ((Counter)
                  get(Metric.ACTIVE_MEMTABLE_COUNT, Tag.REGION.toString(), dataRegionId.toString()))
              .getCount());
    } finally {
      metrics.recordActiveMemTableCount(String.valueOf(dataRegionId.getId()), -2);
      metrics.removeActiveMemtableCounterMetrics(dataRegionId);
    }
    assertEquals(0, countActiveMemTableCounters(dataRegionId));
  }

  @Test
  public void testLoadTsFileOtherMemory() {
    final LoadTsFileMemMetricSet metrics = LoadTsFileMemMetricSet.getInstance();
    bind(metrics);
    metrics.updateOtherMemory(100);
    try {
      service.restartService();
      assertEquals(100, getLoadTsFileOtherMemory());
      metrics.updateOtherMemory(-40);
      assertEquals(60, getLoadTsFileOtherMemory());
    } finally {
      metrics.updateOtherMemory(-60);
    }
  }

  @Test
  public void testActiveLoadingFilesCounters() {
    final ActiveLoadingFilesNumberMetricsSet metrics =
        ActiveLoadingFilesNumberMetricsSet.getInstance();
    bind(metrics);
    metrics.updatePendingDirList(Collections.singleton("pendingDir"));
    metrics.updateFailedDir("failedDir");
    metrics.increaseQueuingFileCounter(2);
    try {
      service.restartService();
      metrics.updatePendingFileCounterInDir("pendingDir", 3);
      metrics.updateTotalFailedFileCounter(4);
      assertEquals(
          3,
          ((Counter)
                  get(
                      Metric.ACTIVE_LOADING_FILES_NUMBER,
                      Tag.TYPE.toString(),
                      "pending - pendingDir"))
              .getCount());
      assertEquals(
          4,
          ((Counter)
                  get(
                      Metric.ACTIVE_LOADING_FILES_NUMBER,
                      Tag.TYPE.toString(),
                      "failed - failedDir"))
              .getCount());
      assertEquals(
          2,
          ((Counter) get(Metric.ACTIVE_LOADING_FILES_NUMBER, Tag.TYPE.toString(), "queuing"))
              .getCount());
    } finally {
      metrics.increaseQueuingFileCounter(-2);
    }
  }

  @Test
  public void testRpcServiceThriftHandlerMetrics() {
    final RPCServiceThriftHandlerMetrics metrics = RPCServiceThriftHandlerMetrics.getInstance();
    bind(metrics);
    bind(new RPCServiceThriftHandlerMetrics(new AtomicLong(5)));

    service.restartService();
    metrics.recordMemoryUsage(7);
    assertEquals(5, ((AutoGauge) get(Metric.THRIFT_CONNECTIONS)).getValue(), 0);
    assertEquals(7, ((Gauge) get(Metric.THRIFT_RPC_MEMORY_USAGE)).getValue());
  }

  @Test
  public void testCacheMetrics() {
    final CacheMetrics metrics = CacheMetrics.getInstance();
    bind(metrics);

    service.restartService();
    metrics.record(true, CacheMetrics.DATABASE_CACHE_NAME);
    assertEquals(
        1,
        ((Counter)
                get(
                    Metric.CACHE,
                    Tag.NAME.toString(),
                    CacheMetrics.DATABASE_CACHE_NAME,
                    Tag.TYPE.toString(),
                    "hit"))
            .getCount());
  }

  private void bind(final IMetricSet metricSet) {
    service.addMetricSet(metricSet);
    boundMetricSets.add(metricSet);
  }

  private long getLoadTsFileOtherMemory() {
    return ((Gauge)
            get(
                Metric.LOAD_MEM,
                Tag.NAME.toString(),
                LoadTsFileMemMetricSet.LOAD_TSFILE_OTHER_MEMORY))
        .getValue();
  }

  private long countActiveMemTableCounters(final DataRegionId dataRegionId) {
    // Match both forms of the region tag
    return service.getAllMetrics().keySet().stream()
        .filter(
            info ->
                Metric.ACTIVE_MEMTABLE_COUNT.toString().equals(info.getName())
                    && (dataRegionId.toString().equals(info.getTags().get(Tag.REGION.toString()))
                        || String.valueOf(dataRegionId.getId())
                            .equals(info.getTags().get(Tag.REGION.toString()))))
        .count();
  }

  private long count(final Metric metric, final String... tags) {
    return service.getAllMetrics().keySet().stream()
        .filter(info -> metric.toString().equals(info.getName()) && hasTags(info, tags))
        .count();
  }

  private IMetric get(final Metric metric, final String... tags) {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (metric.toString().equals(entry.getKey().getName()) && hasTags(entry.getKey(), tags)) {
        return entry.getValue();
      }
    }
    throw new AssertionError(metric + Arrays.toString(tags) + " is not registered");
  }

  private static boolean hasTags(final MetricInfo info, final String... tags) {
    for (int i = 0; i < tags.length; i += 2) {
      if (!tags[i + 1].equals(info.getTags().get(tags[i]))) {
        return false;
      }
    }
    return true;
  }
}
