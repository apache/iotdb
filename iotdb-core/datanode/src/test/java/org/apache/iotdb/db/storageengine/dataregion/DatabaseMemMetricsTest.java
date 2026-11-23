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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ResidentSeriesCounter;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessorInfo;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessorInfoMetrics;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A database has several data regions and TsFileProcessors, whose memory is exported as a single
 * gauge of the database.
 */
public class DatabaseMemMetricsTest {

  private static final String DATABASE = "root.db";

  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
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
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testDatabaseMemSumsUpTheDataRegions() {
    final IMetricSet first = new DataRegionMetrics(mockDataRegion(1, 100));
    final IMetricSet second = new DataRegionMetrics(mockDataRegion(2, 200));
    checkSumOfTwoOwners(first, second, "database_" + DATABASE, 300, 100);
  }

  @Test
  public void testResidentSeriesGaugeFollowsRegionLifecycle() {
    IMetricSet first = new DataRegionMetrics(mockDataRegion(1, 0));
    IMetricSet second = new DataRegionMetrics(mockDataRegion(2, 0));
    service.addMetricSet(first);
    service.addMetricSet(second);
    try {
      service.restartService();
      assertEquals(0, residentSeriesGauge(1).getValue(), 0);
      assertEquals(0, residentSeriesGauge(2).getValue(), 0);
      service.removeMetricSet(first);
      assertNull(residentSeriesGauge(1));
      assertEquals(0, residentSeriesGauge(2).getValue(), 0);
    } finally {
      service.removeMetricSet(first);
      service.removeMetricSet(second);
    }
    assertNull(residentSeriesGauge(2));
  }

  private AutoGauge residentSeriesGauge(int regionId) {
    return (AutoGauge)
        findMetric(
            Metric.RESIDENT_MEMTABLE_SERIES_COUNT,
            Tag.REGION,
            new DataRegionId(regionId).toString());
  }

  @Test
  public void testChunkMetadataMemSumsUpTheTsFileProcessors() {
    final IMetricSet first = new TsFileProcessorInfoMetrics(DATABASE, mockTsFileProcessorInfo(10));
    final IMetricSet second = new TsFileProcessorInfoMetrics(DATABASE, mockTsFileProcessorInfo(20));
    checkSumOfTwoOwners(first, second, "chunkMetaData_" + DATABASE, 30, 10);
  }

  private void checkSumOfTwoOwners(
      final IMetricSet first,
      final IMetricSet second,
      final String name,
      final long sum,
      final long firstValue) {
    service.addMetricSet(first);
    service.addMetricSet(second);
    try {
      assertEquals(sum, getMem(name).getValue(), 0);
      service.restartService();
      assertEquals(sum, getMem(name).getValue(), 0);

      // The gauge stays while the database has any owner left
      service.removeMetricSet(second);
      assertEquals(firstValue, getMem(name).getValue(), 0);
    } finally {
      service.removeMetricSet(first);
      service.removeMetricSet(second);
    }
    assertNull(findMem(name));
  }

  private static DataRegion mockDataRegion(final int regionId, final long memCost) {
    final DataRegion dataRegion = mock(DataRegion.class);
    when(dataRegion.getDataRegionId()).thenReturn(regionId);
    when(dataRegion.getResidentSeriesCounter()).thenReturn(new ResidentSeriesCounter());
    when(dataRegion.getDatabaseName()).thenReturn(DATABASE);
    when(dataRegion.getMemCost()).thenReturn(memCost);
    return dataRegion;
  }

  private static TsFileProcessorInfo mockTsFileProcessorInfo(final long memCost) {
    final TsFileProcessorInfo tsFileProcessorInfo = mock(TsFileProcessorInfo.class);
    when(tsFileProcessorInfo.getMemCost()).thenReturn(memCost);
    return tsFileProcessorInfo;
  }

  private AutoGauge getMem(final String name) {
    final IMetric metric = findMem(name);
    if (metric == null) {
      throw new AssertionError(Metric.MEM + " " + name + " is not registered");
    }
    return (AutoGauge) metric;
  }

  private IMetric findMem(final String name) {
    return findMetric(Metric.MEM, Tag.NAME, name);
  }

  private IMetric findMetric(Metric metric, Tag tag, String name) {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (metric.toString().equals(entry.getKey().getName())
          && name.equals(entry.getKey().getTags().get(tag.toString()))) {
        return entry.getValue();
      }
    }
    return null;
  }
}
