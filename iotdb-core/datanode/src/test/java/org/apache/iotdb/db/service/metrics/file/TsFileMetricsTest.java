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

package org.apache.iotdb.db.service.metrics.file;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TsFileMetricsTest {

  private static final String DATABASE = "root.tsfile_metrics_test";
  private static final String REGION = "1";

  private final TsFileMetrics metrics = new TsFileMetrics();
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
    service.addMetricSet(metrics);
  }

  @After
  public void tearDown() {
    service.removeMetricSet(metrics);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testFileGaugesSurviveMetricServiceRestart() {
    metrics.addTsFile(mockTsFile("1-1-0-0.tsfile", 100));
    assertFileGauges(1, 100);

    for (final MetricLevel level :
        new MetricLevel[] {MetricLevel.ALL, MetricLevel.IMPORTANT, MetricLevel.IMPORTANT}) {
      config.setMetricLevel(level);
      service.restartService();
      assertFileGauges(1, 100);
    }

    // Updates after the restart must reach the gauges that are currently registered.
    final TsFileResource second = mockTsFile("2-2-0-0.tsfile", 50);
    metrics.addTsFile(second);
    assertFileGauges(2, 150);
    Mockito.when(second.markAsUnrecordedByMetric()).thenReturn(true);
    metrics.deleteFile(Collections.singletonList(second));
    assertFileGauges(1, 100);
  }

  @Test
  public void testFileGaugesAreCreatedWhenLevelIsEnabledByRestart() {
    config.setMetricLevel(MetricLevel.OFF);
    service.restartService();
    metrics.addTsFile(mockTsFile("1-1-0-0.tsfile", 100));

    config.setMetricLevel(MetricLevel.IMPORTANT);
    service.restartService();
    assertFileGauges(1, 100);
  }

  @Test
  public void testFileGaugesSurviveMetricServiceStopAndStart() {
    metrics.addTsFile(mockTsFile("1-1-0-0.tsfile", 100));
    service.stopService();
    // Files recorded while the metric service is stopped are exported once it starts again.
    metrics.addTsFile(mockTsFile("2-2-0-0.tsfile", 50));
    service.startService();
    assertFileGauges(2, 150);
  }

  private static TsFileResource mockTsFile(final String fileName, final long size) {
    final TsFileResource resource = Mockito.mock(TsFileResource.class);
    Mockito.when(resource.markAsRecordedByMetric()).thenReturn(true);
    Mockito.when(resource.getTsFileSize()).thenReturn(size);
    Mockito.when(resource.isSeq()).thenReturn(true);
    Mockito.when(resource.getDatabaseName()).thenReturn(DATABASE);
    Mockito.when(resource.getDataRegionId()).thenReturn(REGION);
    Mockito.when(resource.getTsFile()).thenReturn(new File(fileName));
    return resource;
  }

  private void assertFileGauges(final long count, final long size) {
    assertEquals(count, getGauge("file_global_count", Tag.REGION.toString(), REGION));
    assertEquals(size, getGauge("file_global_size", Tag.REGION.toString(), REGION));
    assertEquals(count, getGauge("file_level_count", "level", "0"));
    assertEquals(size, getGauge("file_level_size", "level", "0"));
  }

  private long getGauge(final String name, final String tagKey, final String tagValue) {
    Gauge gauge = null;
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      final Map<String, String> tags = entry.getKey().getTags();
      if (name.equals(entry.getKey().getName())
          && "seq".equals(tags.get(Tag.NAME.toString()))
          && tagValue.equals(tags.get(tagKey))) {
        gauge = (Gauge) entry.getValue();
      }
    }
    assertNotNull(name, gauge);
    return gauge.getValue();
  }
}
