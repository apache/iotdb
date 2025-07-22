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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeResourceMetrics implements IMetricSet {

  private static final String PIPE_USED_MEMORY = "PipeUsedMemory";
  private static final String PIPE_USED_FLOATING_MEMORY = "PipeUsedFloatingMemory";

  private static final String PIPE_TABLET_USED_MEMORY = "PipeTabletUsedMemory";

  private static final String PIPE_TS_FILE_USED_MEMORY = "PipeTsFileUsedMemory";

  private static final String PIPE_TOTAL_MEMORY = "PipeTotalMemory";

  private Counter diskIOCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private static final String PIPE_FLOATING_MEMORY = "PipeFloatingMemory";

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    // pipe memory related
    metricService.createAutoGauge(
        Metric.PIPE_MEM.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.memory(),
        PipeMemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        PIPE_USED_MEMORY);
    metricService.createAutoGauge(
        Metric.PIPE_MEM.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.memory(),
        PipeMemoryManager::getUsedMemorySizeInBytesOfTablets,
        Tag.NAME.toString(),
        PIPE_TABLET_USED_MEMORY);
    metricService.createAutoGauge(
        Metric.PIPE_MEM.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.memory(),
        PipeMemoryManager::getUsedMemorySizeInBytesOfTsFiles,
        Tag.NAME.toString(),
        PIPE_TS_FILE_USED_MEMORY);
    metricService.createAutoGauge(
        Metric.PIPE_MEM.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.memory(),
        PipeMemoryManager::getTotalNonFloatingMemorySizeInBytes,
        Tag.NAME.toString(),
        PIPE_TOTAL_MEMORY);
    metricService.createAutoGauge(
        Metric.PIPE_MEM.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.memory(),
        o -> PipeDataNodeResourceManager.memory().getTotalFloatingMemorySizeInBytes(),
        Tag.NAME.toString(),
        PIPE_FLOATING_MEMORY);
    metricService.createAutoGauge(
        Metric.PIPE_MEM.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.memory(),
        o -> PipeDataNodeAgent.task().getAllFloatingMemoryUsageInByte(),
        Tag.NAME.toString(),
        PIPE_USED_FLOATING_MEMORY);
    // phantom reference count
    metricService.createAutoGauge(
        Metric.PIPE_PHANTOM_REFERENCE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.ref(),
        PipePhantomReferenceManager::getPhantomReferenceCount);
    // tsFile send rate
    diskIOCounter =
        metricService.getOrCreateCounter(
            Metric.PIPE_TSFILE_SEND_DISK_IO.toString(), MetricLevel.IMPORTANT);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    // pipe memory related
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_MEM.toString(), Tag.NAME.toString(), PIPE_USED_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_MEM.toString(),
        Tag.NAME.toString(),
        PIPE_TABLET_USED_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_MEM.toString(),
        Tag.NAME.toString(),
        PIPE_TS_FILE_USED_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_MEM.toString(), Tag.NAME.toString(), PIPE_TOTAL_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_MEM.toString(),
        Tag.NAME.toString(),
        PIPE_FLOATING_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_MEM.toString(),
        Tag.NAME.toString(),
        PIPE_USED_FLOATING_MEMORY);
    // resource reference count
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_LINKED_TSFILE_COUNT.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_LINKED_TSFILE_SIZE.toString());
    // phantom reference count
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_PHANTOM_REFERENCE_COUNT.toString());

    metricService.remove(MetricType.RATE, Metric.PIPE_TSFILE_SEND_DISK_IO.toString());
  }

  public void recordDiskIO(final long bytes) {
    diskIOCounter.inc(bytes);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeResourceMetricsHolder {

    private static final PipeResourceMetrics INSTANCE = new PipeResourceMetrics();

    private PipeResourceMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeResourceMetrics getInstance() {
    return PipeResourceMetrics.PipeResourceMetricsHolder.INSTANCE;
  }

  private PipeResourceMetrics() {
    // empty constructor
  }
}
