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

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.resource.wal.PipeWALResourceManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeResourceMetrics implements IMetricSet {

  private static final String PIPE_USED_MEMORY = "PipeUsedMemory";

  private static final String PIPE_TOTAL_MEMORY = "PipeTotalMemory";

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
        PipeMemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        PIPE_TOTAL_MEMORY);
    // resource reference count
    metricService.createAutoGauge(
        Metric.PIPE_PINNED_MEMTABLE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.wal(),
        PipeWALResourceManager::getPinnedWalCount);
    metricService.createAutoGauge(
        Metric.PIPE_LINKED_TSFILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.tsfile(),
        PipeTsFileResourceManager::getLinkedTsfileCount);
    // phantom reference count
    metricService.createAutoGauge(
        Metric.PIPE_PHANTOM_REFERENCE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeDataNodeResourceManager.ref(),
        PipePhantomReferenceManager::getPhantomReferenceCount);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    // pipe memory related
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_MEM.toString(), Tag.NAME.toString(), PIPE_USED_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_MEM.toString(), Tag.NAME.toString(), PIPE_TOTAL_MEMORY);
    // resource reference count
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_PINNED_MEMTABLE_COUNT.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_LINKED_TSFILE_COUNT.toString());
    // phantom reference count
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_PHANTOM_REFERENCE_COUNT.toString());
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
