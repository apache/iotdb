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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALInsertNodeCache;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeWALInsertNodeCacheMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeWALInsertNodeCacheMetrics.class);

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PIPE_WAL_INSERT_NODE_CACHE_HIT_RATE.toString(),
        MetricLevel.IMPORTANT,
        WALInsertNodeCache.getInstance(),
        WALInsertNodeCache::getCacheHitRate);
    metricService.createAutoGauge(
        Metric.PIPE_WAL_INSERT_NODE_CACHE_HIT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        WALInsertNodeCache.getInstance(),
        WALInsertNodeCache::getCacheHitCount);
    metricService.createAutoGauge(
        Metric.PIPE_WAL_INSERT_NODE_CACHE_REQUEST_COUNT.toString(),
        MetricLevel.IMPORTANT,
        WALInsertNodeCache.getInstance(),
        WALInsertNodeCache::getCacheRequestCount);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_WAL_INSERT_NODE_CACHE_HIT_RATE.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_WAL_INSERT_NODE_CACHE_HIT_COUNT.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_WAL_INSERT_NODE_CACHE_REQUEST_COUNT.toString());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeWALInsertNodeCacheMetricsHolder {

    private static final PipeWALInsertNodeCacheMetrics INSTANCE =
        new PipeWALInsertNodeCacheMetrics();

    private PipeWALInsertNodeCacheMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeWALInsertNodeCacheMetrics getInstance() {
    return PipeWALInsertNodeCacheMetricsHolder.INSTANCE;
  }

  private PipeWALInsertNodeCacheMetrics() {
    // empty constructor
  }
}
