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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class CacheMetrics implements IMetricSet {
  public static final String DATABASE_CACHE_NAME = "Database";
  public static final String SCHEMA_PARTITION_CACHE_NAME = "SchemaPartition";
  public static final String DATA_PARTITION_CACHE_NAME = "DataPartition";
  private static final String HIT = "hit";
  private static final String ALL = "all";
  private Counter databaseCacheHitCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter schemaPartitionCacheHitCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter dataPartitionCacheHitCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter databaseCacheTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter schemaPartitionCacheTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter dataPartitionCacheTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    databaseCacheHitCounter =
        metricService.getOrCreateCounter(
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            DATABASE_CACHE_NAME,
            Tag.TYPE.toString(),
            HIT);
    schemaPartitionCacheHitCounter =
        metricService.getOrCreateCounter(
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SCHEMA_PARTITION_CACHE_NAME,
            Tag.TYPE.toString(),
            HIT);
    dataPartitionCacheHitCounter =
        metricService.getOrCreateCounter(
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            DATA_PARTITION_CACHE_NAME,
            Tag.TYPE.toString(),
            HIT);
    databaseCacheTotalCounter =
        metricService.getOrCreateCounter(
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            DATABASE_CACHE_NAME,
            Tag.TYPE.toString(),
            ALL);
    schemaPartitionCacheTotalCounter =
        metricService.getOrCreateCounter(
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SCHEMA_PARTITION_CACHE_NAME,
            Tag.TYPE.toString(),
            ALL);
    dataPartitionCacheTotalCounter =
        metricService.getOrCreateCounter(
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            DATA_PARTITION_CACHE_NAME,
            Tag.TYPE.toString(),
            ALL);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Arrays.asList(DATABASE_CACHE_NAME, SCHEMA_PARTITION_CACHE_NAME, DATA_PARTITION_CACHE_NAME)
        .forEach(
            name -> {
              metricService.remove(
                  MetricType.COUNTER,
                  Metric.CACHE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  HIT);
              metricService.remove(
                  MetricType.COUNTER,
                  Metric.CACHE.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  ALL);
            });
    databaseCacheHitCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    schemaPartitionCacheHitCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    dataPartitionCacheHitCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    databaseCacheTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    schemaPartitionCacheTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    dataPartitionCacheTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  }

  public void record(boolean result, String name) {
    switch (name) {
      case DATABASE_CACHE_NAME:
        databaseCacheTotalCounter.inc();
        if (result) {
          databaseCacheHitCounter.inc();
        }
        break;
      case SCHEMA_PARTITION_CACHE_NAME:
        schemaPartitionCacheTotalCounter.inc();
        if (result) {
          schemaPartitionCacheHitCounter.inc();
        }
        break;
      case DATA_PARTITION_CACHE_NAME:
        dataPartitionCacheTotalCounter.inc();
        if (result) {
          dataPartitionCacheHitCounter.inc();
        }
        break;
      default:
        break;
    }
  }
}
