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

package org.apache.iotdb.db.schemaengine.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class SchemaRegionCachedMetric implements ISchemaRegionMetric {

  private static final String PINNED_NODE_NUM = "pbtree_pinned_num";
  private static final String UNPINNED_NODE_NUM = "pbtree_unpinned_num";
  private static final String PINNED_MEM_SIZE = "pbtree_pinned_mem";
  private static final String UNPINNED_MEM_SIZE = "pbtree_unpinned_mem";
  private static final String VOLATILE_NODE_NUM = "pbtree_volatile_node_num";
  private static final String CACHE_NODE_NUM = "pbtree_cache_node_num";
  private static final String MLOG_LENGTH = "pbtree_mlog_length";
  private static final String MLOG_CHECKPOINT = "pbtree_mlog_checkpoint";
  private static final String RELEASE_TIMER = "pbtree_release_timer";
  private static final String RELEASE_MEM = "pbtree_release_mem";
  private static final String RELEASE_NODE = "pbtree_release_node";
  private static final String FLUSH_TIMER = "pbtree_flush_timer";
  private static final String FLUSH_MEM = "pbtree_flush_mem";
  private static final String FLUSH_NODE = "pbtree_flush_node";
  private static final String LOAD_MEM = "pbtree_load_mem";
  private static final String LOAD_NODE = "pbtree_load_node";
  private static final String LOAD_PAGE_NUM = "pbtree_load_page_num";
  private static final String FLUSH_PAGE_NUM = "pbtree_flush_page_num";

  private Timer releaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer flushTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Counter releaseMem = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter releaseNode = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter flushMem = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter flushNode = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter loadMem = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter loadNode = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter loadPageNum = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter flushPageNum = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private final CachedSchemaRegionStatistics regionStatistics;
  private final String regionTagValue;

  // MemSchemaRegionMetric is a subset of CachedSchemaRegionMetric
  private final SchemaRegionMemMetric memSchemaRegionMetric;
  private final String database;

  public SchemaRegionCachedMetric(CachedSchemaRegionStatistics regionStatistics, String database) {
    this.regionStatistics = regionStatistics;
    this.regionTagValue = String.format("SchemaRegion[%d]", regionStatistics.getSchemaRegionId());
    this.memSchemaRegionMetric = new SchemaRegionMemMetric(regionStatistics, database);
    this.database = database;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    memSchemaRegionMetric.bindTo(metricService);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getPinnedMNodeNum,
        Tag.NAME.toString(),
        PINNED_NODE_NUM,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getUnpinnedMNodeNum,
        Tag.NAME.toString(),
        UNPINNED_NODE_NUM,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getPinnedMemorySize,
        Tag.NAME.toString(),
        PINNED_MEM_SIZE,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getUnpinnedMemorySize,
        Tag.NAME.toString(),
        UNPINNED_MEM_SIZE,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getVolatileMNodeNum,
        Tag.NAME.toString(),
        VOLATILE_NODE_NUM,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getCacheNodeNum,
        Tag.NAME.toString(),
        CACHE_NODE_NUM,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getMLogLength,
        Tag.NAME.toString(),
        MLOG_LENGTH,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    metricService.createAutoGauge(
        Metric.SCHEMA_REGION.toString(),
        MetricLevel.IMPORTANT,
        regionStatistics,
        CachedSchemaRegionStatistics::getMlogCheckPoint,
        Tag.NAME.toString(),
        MLOG_CHECKPOINT,
        Tag.REGION.toString(),
        regionTagValue,
        Tag.DATABASE.toString(),
        database);
    flushTimer =
        metricService.getOrCreateTimer(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FLUSH_TIMER,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    releaseTimer =
        metricService.getOrCreateTimer(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RELEASE_TIMER,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    releaseMem =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RELEASE_MEM,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    releaseNode =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            RELEASE_NODE,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    flushMem =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FLUSH_MEM,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    flushNode =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FLUSH_NODE,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    loadNode =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LOAD_NODE,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    loadMem =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LOAD_MEM,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    flushPageNum =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FLUSH_PAGE_NUM,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
    loadPageNum =
        metricService.getOrCreateCounter(
            Metric.SCHEMA_REGION.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LOAD_PAGE_NUM,
            Tag.REGION.toString(),
            regionTagValue,
            Tag.DATABASE.toString(),
            database);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    memSchemaRegionMetric.unbindFrom(metricService);
    releaseTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    flushTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    releaseMem = DoNothingMetricManager.DO_NOTHING_COUNTER;
    releaseNode = DoNothingMetricManager.DO_NOTHING_COUNTER;
    flushMem = DoNothingMetricManager.DO_NOTHING_COUNTER;
    flushNode = DoNothingMetricManager.DO_NOTHING_COUNTER;
    loadMem = DoNothingMetricManager.DO_NOTHING_COUNTER;
    loadNode = DoNothingMetricManager.DO_NOTHING_COUNTER;
    flushPageNum = DoNothingMetricManager.DO_NOTHING_COUNTER;
    loadPageNum = DoNothingMetricManager.DO_NOTHING_COUNTER;
    Arrays.asList(
            PINNED_NODE_NUM,
            UNPINNED_NODE_NUM,
            PINNED_MEM_SIZE,
            UNPINNED_MEM_SIZE,
            VOLATILE_NODE_NUM,
            CACHE_NODE_NUM,
            MLOG_LENGTH,
            MLOG_CHECKPOINT)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.AUTO_GAUGE,
                    Metric.SCHEMA_REGION.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.REGION.toString(),
                    regionTagValue,
                    Tag.DATABASE.toString(),
                    database));
    Arrays.asList(FLUSH_TIMER, RELEASE_TIMER)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.SCHEMA_REGION.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.REGION.toString(),
                    regionTagValue,
                    Tag.DATABASE.toString(),
                    database));
    Arrays.asList(
            RELEASE_MEM,
            RELEASE_NODE,
            FLUSH_MEM,
            FLUSH_NODE,
            LOAD_MEM,
            LOAD_NODE,
            FLUSH_PAGE_NUM,
            LOAD_PAGE_NUM)
        .forEach(
            name ->
                metricService.remove(
                    MetricType.COUNTER,
                    Metric.SCHEMA_REGION.toString(),
                    Tag.NAME.toString(),
                    name,
                    Tag.REGION.toString(),
                    regionTagValue,
                    Tag.DATABASE.toString(),
                    database));
  }

  public void recordRelease(long time, long mem, long node) {
    releaseTimer.update(time, TimeUnit.MILLISECONDS);
    releaseMem.inc(mem);
    releaseNode.inc(node);
  }

  public void recordFlush(long time, long mem, long node) {
    flushTimer.update(time, TimeUnit.MILLISECONDS);
    flushMem.inc(mem);
    flushNode.inc(node);
  }

  public void recordLoadFromDisk(long mem, long node) {
    loadMem.inc(mem);
    loadNode.inc(node);
  }

  public void recordFlushPageNum(int pageNum) {
    flushPageNum.inc(pageNum);
  }

  public void recordLoadPageNum(int pageNum) {
    loadPageNum.inc(pageNum);
  }

  public void recordTraverser(long time) {
    memSchemaRegionMetric.recordTraverser(time);
  }
}
