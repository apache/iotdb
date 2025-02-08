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

package org.apache.iotdb.db.queryengine.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class SeriesScanCostMetricSet implements IMetricSet {

  public static SeriesScanCostMetricSet getInstance() {
    return SeriesScanCostMetricSet.InstanceHolder.INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // load timeseries metadata
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public static final String LOAD_TIMESERIES_METADATA = "load_timeseries_metadata";
  public static final String LOAD_ALIGNED_TIMESERIES_METADATA = "load_aligned_timeseries_metadata";
  public static final String ALIGNED = "aligned";
  public static final String NON_ALIGNED = "non_aligned";
  public static final String MEM = "mem";
  public static final String DISK = "disk";
  public static final String MEM_AND_DISK = "mem_and_disk";

  public static final String SEQUENCE = "sequence";
  public static final String UNSEQUENCE = "unsequence";

  public static final String SEQ_AND_UNSEQ = "seq_and_unseq";

  public static final String BLOOM_FILTER = "bloom_filter";
  public static final String TIMESERIES_METADATA = "timeseries_metadata";
  public static final String CHUNK = "chunk";

  private Histogram loadBloomFilterFromCacheCountHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadBloomFilterFromDiskCountHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Counter loadBloomFilterActualIOSizeCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Timer loadBloomFilterTime = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordBloomFilterMetrics(
      long loadBloomFilterFromCacheCount,
      long loadBloomFilterFromDiskCount,
      long loadBloomFilterActualIOSize,
      long loadBloomFilterNanoTime) {
    loadBloomFilterFromCacheCountHistogram.update(loadBloomFilterFromCacheCount);
    loadBloomFilterFromDiskCountHistogram.update(loadBloomFilterFromDiskCount);
    loadBloomFilterActualIOSizeCounter.inc(loadBloomFilterActualIOSize);
    loadBloomFilterTime.updateNanos(loadBloomFilterNanoTime);
  }

  private void bindBloomFilter(AbstractMetricService metricService) {
    loadBloomFilterFromCacheCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_QUERY_CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            BLOOM_FILTER,
            Tag.FROM.toString(),
            CACHE);
    loadBloomFilterFromDiskCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_QUERY_CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            BLOOM_FILTER,
            Tag.FROM.toString(),
            DISK);
    loadBloomFilterActualIOSizeCounter =
        metricService.getOrCreateCounter(
            Metric.QUERY_DISK_READ.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            BLOOM_FILTER);
    loadBloomFilterTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BLOOM_FILTER,
            Tag.TYPE.toString(),
            SEQ_AND_UNSEQ,
            Tag.FROM.toString(),
            MEM_AND_DISK);
  }

  private void unbindBloomFilter(AbstractMetricService metricService) {
    loadBloomFilterFromCacheCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadBloomFilterFromDiskCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadBloomFilterActualIOSizeCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    loadBloomFilterTime = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.METRIC_QUERY_CACHE.toString(),
        Tag.TYPE.toString(),
        BLOOM_FILTER,
        Tag.FROM.toString(),
        CACHE);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.METRIC_QUERY_CACHE.toString(),
        Tag.TYPE.toString(),
        BLOOM_FILTER,
        Tag.FROM.toString(),
        DISK);
    metricService.remove(
        MetricType.COUNTER, Metric.QUERY_DISK_READ.toString(), Tag.TYPE.toString(), BLOOM_FILTER);
    metricService.remove(
        MetricType.TIMER,
        Metric.SERIES_SCAN_COST.toString(),
        Tag.STAGE.toString(),
        BLOOM_FILTER,
        Tag.TYPE.toString(),
        SEQ_AND_UNSEQ,
        Tag.FROM.toString(),
        MEM_AND_DISK);
  }

  private Histogram loadTimeSeriesMetadataDiskSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataDiskUnSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataMemSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataMemUnSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private Histogram loadTimeSeriesMetadataAlignedDiskSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataAlignedDiskUnSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataAlignedMemSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataAlignedMemUnSeqHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private Timer loadTimeSeriesMetadataDiskSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeSeriesMetadataDiskUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeSeriesMetadataMemSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeSeriesMetadataMemUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Timer loadTimeSeriesMetadataAlignedDiskSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeSeriesMetadataAlignedDiskUnSeqTime =
      DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeSeriesMetadataAlignedMemSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeSeriesMetadataAlignedMemUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Histogram loadTimeSeriesMetadataFromCacheCountHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadTimeSeriesMetadataFromDiskCountHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Counter loadTimeSeriesMetadataActualIOSizeCounter =
      DoNothingMetricManager.DO_NOTHING_COUNTER;

  public void recordNonAlignedTimeSeriesMetadataCount(long c1, long c2, long c3, long c4) {
    loadTimeSeriesMetadataDiskSeqHistogram.update(c1);
    loadTimeSeriesMetadataDiskUnSeqHistogram.update(c2);
    loadTimeSeriesMetadataMemSeqHistogram.update(c3);
    loadTimeSeriesMetadataMemUnSeqHistogram.update(c4);
  }

  public void recordNonAlignedTimeSeriesMetadataTime(long t1, long t2, long t3, long t4) {
    loadTimeSeriesMetadataDiskSeqTime.updateNanos(t1);
    loadTimeSeriesMetadataDiskUnSeqTime.updateNanos(t2);
    loadTimeSeriesMetadataMemSeqTime.updateNanos(t3);
    loadTimeSeriesMetadataMemUnSeqTime.updateNanos(t4);
  }

  public void recordAlignedTimeSeriesMetadataCount(long c1, long c2, long c3, long c4) {
    loadTimeSeriesMetadataAlignedDiskSeqHistogram.update(c1);
    loadTimeSeriesMetadataAlignedDiskUnSeqHistogram.update(c2);
    loadTimeSeriesMetadataAlignedMemSeqHistogram.update(c3);
    loadTimeSeriesMetadataAlignedMemUnSeqHistogram.update(c4);
  }

  public void recordAlignedTimeSeriesMetadataTime(long t1, long t2, long t3, long t4) {
    loadTimeSeriesMetadataAlignedDiskSeqTime.updateNanos(t1);
    loadTimeSeriesMetadataAlignedDiskUnSeqTime.updateNanos(t2);
    loadTimeSeriesMetadataAlignedMemSeqTime.updateNanos(t3);
    loadTimeSeriesMetadataAlignedMemUnSeqTime.updateNanos(t4);
  }

  public void recordTimeSeriesMetadataMetrics(
      long loadTimeSeriesMetadataFromCacheCount,
      long loadTimeSeriesMetadataFromDiskCount,
      long loadTimeSeriesMetadataActualIOSize) {
    loadTimeSeriesMetadataFromCacheCountHistogram.update(loadTimeSeriesMetadataFromCacheCount);
    loadTimeSeriesMetadataFromDiskCountHistogram.update(loadTimeSeriesMetadataFromDiskCount);
    loadTimeSeriesMetadataActualIOSizeCounter.inc(loadTimeSeriesMetadataActualIOSize);
  }

  private void bindTimeseriesMetadata(AbstractMetricService metricService) {
    loadTimeSeriesMetadataDiskSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataDiskUnSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataMemSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            MEM);
    loadTimeSeriesMetadataMemUnSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            MEM);

    loadTimeSeriesMetadataDiskSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataDiskUnSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataMemSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            MEM);
    loadTimeSeriesMetadataMemUnSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            MEM);
  }

  private void bindAlignedTimeseriesMetadata(AbstractMetricService metricService) {
    loadTimeSeriesMetadataAlignedDiskSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataAlignedDiskUnSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataAlignedMemSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            MEM);
    loadTimeSeriesMetadataAlignedMemUnSeqHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            MEM);

    loadTimeSeriesMetadataAlignedDiskSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataAlignedDiskUnSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataAlignedMemSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            SEQUENCE,
            Tag.FROM.toString(),
            MEM);
    loadTimeSeriesMetadataAlignedMemUnSeqTime =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_ALIGNED_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            UNSEQUENCE,
            Tag.FROM.toString(),
            MEM);
  }

  private void bindTimeSeriesMetadataCache(AbstractMetricService metricService) {
    loadTimeSeriesMetadataFromCacheCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_QUERY_CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TIMESERIES_METADATA,
            Tag.FROM.toString(),
            CACHE);
    loadTimeSeriesMetadataFromDiskCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_QUERY_CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TIMESERIES_METADATA,
            Tag.FROM.toString(),
            DISK);
    loadTimeSeriesMetadataActualIOSizeCounter =
        metricService.getOrCreateCounter(
            Metric.QUERY_DISK_READ.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TIMESERIES_METADATA);
  }

  private void unbindTimeSeriesMetadataCache(AbstractMetricService metricService) {
    loadTimeSeriesMetadataFromCacheCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataFromDiskCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataActualIOSizeCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.METRIC_QUERY_CACHE.toString(),
        Tag.TYPE.toString(),
        TIMESERIES_METADATA,
        Tag.FROM.toString(),
        CACHE);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.METRIC_QUERY_CACHE.toString(),
        Tag.TYPE.toString(),
        TIMESERIES_METADATA,
        Tag.FROM.toString(),
        DISK);
    metricService.remove(
        MetricType.COUNTER,
        Metric.QUERY_DISK_READ.toString(),
        Tag.TYPE.toString(),
        TIMESERIES_METADATA);
  }

  private void unbindTimeseriesMetadata(AbstractMetricService metricService) {
    loadTimeSeriesMetadataDiskSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataDiskUnSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataMemSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataMemUnSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataAlignedDiskSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataAlignedDiskUnSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataAlignedMemSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadTimeSeriesMetadataAlignedMemUnSeqHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

    loadTimeSeriesMetadataDiskSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataDiskUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataMemSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataMemUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataAlignedDiskSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataAlignedDiskUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataAlignedMemSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeSeriesMetadataAlignedMemUnSeqTime = DoNothingMetricManager.DO_NOTHING_TIMER;

    for (String type : Arrays.asList(ALIGNED, NON_ALIGNED)) {
      for (String from : Arrays.asList(MEM, DISK)) {
        for (String stage :
            Arrays.asList(LOAD_TIMESERIES_METADATA, LOAD_ALIGNED_TIMESERIES_METADATA)) {
          metricService.remove(
              MetricType.HISTOGRAM,
              Metric.METRIC_LOAD_TIME_SERIES_METADATA.toString(),
              Tag.STAGE.toString(),
              stage,
              Tag.TYPE.toString(),
              type,
              Tag.FROM.toString(),
              from);

          metricService.remove(
              MetricType.TIMER,
              Metric.SERIES_SCAN_COST.toString(),
              Tag.STAGE.toString(),
              LOAD_ALIGNED_TIMESERIES_METADATA,
              Tag.TYPE.toString(),
              type,
              Tag.FROM.toString(),
              from);
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // read timeseries metadata
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String READ_TIMESERIES_METADATA = "read_timeseries_metadata";
  private static final String CACHE = "cache";
  private static final String FILE = "file";
  private static final String NULL = "null";
  public static final String READ_TIMESERIES_METADATA_CACHE =
      READ_TIMESERIES_METADATA + "_" + CACHE;
  public static final String READ_TIMESERIES_METADATA_FILE = READ_TIMESERIES_METADATA + "_" + FILE;
  private Timer readTimeseriesMetadataCacheTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer readTimeseriesMetadataFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindReadTimeseriesMetadata(AbstractMetricService metricService) {
    readTimeseriesMetadataCacheTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            READ_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            CACHE);
    readTimeseriesMetadataFileTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            READ_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            FILE);
  }

  private void unbindReadTimeseriesMetadata(AbstractMetricService metricService) {
    readTimeseriesMetadataCacheTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    readTimeseriesMetadataFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(CACHE, FILE)
        .forEach(
            from ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.SERIES_SCAN_COST.toString(),
                    Tag.STAGE.toString(),
                    READ_TIMESERIES_METADATA,
                    Tag.TYPE.toString(),
                    NULL,
                    Tag.FROM.toString(),
                    from));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // read timeseries metadata aligned
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String TIMESERIES_METADATA_MODIFICATION = "timeseries_metadata_modification";
  private static final String HISTOGRAM_TIMESERIES_METADATA_MODIFICATION =
      "histogram_timeseries_metadata_modification";
  public static final String TIMESERIES_METADATA_MODIFICATION_ALIGNED =
      TIMESERIES_METADATA_MODIFICATION + "_" + ALIGNED;
  public static final String TIMESERIES_METADATA_MODIFICATION_NONALIGNED =
      TIMESERIES_METADATA_MODIFICATION + "_" + NON_ALIGNED;
  private Histogram timeseriesMetadataModificationAlignedHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram timeseriesMetadataModificationNonAlignedHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Timer timeseriesMetadataModificationAlignedTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer timeseriesMetadataModificationNonAlignedTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordTimeSeriesMetadataModification(
      long alignedCount, long nonAlignedCount, long alignedTime, long nonAlignedTime) {
    timeseriesMetadataModificationAlignedHistogram.update(alignedCount);
    timeseriesMetadataModificationNonAlignedHistogram.update(nonAlignedCount);
    timeseriesMetadataModificationAlignedTimer.updateNanos(alignedTime);
    timeseriesMetadataModificationNonAlignedTimer.updateNanos(nonAlignedTime);
  }

  private void bindTimeseriesMetadataModification(AbstractMetricService metricService) {
    timeseriesMetadataModificationAlignedHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_TIMESERIES_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            NULL);
    timeseriesMetadataModificationNonAlignedHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_TIMESERIES_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            NULL);

    timeseriesMetadataModificationAlignedTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            TIMESERIES_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            NULL);
    timeseriesMetadataModificationNonAlignedTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            TIMESERIES_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            NULL);
  }

  private void unbindTimeseriesMetadataModification(AbstractMetricService metricService) {
    timeseriesMetadataModificationAlignedHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    timeseriesMetadataModificationNonAlignedHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    timeseriesMetadataModificationAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    timeseriesMetadataModificationNonAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.SERIES_SCAN_COST.toString(),
                    Tag.STAGE.toString(),
                    TIMESERIES_METADATA_MODIFICATION,
                    Tag.TYPE.toString(),
                    type,
                    Tag.FROM.toString(),
                    NULL));
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.HISTOGRAM,
                    Metric.SERIES_SCAN_COST.toString(),
                    Tag.STAGE.toString(),
                    HISTOGRAM_TIMESERIES_METADATA_MODIFICATION,
                    Tag.TYPE.toString(),
                    type,
                    Tag.FROM.toString(),
                    NULL));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // load chunk metadata list
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String LOAD_CHUNK_METADATA_LIST = "load_chunk_metadata_list";
  public static final String LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM =
      LOAD_CHUNK_METADATA_LIST + "_" + ALIGNED + "_" + MEM;
  public static final String LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK =
      LOAD_CHUNK_METADATA_LIST + "_" + ALIGNED + "_" + DISK;
  public static final String LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM =
      LOAD_CHUNK_METADATA_LIST + "_" + NON_ALIGNED + "_" + MEM;
  public static final String LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK =
      LOAD_CHUNK_METADATA_LIST + "_" + NON_ALIGNED + "_" + DISK;
  private Timer loadChunkMetadataListAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadChunkMetadataListAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadChunkMetadataListNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadChunkMetadataListNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindLoadChunkMetadataList(AbstractMetricService metricService) {
    loadChunkMetadataListAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    loadChunkMetadataListAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    loadChunkMetadataListNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    loadChunkMetadataListNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
  }

  private void unbindLoadChunkMetadataList(AbstractMetricService metricService) {
    loadChunkMetadataListAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadChunkMetadataListAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadChunkMetadataListNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadChunkMetadataListNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                Arrays.asList(MEM, DISK)
                    .forEach(
                        from ->
                            metricService.remove(
                                MetricType.TIMER,
                                Metric.SERIES_SCAN_COST.toString(),
                                Tag.STAGE.toString(),
                                LOAD_CHUNK_METADATA_LIST,
                                Tag.TYPE.toString(),
                                type,
                                Tag.FROM.toString(),
                                from)));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // chunk metadata modification
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String CHUNK_METADATA_MODIFICATION = "chunk_metadata_modification";
  public static final String CHUNK_METADATA_MODIFICATION_ALIGNED_MEM =
      CHUNK_METADATA_MODIFICATION + "_" + ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_MODIFICATION_ALIGNED_DISK =
      CHUNK_METADATA_MODIFICATION + "_" + ALIGNED + "_" + DISK;
  public static final String CHUNK_METADATA_MODIFICATION_NONALIGNED_MEM =
      CHUNK_METADATA_MODIFICATION + "_" + NON_ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK =
      CHUNK_METADATA_MODIFICATION + "_" + NON_ALIGNED + "_" + DISK;
  private Timer chunkMetadataModificationAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer chunkMetadataModificationAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer chunkMetadataModificationNonAlignedMemTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer chunkMetadataModificationNonAlignedDiskTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindChunkMetadataModification(AbstractMetricService metricService) {
    constructChunkReadersAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReadersAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    constructChunkReadersNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReadersNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
  }

  private void unbindChunkMetadataModification(AbstractMetricService metricService) {
    chunkMetadataModificationAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    chunkMetadataModificationAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    chunkMetadataModificationNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    chunkMetadataModificationNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                Arrays.asList(MEM, DISK)
                    .forEach(
                        from ->
                            metricService.remove(
                                MetricType.TIMER,
                                Metric.SERIES_SCAN_COST.toString(),
                                Tag.STAGE.toString(),
                                CHUNK_METADATA_MODIFICATION,
                                Tag.TYPE.toString(),
                                type,
                                Tag.FROM.toString(),
                                from)));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // chunk metadata filter
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String CHUNK_METADATA_FILTER = "chunk_metadata_filter";
  public static final String CHUNK_METADATA_FILTER_ALIGNED_MEM =
      CHUNK_METADATA_FILTER + "_" + ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_FILTER_ALIGNED_DISK =
      CHUNK_METADATA_FILTER + "_" + ALIGNED + "_" + DISK;
  public static final String CHUNK_METADATA_FILTER_NONALIGNED_MEM =
      CHUNK_METADATA_FILTER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_FILTER_NONALIGNED_DISK =
      CHUNK_METADATA_FILTER + "_" + NON_ALIGNED + "_" + DISK;

  private Timer chunkMetadataFilterAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer chunkMetadataFilterAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer chunkMetadataFilterNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer chunkMetadataFilterNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindChunkMetadataFilter(AbstractMetricService metricService) {
    chunkMetadataModificationAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    chunkMetadataModificationAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    chunkMetadataModificationNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
    chunkMetadataModificationNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
  }

  private void unbindChunkMetadataFilter(AbstractMetricService metricService) {
    chunkMetadataFilterAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    chunkMetadataFilterAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    chunkMetadataFilterNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    chunkMetadataFilterNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                Arrays.asList(MEM, DISK)
                    .forEach(
                        from ->
                            metricService.remove(
                                MetricType.TIMER,
                                Metric.SERIES_SCAN_COST.toString(),
                                Tag.STAGE.toString(),
                                CHUNK_METADATA_FILTER,
                                Tag.TYPE.toString(),
                                type,
                                Tag.FROM.toString(),
                                from)));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // construct chunk reader
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String CONSTRUCT_CHUNK_READER = "construct_chunk_reader";
  private static final String HISTOGRAM_CONSTRUCT_CHUNK_READER = "histogram_construct_chunk_reader";

  private Histogram constructChunkReadersAlignedMemHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram constructChunkReadersAlignedDiskHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram constructChunkReadersNonAlignedMemHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram constructChunkReadersNonAlignedDiskHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private Timer constructChunkReadersAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer constructChunkReadersAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer constructChunkReadersNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer constructChunkReadersNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Histogram loadChunkFromCacheCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram loadChunkFromDiskCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Counter loadChunkActualIOSizeCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  public void recordConstructChunkReadersCount(
      long alignedMemCount,
      long alignedDiskCount,
      long nonAlignedMemCount,
      long nonAlignedDiskCount) {
    constructChunkReadersAlignedMemHistogram.update(alignedMemCount);
    constructChunkReadersAlignedDiskHistogram.update(alignedDiskCount);
    constructChunkReadersNonAlignedMemHistogram.update(nonAlignedMemCount);
    constructChunkReadersNonAlignedDiskHistogram.update(nonAlignedDiskCount);
  }

  public void recordConstructChunkReadersTime(
      long alignedMemTime, long alignedDiskTime, long nonAlignedMemTime, long nonAlignedDiskTime) {
    constructChunkReadersAlignedMemTimer.updateNanos(alignedMemTime);
    constructChunkReadersAlignedDiskTimer.updateNanos(alignedDiskTime);
    constructChunkReadersNonAlignedMemTimer.updateNanos(nonAlignedMemTime);
    constructChunkReadersNonAlignedDiskTimer.updateNanos(nonAlignedDiskTime);
  }

  public void recordChunkMetrics(
      long loadChunkFromCacheCount, long loadChunkFromDiskCount, long loadChunkActualIOSize) {
    loadChunkFromCacheCountHistogram.update(loadChunkFromCacheCount);
    loadChunkFromDiskCountHistogram.update(loadChunkFromDiskCount);
    loadChunkActualIOSizeCounter.inc(loadChunkActualIOSize);
  }

  private void bindChunk(AbstractMetricService metricService) {
    loadChunkFromCacheCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_QUERY_CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            CHUNK,
            Tag.FROM.toString(),
            CACHE);
    loadChunkFromDiskCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.METRIC_QUERY_CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            CHUNK,
            Tag.FROM.toString(),
            DISK);
    loadChunkActualIOSizeCounter =
        metricService.getOrCreateCounter(
            Metric.QUERY_DISK_READ.toString(), MetricLevel.IMPORTANT, Tag.TYPE.toString(), CHUNK);
  }

  private void unbindChunk(AbstractMetricService metricService) {
    loadChunkFromCacheCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadChunkFromDiskCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    loadChunkActualIOSizeCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.METRIC_QUERY_CACHE.toString(),
        Tag.TYPE.toString(),
        CHUNK,
        Tag.FROM.toString(),
        CACHE);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.METRIC_QUERY_CACHE.toString(),
        Tag.TYPE.toString(),
        CHUNK,
        Tag.FROM.toString(),
        DISK);
    metricService.remove(
        MetricType.COUNTER, Metric.QUERY_DISK_READ.toString(), Tag.TYPE.toString(), CHUNK);
  }

  private void bindConstructChunkReader(AbstractMetricService metricService) {
    constructChunkReadersAlignedMemHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReadersAlignedDiskHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    constructChunkReadersNonAlignedMemHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReadersNonAlignedDiskHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);

    constructChunkReadersAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReadersAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    constructChunkReadersNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReadersNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
  }

  private void unbindConstructChunkReader(AbstractMetricService metricService) {
    constructChunkReadersAlignedMemHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    constructChunkReadersAlignedDiskHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    constructChunkReadersNonAlignedMemHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    constructChunkReadersNonAlignedDiskHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

    constructChunkReadersAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    constructChunkReadersAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    constructChunkReadersNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    constructChunkReadersNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    for (String type : Arrays.asList(ALIGNED, NON_ALIGNED)) {
      for (String from : Arrays.asList(MEM, DISK)) {
        metricService.remove(
            MetricType.HISTOGRAM,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            HISTOGRAM_CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            type,
            Tag.FROM.toString(),
            from);

        metricService.remove(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            type,
            Tag.FROM.toString(),
            from);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // read chunk
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String READ_CHUNK = "read_chunk";
  private static final String ALL = "all";
  public static final String READ_CHUNK_CACHE = READ_CHUNK + "_" + CACHE;
  public static final String READ_CHUNK_FILE = READ_CHUNK + "_" + FILE;
  private Timer readChunkCacheTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer readChunkFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindReadChunk(AbstractMetricService metricService) {
    readChunkCacheTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            READ_CHUNK_CACHE,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            CACHE);
    readChunkFileTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            READ_CHUNK_FILE,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            FILE);
  }

  private void unbindReadChunk(AbstractMetricService metricService) {
    readChunkCacheTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    readChunkFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(CACHE, FILE)
        .forEach(
            from ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.SERIES_SCAN_COST.toString(),
                    Tag.STAGE.toString(),
                    READ_CHUNK + "_" + from,
                    Tag.TYPE.toString(),
                    NULL,
                    Tag.FROM.toString(),
                    from));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // init chunk reader
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String INIT_CHUNK_READER = "init_chunk_reader";
  public static final String INIT_CHUNK_READER_ALIGNED_MEM =
      INIT_CHUNK_READER + "_" + ALIGNED + "_" + MEM;
  public static final String INIT_CHUNK_READER_ALIGNED_DISK =
      INIT_CHUNK_READER + "_" + ALIGNED + "_" + DISK;
  public static final String INIT_CHUNK_READER_NONALIGNED_MEM =
      INIT_CHUNK_READER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String INIT_CHUNK_READER_NONALIGNED_DISK =
      INIT_CHUNK_READER + "_" + NON_ALIGNED + "_" + DISK;
  private Timer initChunkReaderAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer initChunkReaderAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer initChunkReaderNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer initChunkReaderNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindInitChunkReader(AbstractMetricService metricService) {
    initChunkReaderAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    initChunkReaderAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    initChunkReaderNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    initChunkReaderNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
  }

  private void unbindInitChunkReader(AbstractMetricService metricService) {
    initChunkReaderAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    initChunkReaderAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    initChunkReaderNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    initChunkReaderNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    for (String type : Arrays.asList(ALIGNED, NON_ALIGNED)) {
      for (String from : Arrays.asList(MEM, DISK)) {
        metricService.remove(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            type,
            Tag.FROM.toString(),
            from);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // build tsblock from page reader
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String BUILD_TSBLOCK_FROM_PAGE_READER = "build_tsblock_from_page_reader";
  private static final String HISTOGRAM_BUILD_TSBLOCK_FROM_PAGE_READER =
      "histogram_build_tsblock_from_page_reader";
  private static final String PAGE_READER_MAX_USED_MEMORY_SIZE = "page_reader_max_used_memory_size";

  private Histogram pageReadersDecodeAlignedMemHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram pageReadersDecodeAlignedDiskHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram pageReadersDecodeNonAlignedMemHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram pageReadersDecodeNonAlignedDiskHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private Histogram pageReaderMaxMemoryHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private Timer pageReadersDecodeAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer pageReadersDecodeAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer pageReadersDecodeNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer pageReadersDecodeNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordPageReadersDecompressCount(
      long alignedMemCount,
      long alignedDiskCount,
      long nonAlignedMemCount,
      long nonAlignedDiskCount) {
    pageReadersDecodeAlignedMemHistogram.update(alignedMemCount);
    pageReadersDecodeAlignedDiskHistogram.update(alignedDiskCount);
    pageReadersDecodeNonAlignedMemHistogram.update(nonAlignedMemCount);
    pageReadersDecodeNonAlignedDiskHistogram.update(nonAlignedDiskCount);
  }

  public void recordPageReadersDecompressTime(
      long alignedMemTime, long alignedDiskTime, long nonAlignedMemTime, long nonAlignedDiskTime) {
    pageReadersDecodeAlignedMemTimer.updateNanos(alignedMemTime);
    pageReadersDecodeAlignedDiskTimer.updateNanos(alignedDiskTime);
    pageReadersDecodeNonAlignedMemTimer.updateNanos(nonAlignedMemTime);
    pageReadersDecodeNonAlignedDiskTimer.updateNanos(nonAlignedDiskTime);
  }

  public void updatePageReaderMemoryUsage(long memorySize) {
    pageReaderMaxMemoryHistogram.update(memorySize);
  }

  private void bindTsBlockFromPageReader(AbstractMetricService metricService) {
    pageReadersDecodeAlignedMemHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    pageReadersDecodeAlignedDiskHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    pageReadersDecodeNonAlignedMemHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    pageReadersDecodeNonAlignedDiskHistogram =
        metricService.getOrCreateHistogram(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            HISTOGRAM_BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);

    pageReadersDecodeAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    pageReadersDecodeAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    pageReadersDecodeNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    pageReadersDecodeNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);

    pageReaderMaxMemoryHistogram =
        metricService.getOrCreateHistogram(
            Metric.MEMORY_USAGE_MONITOR.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            PAGE_READER_MAX_USED_MEMORY_SIZE);
  }

  private void unbindTsBlockFromPageReader(AbstractMetricService metricService) {
    pageReadersDecodeAlignedMemHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    pageReadersDecodeAlignedDiskHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    pageReadersDecodeNonAlignedMemHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    pageReadersDecodeNonAlignedDiskHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    pageReaderMaxMemoryHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

    pageReadersDecodeAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    pageReadersDecodeAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    pageReadersDecodeNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    pageReadersDecodeNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    for (String type : Arrays.asList(ALIGNED, NON_ALIGNED)) {
      for (String from : Arrays.asList(MEM, DISK)) {
        metricService.remove(
            MetricType.HISTOGRAM,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            HISTOGRAM_BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            type,
            Tag.FROM.toString(),
            from);

        metricService.remove(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            type,
            Tag.FROM.toString(),
            from);
      }
    }

    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.MEMORY_USAGE_MONITOR.toString(),
        Tag.TYPE.toString(),
        PAGE_READER_MAX_USED_MEMORY_SIZE);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // build tsblock from merge reader
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String BUILD_TSBLOCK_FROM_MERGE_READER = "build_tsblock_from_merge_reader";
  public static final String BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED =
      BUILD_TSBLOCK_FROM_MERGE_READER + "_" + ALIGNED;
  public static final String BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED =
      BUILD_TSBLOCK_FROM_MERGE_READER + "_" + NON_ALIGNED;

  private Timer buildTsBlockFromMergeReaderAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer buildTsBlockFromMergeReaderNonAlignedTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindBuildTsBlockFromMergeReader(AbstractMetricService metricService) {
    buildTsBlockFromMergeReaderAlignedTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_MERGE_READER,
            Tag.FROM.toString(),
            NULL,
            Tag.TYPE.toString(),
            ALIGNED);
    buildTsBlockFromMergeReaderNonAlignedTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_MERGE_READER,
            Tag.FROM.toString(),
            NULL,
            Tag.TYPE.toString(),
            NON_ALIGNED);
  }

  private void unbindBuildTsBlockFromMergeReader(AbstractMetricService metricService) {
    buildTsBlockFromMergeReaderAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    buildTsBlockFromMergeReaderNonAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.SERIES_SCAN_COST.toString(),
                    Tag.STAGE.toString(),
                    BUILD_TSBLOCK_FROM_MERGE_READER,
                    Tag.FROM.toString(),
                    NULL,
                    Tag.TYPE.toString(),
                    type));
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindBloomFilter(metricService);
    bindTimeseriesMetadata(metricService);
    bindAlignedTimeseriesMetadata(metricService);
    bindTimeSeriesMetadataCache(metricService);
    bindReadTimeseriesMetadata(metricService);
    bindTimeseriesMetadataModification(metricService);
    bindLoadChunkMetadataList(metricService);
    bindChunkMetadataModification(metricService);
    bindChunkMetadataFilter(metricService);
    bindConstructChunkReader(metricService);
    bindReadChunk(metricService);
    bindChunk(metricService);
    bindInitChunkReader(metricService);
    bindTsBlockFromPageReader(metricService);
    bindBuildTsBlockFromMergeReader(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindBloomFilter(metricService);
    unbindTimeseriesMetadata(metricService);
    unbindTimeSeriesMetadataCache(metricService);
    unbindReadTimeseriesMetadata(metricService);
    unbindTimeseriesMetadataModification(metricService);
    unbindLoadChunkMetadataList(metricService);
    unbindChunkMetadataModification(metricService);
    unbindChunkMetadataFilter(metricService);
    unbindConstructChunkReader(metricService);
    unbindReadChunk(metricService);
    unbindChunk(metricService);
    unbindInitChunkReader(metricService);
    unbindTsBlockFromPageReader(metricService);
    unbindBuildTsBlockFromMergeReader(metricService);
  }

  public void recordSeriesScanCost(String type, long cost) {
    switch (type) {
      case TIMESERIES_METADATA_MODIFICATION_ALIGNED:
        timeseriesMetadataModificationAlignedTimer.updateNanos(cost);
        break;
      case TIMESERIES_METADATA_MODIFICATION_NONALIGNED:
        timeseriesMetadataModificationNonAlignedTimer.updateNanos(cost);
        break;
      case LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM:
        loadChunkMetadataListAlignedMemTimer.updateNanos(cost);
        break;
      case LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK:
        loadChunkMetadataListAlignedDiskTimer.updateNanos(cost);
        break;
      case LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM:
        loadChunkMetadataListNonAlignedMemTimer.updateNanos(cost);
        break;
      case LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK:
        loadChunkMetadataListNonAlignedDiskTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_FILTER_ALIGNED_MEM:
        chunkMetadataFilterAlignedMemTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_FILTER_ALIGNED_DISK:
        chunkMetadataFilterAlignedDiskTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_FILTER_NONALIGNED_MEM:
        chunkMetadataFilterNonAlignedMemTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_FILTER_NONALIGNED_DISK:
        chunkMetadataFilterNonAlignedDiskTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_MODIFICATION_ALIGNED_MEM:
        chunkMetadataModificationAlignedMemTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_MODIFICATION_ALIGNED_DISK:
        chunkMetadataModificationAlignedDiskTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_MODIFICATION_NONALIGNED_MEM:
        chunkMetadataModificationNonAlignedMemTimer.updateNanos(cost);
        break;
      case CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK:
        chunkMetadataModificationNonAlignedDiskTimer.updateNanos(cost);
        break;
      case INIT_CHUNK_READER_ALIGNED_MEM:
        initChunkReaderAlignedMemTimer.updateNanos(cost);
        break;
      case INIT_CHUNK_READER_ALIGNED_DISK:
        initChunkReaderAlignedDiskTimer.updateNanos(cost);
        break;
      case INIT_CHUNK_READER_NONALIGNED_MEM:
        initChunkReaderNonAlignedMemTimer.updateNanos(cost);
        break;
      case INIT_CHUNK_READER_NONALIGNED_DISK:
        initChunkReaderNonAlignedDiskTimer.updateNanos(cost);
        break;
      case BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED:
        buildTsBlockFromMergeReaderAlignedTimer.updateNanos(cost);
        break;
      case BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED:
        buildTsBlockFromMergeReaderNonAlignedTimer.updateNanos(cost);
        break;
      case READ_TIMESERIES_METADATA_CACHE:
        readTimeseriesMetadataCacheTimer.updateNanos(cost);
        break;
      case READ_TIMESERIES_METADATA_FILE:
        readTimeseriesMetadataFileTimer.updateNanos(cost);
        break;
      case READ_CHUNK_CACHE:
        readChunkCacheTimer.updateNanos(cost);
        break;
      case READ_CHUNK_FILE:
        readChunkFileTimer.updateNanos(cost);
        break;
      default:
        break;
    }
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final SeriesScanCostMetricSet INSTANCE = new SeriesScanCostMetricSet();
  }

  private SeriesScanCostMetricSet() {
    // empty constructor
  }
}
