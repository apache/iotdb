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

package org.apache.iotdb.db.mpp.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class SeriesScanCostMetricSet implements IMetricSet {
  private static final SeriesScanCostMetricSet INSTANCE = new SeriesScanCostMetricSet();

  private SeriesScanCostMetricSet() {
    // empty constructor
  }

  // region load timeseries metadata
  private static final String LOAD_TIMESERIES_METADATA = "load_timeseries_metadata";
  private static final String ALIGNED = "aligned";
  private static final String NON_ALIGNED = "non_aligned";
  private static final String MEM = "mem";
  private static final String DISK = "disk";
  public static final String LOAD_TIMESERIES_METADATA_ALIGNED_MEM =
      LOAD_TIMESERIES_METADATA + "_" + ALIGNED + "_" + MEM;
  public static final String LOAD_TIMESERIES_METADATA_ALIGNED_DISK =
      LOAD_TIMESERIES_METADATA + "_" + ALIGNED + "_" + DISK;
  public static final String LOAD_TIMESERIES_METADATA_NONALIGNED_MEM =
      LOAD_TIMESERIES_METADATA + "_" + NON_ALIGNED + "_" + MEM;
  public static final String LOAD_TIMESERIES_METADATA_NONALIGNED_DISK =
      LOAD_TIMESERIES_METADATA + "_" + NON_ALIGNED + "_" + DISK;
  private Timer loadTimeseriesMetadataAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeseriesMetadataAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeseriesMetadataNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer loadTimeseriesMetadataNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindTimeseriesMetadata(AbstractMetricService metricService) {
    loadTimeseriesMetadataAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    loadTimeseriesMetadataAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    loadTimeseriesMetadataNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    loadTimeseriesMetadataNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
  }

  private void unbindTimeseriesMetadata(AbstractMetricService metricService) {
    loadTimeseriesMetadataAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeseriesMetadataAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeseriesMetadataNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    loadTimeseriesMetadataNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
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
                                LOAD_TIMESERIES_METADATA,
                                Tag.TYPE.toString(),
                                type,
                                Tag.FROM.toString(),
                                from)));
  }
  // endregion

  // region read timeseries metadata
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
            from -> {
              metricService.remove(
                  MetricType.TIMER,
                  Metric.SERIES_SCAN_COST.toString(),
                  Tag.STAGE.toString(),
                  READ_TIMESERIES_METADATA,
                  Tag.TYPE.toString(),
                  NULL,
                  Tag.FROM.toString(),
                  from);
            });
  }
  // endregion

  // region read timeseries metadata aligned
  private static final String TIMESERIES_METADATA_MODIFICATION = "timeseries_metadata_modification";
  public static final String TIMESERIES_METADATA_MODIFICATION_ALIGNED =
      TIMESERIES_METADATA_MODIFICATION + "_" + ALIGNED;
  public static final String TIMESERIES_METADATA_MODIFICATION_NONALIGNED =
      TIMESERIES_METADATA_MODIFICATION + "_" + NON_ALIGNED;
  private Timer timeseriesMetadataModificationAlignedTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer timeseriesMetadataModificationNonAlignedTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindTimeseriesMetadataModification(AbstractMetricService metricService) {
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
    timeseriesMetadataModificationAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    timeseriesMetadataModificationNonAlignedTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type -> {
              metricService.remove(
                  MetricType.TIMER,
                  Metric.SERIES_SCAN_COST.toString(),
                  Tag.STAGE.toString(),
                  TIMESERIES_METADATA_MODIFICATION,
                  Tag.TYPE.toString(),
                  type,
                  Tag.FROM.toString(),
                  NULL);
            });
  }
  // endregion

  // region load chunk metadata list
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
            type -> {
              Arrays.asList(MEM, DISK)
                  .forEach(
                      from -> {
                        metricService.remove(
                            MetricType.TIMER,
                            Metric.SERIES_SCAN_COST.toString(),
                            Tag.STAGE.toString(),
                            LOAD_CHUNK_METADATA_LIST,
                            Tag.TYPE.toString(),
                            type,
                            Tag.FROM.toString(),
                            from);
                      });
            });
  }

  // endregion

  // region chunk metadata modification
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
    constructChunkReaderAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReaderAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    constructChunkReaderNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReaderNonAlignedDiskTimer =
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
  // endregion

  // region chunk metadata filter
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

  // endregion

  // region construct chunk reader
  private static final String CONSTRUCT_CHUNK_READER = "construct_chunk_reader";
  public static final String CONSTRUCT_CHUNK_READER_ALIGNED_MEM =
      CONSTRUCT_CHUNK_READER + "_" + ALIGNED + "_" + MEM;
  public static final String CONSTRUCT_CHUNK_READER_ALIGNED_DISK =
      CONSTRUCT_CHUNK_READER + "_" + ALIGNED + "_" + DISK;
  public static final String CONSTRUCT_CHUNK_READER_NONALIGNED_MEM =
      CONSTRUCT_CHUNK_READER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String CONSTRUCT_CHUNK_READER_NONALIGNED_DISK =
      CONSTRUCT_CHUNK_READER + "_" + NON_ALIGNED + "_" + DISK;
  private Timer constructChunkReaderAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer constructChunkReaderAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer constructChunkReaderNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer constructChunkReaderNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindConstructChunkReader(AbstractMetricService metricService) {
    constructChunkReaderAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReaderAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    constructChunkReaderNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    constructChunkReaderNonAlignedDiskTimer =
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
    constructChunkReaderAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    constructChunkReaderAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    constructChunkReaderNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    constructChunkReaderNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type ->
                Arrays.asList(MEM, DISK)
                    .forEach(
                        from -> {
                          metricService.remove(
                              MetricType.TIMER,
                              Metric.SERIES_SCAN_COST.toString(),
                              Tag.STAGE.toString(),
                              CONSTRUCT_CHUNK_READER,
                              Tag.TYPE.toString(),
                              type,
                              Tag.FROM.toString(),
                              from);
                        }));
  }

  // endregion

  // region read chunk
  private static final String READ_CHUNK = "read_chunk";
  private static final String ALL = "all";
  public static final String READ_CHUNK_ALL = READ_CHUNK + "_" + ALL;
  public static final String READ_CHUNK_FILE = READ_CHUNK + "_" + FILE;
  private Timer readChunkAllTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer readChunkFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindReadChunk(AbstractMetricService metricService) {
    readChunkAllTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            READ_CHUNK,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            ALL);
    readChunkFileTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            READ_CHUNK,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            FILE);
  }

  private void unbindReadChunk(AbstractMetricService metricService) {
    readChunkAllTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    readChunkFileTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALL, FILE)
        .forEach(
            from ->
                metricService.remove(
                    MetricType.TIMER,
                    Metric.SERIES_SCAN_COST.toString(),
                    Tag.STAGE.toString(),
                    READ_CHUNK,
                    Tag.TYPE.toString(),
                    NULL,
                    Tag.FROM.toString(),
                    from));
  }
  // endregion

  // region init chunk reader
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
                                INIT_CHUNK_READER,
                                Tag.TYPE.toString(),
                                type,
                                Tag.FROM.toString(),
                                from)));
  }
  // endregion

  // region build tsblock from page reader
  private static final String BUILD_TSBLOCK_FROM_PAGE_READER = "build_tsblock_from_page_reader";
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + ALIGNED + "_" + MEM;
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + ALIGNED + "_" + DISK;
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + NON_ALIGNED + "_" + DISK;

  private Timer buildTsBlockFromPageReaderAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer buildTsBlockFromPageReaderAlignedDiskTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer buildTsBlockFromPageReaderNonAlignedMemTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer buildTsBlockFromPageReaderNonAlignedDiskTimer =
      DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindTsBlockFromPageReader(AbstractMetricService metricService) {
    buildTsBlockFromPageReaderAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM);
    buildTsBlockFromPageReaderAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK);
    buildTsBlockFromPageReaderNonAlignedMemTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM);
    buildTsBlockFromPageReaderNonAlignedDiskTimer =
        metricService.getOrCreateTimer(
            Metric.SERIES_SCAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK);
  }

  private void unbindTsBlockFromPageReader(AbstractMetricService metricService) {
    buildTsBlockFromPageReaderAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    buildTsBlockFromPageReaderAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    buildTsBlockFromPageReaderNonAlignedMemTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    buildTsBlockFromPageReaderNonAlignedDiskTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(ALIGNED, NON_ALIGNED)
        .forEach(
            type -> {
              Arrays.asList(MEM, DISK)
                  .forEach(
                      from -> {
                        metricService.remove(
                            MetricType.TIMER,
                            Metric.SERIES_SCAN_COST.toString(),
                            Tag.STAGE.toString(),
                            BUILD_TSBLOCK_FROM_PAGE_READER,
                            Tag.TYPE.toString(),
                            type,
                            Tag.FROM.toString(),
                            from);
                      });
            });
  }
  // endregion

  // region build tsblock from merge reader
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
  // endregion

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindTimeseriesMetadata(metricService);
    bindReadTimeseriesMetadata(metricService);
    bindTimeseriesMetadataModification(metricService);
    bindLoadChunkMetadataList(metricService);
    bindChunkMetadataModification(metricService);
    bindChunkMetadataFilter(metricService);
    bindConstructChunkReader(metricService);
    bindReadChunk(metricService);
    bindInitChunkReader(metricService);
    bindTsBlockFromPageReader(metricService);
    bindBuildTsBlockFromMergeReader(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindTimeseriesMetadata(metricService);
    unbindReadTimeseriesMetadata(metricService);
    unbindTimeseriesMetadataModification(metricService);
    unbindLoadChunkMetadataList(metricService);
    unbindChunkMetadataModification(metricService);
    unbindChunkMetadataFilter(metricService);
    unbindConstructChunkReader(metricService);
    unbindReadChunk(metricService);
    unbindInitChunkReader(metricService);
    unbindTsBlockFromPageReader(metricService);
    unbindBuildTsBlockFromMergeReader(metricService);
  }

  public void recordSeriesScanCost(String type, long cost) {
    switch (type) {
      case LOAD_TIMESERIES_METADATA_ALIGNED_MEM:
        loadTimeseriesMetadataAlignedMemTimer.updateNanos(cost);
        break;
      case LOAD_TIMESERIES_METADATA_ALIGNED_DISK:
        loadTimeseriesMetadataAlignedDiskTimer.updateNanos(cost);
        break;
      case LOAD_TIMESERIES_METADATA_NONALIGNED_MEM:
        loadTimeseriesMetadataNonAlignedMemTimer.updateNanos(cost);
        break;
      case LOAD_TIMESERIES_METADATA_NONALIGNED_DISK:
        loadTimeseriesMetadataNonAlignedDiskTimer.updateNanos(cost);
        break;
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
      case CONSTRUCT_CHUNK_READER_ALIGNED_MEM:
        constructChunkReaderAlignedMemTimer.updateNanos(cost);
        break;
      case CONSTRUCT_CHUNK_READER_ALIGNED_DISK:
        constructChunkReaderAlignedDiskTimer.updateNanos(cost);
        break;
      case CONSTRUCT_CHUNK_READER_NONALIGNED_MEM:
        constructChunkReaderNonAlignedMemTimer.updateNanos(cost);
        break;
      case CONSTRUCT_CHUNK_READER_NONALIGNED_DISK:
        constructChunkReaderNonAlignedDiskTimer.updateNanos(cost);
        break;
      case BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM:
        buildTsBlockFromPageReaderAlignedMemTimer.updateNanos(cost);
        break;
      case BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK:
        buildTsBlockFromPageReaderAlignedDiskTimer.updateNanos(cost);
        break;
      case BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM:
        buildTsBlockFromPageReaderNonAlignedMemTimer.updateNanos(cost);
        break;
      case BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK:
        buildTsBlockFromPageReaderNonAlignedDiskTimer.updateNanos(cost);
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
      case READ_CHUNK_ALL:
        readChunkAllTimer.updateNanos(cost);
        break;
      case READ_CHUNK_FILE:
        readChunkFileTimer.updateNanos(cost);
        break;
      default:
        break;
    }
  }

  public static SeriesScanCostMetricSet getInstance() {
    return INSTANCE;
  }
}
