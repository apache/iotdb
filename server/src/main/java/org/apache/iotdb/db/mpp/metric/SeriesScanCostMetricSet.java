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
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.HashMap;
import java.util.Map;

public class SeriesScanCostMetricSet implements IMetricSet {
  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

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

  static {
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String READ_TIMESERIES_METADATA = "read_timeseries_metadata";
  private static final String CACHE = "cache";
  private static final String FILE = "file";
  private static final String NULL = "null";
  public static final String READ_TIMESERIES_METADATA_CACHE =
      READ_TIMESERIES_METADATA + "_" + CACHE;
  public static final String READ_TIMESERIES_METADATA_FILE = READ_TIMESERIES_METADATA + "_" + FILE;

  static {
    metricInfoMap.put(
        READ_TIMESERIES_METADATA_CACHE,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            READ_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            CACHE));
    metricInfoMap.put(
        READ_TIMESERIES_METADATA_FILE,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            READ_TIMESERIES_METADATA,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            FILE));
  }

  private static final String TIMESERIES_METADATA_MODIFICATION = "timeseries_metadata_modification";
  public static final String TIMESERIES_METADATA_MODIFICATION_ALIGNED =
      TIMESERIES_METADATA_MODIFICATION + "_" + ALIGNED;
  public static final String TIMESERIES_METADATA_MODIFICATION_NONALIGNED =
      TIMESERIES_METADATA_MODIFICATION + "_" + NON_ALIGNED;

  static {
    metricInfoMap.put(
        TIMESERIES_METADATA_MODIFICATION_ALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            TIMESERIES_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            NULL));
    metricInfoMap.put(
        TIMESERIES_METADATA_MODIFICATION_NONALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            TIMESERIES_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            NULL));
  }

  private static final String LOAD_CHUNK_METADATA_LIST = "load_chunk_metadata_list";
  public static final String LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM =
      LOAD_CHUNK_METADATA_LIST + "_" + ALIGNED + "_" + MEM;
  public static final String LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK =
      LOAD_CHUNK_METADATA_LIST + "_" + ALIGNED + "_" + DISK;
  public static final String LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM =
      LOAD_CHUNK_METADATA_LIST + "_" + NON_ALIGNED + "_" + MEM;
  public static final String LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK =
      LOAD_CHUNK_METADATA_LIST + "_" + NON_ALIGNED + "_" + DISK;

  static {
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            LOAD_CHUNK_METADATA_LIST,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String CHUNK_METADATA_MODIFICATION = "chunk_metadata_modification";
  public static final String CHUNK_METADATA_MODIFICATION_ALIGNED_MEM =
      CHUNK_METADATA_MODIFICATION + "_" + ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_MODIFICATION_ALIGNED_DISK =
      CHUNK_METADATA_MODIFICATION + "_" + ALIGNED + "_" + DISK;
  public static final String CHUNK_METADATA_MODIFICATION_NONALIGNED_MEM =
      CHUNK_METADATA_MODIFICATION + "_" + NON_ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK =
      CHUNK_METADATA_MODIFICATION + "_" + NON_ALIGNED + "_" + DISK;

  static {
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_MODIFICATION,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String CHUNK_METADATA_FILTER = "chunk_metadata_filter";
  public static final String CHUNK_METADATA_FILTER_ALIGNED_MEM =
      CHUNK_METADATA_FILTER + "_" + ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_FILTER_ALIGNED_DISK =
      CHUNK_METADATA_FILTER + "_" + ALIGNED + "_" + DISK;
  public static final String CHUNK_METADATA_FILTER_NONALIGNED_MEM =
      CHUNK_METADATA_FILTER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String CHUNK_METADATA_FILTER_NONALIGNED_DISK =
      CHUNK_METADATA_FILTER + "_" + NON_ALIGNED + "_" + DISK;

  static {
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CHUNK_METADATA_FILTER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String CONSTRUCT_CHUNK_READER = "construct_chunk_reader";
  public static final String CONSTRUCT_CHUNK_READER_ALIGNED_MEM =
      CONSTRUCT_CHUNK_READER + "_" + ALIGNED + "_" + MEM;
  public static final String CONSTRUCT_CHUNK_READER_ALIGNED_DISK =
      CONSTRUCT_CHUNK_READER + "_" + ALIGNED + "_" + DISK;
  public static final String CONSTRUCT_CHUNK_READER_NONALIGNED_MEM =
      CONSTRUCT_CHUNK_READER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String CONSTRUCT_CHUNK_READER_NONALIGNED_DISK =
      CONSTRUCT_CHUNK_READER + "_" + NON_ALIGNED + "_" + DISK;

  static {
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            CONSTRUCT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String READ_CHUNK = "read_chunk";
  private static final String ALL = "all";
  public static final String READ_CHUNK_ALL = READ_CHUNK + "_" + ALL;
  public static final String READ_CHUNK_FILE = READ_CHUNK + "_" + FILE;

  static {
    metricInfoMap.put(
        READ_CHUNK_ALL,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            READ_CHUNK,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            ALL));
    metricInfoMap.put(
        READ_CHUNK_FILE,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            READ_CHUNK,
            Tag.TYPE.toString(),
            NULL,
            Tag.FROM.toString(),
            FILE));
  }

  private static final String INIT_CHUNK_READER = "init_chunk_reader";
  public static final String INIT_CHUNK_READER_ALIGNED_MEM =
      INIT_CHUNK_READER + "_" + ALIGNED + "_" + MEM;
  public static final String INIT_CHUNK_READER_ALIGNED_DISK =
      INIT_CHUNK_READER + "_" + ALIGNED + "_" + DISK;
  public static final String INIT_CHUNK_READER_NONALIGNED_MEM =
      INIT_CHUNK_READER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String INIT_CHUNK_READER_NONALIGNED_DISK =
      INIT_CHUNK_READER + "_" + NON_ALIGNED + "_" + DISK;

  static {
    metricInfoMap.put(
        INIT_CHUNK_READER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        INIT_CHUNK_READER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        INIT_CHUNK_READER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        INIT_CHUNK_READER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            INIT_CHUNK_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String BUILD_TSBLOCK_FROM_PAGE_READER = "build_tsblock_from_page_reader";
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + ALIGNED + "_" + MEM;
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + ALIGNED + "_" + DISK;
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + NON_ALIGNED + "_" + MEM;
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK =
      BUILD_TSBLOCK_FROM_PAGE_READER + "_" + NON_ALIGNED + "_" + DISK;

  static {
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            ALIGNED,
            Tag.FROM.toString(),
            DISK));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            MEM));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_PAGE_READER,
            Tag.TYPE.toString(),
            NON_ALIGNED,
            Tag.FROM.toString(),
            DISK));
  }

  private static final String BUILD_TSBLOCK_FROM_MERGE_READER = "build_tsblock_from_merge_reader";
  public static final String BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED =
      BUILD_TSBLOCK_FROM_MERGE_READER + "_" + ALIGNED;
  public static final String BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED =
      BUILD_TSBLOCK_FROM_MERGE_READER + "_" + NON_ALIGNED;

  static {
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_MERGE_READER,
            Tag.FROM.toString(),
            NULL,
            Tag.TYPE.toString(),
            ALIGNED));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            Metric.SERIES_SCAN_COST.toString(),
            Tag.STAGE.toString(),
            BUILD_TSBLOCK_FROM_MERGE_READER,
            Tag.FROM.toString(),
            NULL,
            Tag.TYPE.toString(),
            NON_ALIGNED));
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.getOrCreateTimer(
          metricInfo.getName(), MetricLevel.IMPORTANT, metricInfo.getTagsInArray());
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.remove(
          MetricType.TIMER, Metric.SERIES_SCAN_COST.toString(), metricInfo.getTagsInArray());
    }
  }
}
