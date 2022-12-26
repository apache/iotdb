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

  private static final String metric = Metric.SERIES_SCAN_COST.toString();

  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  public static final String LOAD_TIMESERIES_METADATA_ALIGNED_MEM =
      "load_timeseries_metadata_aligned_mem";
  public static final String LOAD_TIMESERIES_METADATA_ALIGNED_DISK =
      "load_timeseries_metadata_aligned_disk";
  public static final String LOAD_TIMESERIES_METADATA_NONALIGNED_MEM =
      "load_timeseries_metadata_nonaligned_mem";
  public static final String LOAD_TIMESERIES_METADATA_NONALIGNED_DISK =
      "load_timeseries_metadata_nonaligned_disk";

  static {
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_timeseries_metadata",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_timeseries_metadata",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_timeseries_metadata",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        LOAD_TIMESERIES_METADATA_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_timeseries_metadata",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String READ_TIMESERIES_METADATA_CACHE = "read_timeseries_metadata_cache";
  public static final String READ_TIMESERIES_METADATA_FILE = "read_timeseries_metadata_file";

  static {
    metricInfoMap.put(
        READ_TIMESERIES_METADATA_CACHE,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "read_timeseries_metadata",
            Tag.TYPE.toString(),
            "null",
            Tag.FROM.toString(),
            "cache"));
    metricInfoMap.put(
        READ_TIMESERIES_METADATA_FILE,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "read_timeseries_metadata",
            Tag.TYPE.toString(),
            "null",
            Tag.FROM.toString(),
            "file"));
  }

  public static final String TIMESERIES_METADATA_MODIFICATION_ALIGNED =
      "timeseries_metadata_modification_aligned";
  public static final String TIMESERIES_METADATA_MODIFICATION_NONALIGNED =
      "timeseries_metadata_modification_nonaligned";

  static {
    metricInfoMap.put(
        TIMESERIES_METADATA_MODIFICATION_ALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "timeseries_metadata_modification",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "null"));
    metricInfoMap.put(
        TIMESERIES_METADATA_MODIFICATION_NONALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "timeseries_metadata_modification",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "null"));
  }

  public static final String LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM =
      "load_chunk_metadata_list_aligned_mem";
  public static final String LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK =
      "load_chunk_metadata_list_aligned_disk";
  public static final String LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM =
      "load_chunk_metadata_list_nonaligned_mem";
  public static final String LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK =
      "load_chunk_metadata_list_nonaligned_disk";

  static {
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_chunk_metadata_list",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_chunk_metadata_list",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_chunk_metadata_list",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        LOAD_CHUNK_METADATA_LIST_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "load_chunk_metadata_list",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String CHUNK_METADATA_MODIFICATION_ALIGNED_MEM =
      "chunk_metadata_modification_aligned_mem";
  public static final String CHUNK_METADATA_MODIFICATION_ALIGNED_DISK =
      "chunk_metadata_modification_aligned_disk";
  public static final String CHUNK_METADATA_MODIFICATION_NONALIGNED_MEM =
      "chunk_metadata_modification_nonaligned_mem";
  public static final String CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK =
      "chunk_metadata_modification_nonaligned_disk";

  static {
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_modification",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_modification",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_modification",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        CHUNK_METADATA_MODIFICATION_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_modification",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String CHUNK_METADATA_FILTER_ALIGNED_MEM =
      "chunk_metadata_filter_aligned_mem";
  public static final String CHUNK_METADATA_FILTER_ALIGNED_DISK =
      "chunk_metadata_filter_aligned_disk";
  public static final String CHUNK_METADATA_FILTER_NONALIGNED_MEM =
      "chunk_metadata_filter_nonaligned_mem";
  public static final String CHUNK_METADATA_FILTER_NONALIGNED_DISK =
      "chunk_metadata_filter_nonaligned_disk";

  static {
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_filter",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_filter",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_filter",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        CHUNK_METADATA_FILTER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "chunk_metadata_filter",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String CONSTRUCT_CHUNK_READER_ALIGNED_MEM =
      "construct_chunk_reader_aligned_mem";
  public static final String CONSTRUCT_CHUNK_READER_ALIGNED_DISK =
      "construct_chunk_reader_aligned_disk";
  public static final String CONSTRUCT_CHUNK_READER_NONALIGNED_MEM =
      "construct_chunk_reader_nonaligned_mem";
  public static final String CONSTRUCT_CHUNK_READER_NONALIGNED_DISK =
      "construct_chunk_reader_nonaligned_disk";

  static {
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "construct_chunk_reader",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "construct_chunk_reader",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "construct_chunk_reader",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        CONSTRUCT_CHUNK_READER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "construct_chunk_reader",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String READ_CHUNK_ALL = "read_chunk_all";
  public static final String READ_CHUNK_FILE = "read_chunk_file";

  static {
    metricInfoMap.put(
        READ_CHUNK_ALL,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "read_chunk",
            Tag.TYPE.toString(),
            "null",
            Tag.FROM.toString(),
            "all"));
    metricInfoMap.put(
        READ_CHUNK_FILE,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "read_chunk",
            Tag.TYPE.toString(),
            "null",
            Tag.FROM.toString(),
            "file"));
  }

  public static final String INIT_CHUNK_READER_ALIGNED_MEM = "init_chunk_reader_aligned_mem";
  public static final String INIT_CHUNK_READER_ALIGNED_DISK = "init_chunk_reader_aligned_disk";
  public static final String INIT_CHUNK_READER_NONALIGNED_MEM = "init_chunk_reader_nonaligned_mem";
  public static final String INIT_CHUNK_READER_NONALIGNED_DISK =
      "init_chunk_reader_nonaligned_disk";

  static {
    metricInfoMap.put(
        INIT_CHUNK_READER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "init_chunk_reader",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        INIT_CHUNK_READER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "init_chunk_reader",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        INIT_CHUNK_READER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "init_chunk_reader",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        INIT_CHUNK_READER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "init_chunk_reader",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM =
      "build_tsblock_from_page_reader_aligned_mem";
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK =
      "build_tsblock_from_page_reader_aligned_disk";
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM =
      "build_tsblock_from_page_reader_nonaligned_mem";
  public static final String BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK =
      "build_tsblock_from_page_reader_nonaligned_disk";

  static {
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "build_tsblock_from_page_reader",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "build_tsblock_from_page_reader",
            Tag.TYPE.toString(),
            "aligned",
            Tag.FROM.toString(),
            "disk"));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "build_tsblock_from_page_reader",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "mem"));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "build_tsblock_from_page_reader",
            Tag.TYPE.toString(),
            "non_aligned",
            Tag.FROM.toString(),
            "disk"));
  }

  public static final String BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED =
      "build_tsblock_from_merge_reader_aligned";
  public static final String BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED =
      "build_tsblock_from_merge_reader_nonaligned";

  static {
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "build_tsblock_from_merge_reader",
            Tag.FROM.toString(),
            "null",
            Tag.TYPE.toString(),
            "aligned"));
    metricInfoMap.put(
        BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.STAGE.toString(),
            "build_tsblock_from_merge_reader",
            Tag.FROM.toString(),
            "null",
            Tag.TYPE.toString(),
            "non_aligned"));
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
      metricService.remove(MetricType.TIMER, metric, metricInfo.getTagsInArray());
    }
  }
}
