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
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class QueryResourceMetricSet implements IMetricSet {
  private static final QueryResourceMetricSet INSTANCE = new QueryResourceMetricSet();

  private QueryResourceMetricSet() {
    // empry constructor
  }

  public static final String SEQUENCE_TSFILE = "sequence_tsfile";
  public static final String UNSEQUENCE_TSFILE = "unsequence_tsfile";
  public static final String FLUSHING_MEMTABLE = "flushing_memtable";
  public static final String WORKING_MEMTABLE = "working_memtable";
  public static final String INIT_QUERY_RESOURCE_RETRY_COUNT = "retry_count";
  private Histogram sequenceTsFileHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram unsequenceTsFileHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram flushingMemTableHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram workingMemTableHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram retryCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  public void recordQueryResourceNum(String type, int count) {
    switch (type) {
      case SEQUENCE_TSFILE:
        sequenceTsFileHistogram.update(count);
        break;
      case UNSEQUENCE_TSFILE:
        unsequenceTsFileHistogram.update(count);
        break;
      case FLUSHING_MEMTABLE:
        flushingMemTableHistogram.update(count);
        break;
      case WORKING_MEMTABLE:
        workingMemTableHistogram.update(count);
        break;
      case INIT_QUERY_RESOURCE_RETRY_COUNT:
        if (count > 0) {
          retryCountHistogram.update(count);
        }
        break;
      default:
        break;
    }
  }

  public void recordInitQueryResourceRetryCount(int count) {
    if (count > 0) {
      retryCountHistogram.update(count);
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    sequenceTsFileHistogram =
        metricService.getOrCreateHistogram(
            Metric.QUERY_RESOURCE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            SEQUENCE_TSFILE);
    unsequenceTsFileHistogram =
        metricService.getOrCreateHistogram(
            Metric.QUERY_RESOURCE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            UNSEQUENCE_TSFILE);
    flushingMemTableHistogram =
        metricService.getOrCreateHistogram(
            Metric.QUERY_RESOURCE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            FLUSHING_MEMTABLE);
    workingMemTableHistogram =
        metricService.getOrCreateHistogram(
            Metric.QUERY_RESOURCE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            WORKING_MEMTABLE);
    retryCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.QUERY_RESOURCE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            INIT_QUERY_RESOURCE_RETRY_COUNT);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Arrays.asList(
            SEQUENCE_TSFILE,
            UNSEQUENCE_TSFILE,
            FLUSHING_MEMTABLE,
            WORKING_MEMTABLE,
            INIT_QUERY_RESOURCE_RETRY_COUNT)
        .forEach(
            type ->
                metricService.remove(
                    MetricType.HISTOGRAM,
                    Metric.QUERY_RESOURCE.toString(),
                    Tag.TYPE.toString(),
                    type));
  }

  public static QueryResourceMetricSet getInstance() {
    return INSTANCE;
  }
}
