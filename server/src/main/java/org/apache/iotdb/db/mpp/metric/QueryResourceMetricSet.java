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
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.List;

public class QueryResourceMetricSet implements IMetricSet {

  public static final String SEQUENCE_TSFILE = "sequence_tsfile";
  public static final String UNSEQUENCE_TSFILE = "unsequence_tsfile";
  public static final String FLUSHING_MEMTABLE = "flushing_memtable";
  public static final String WORKING_MEMTABLE = "working_memtable";

  private static final String metric = Metric.QUERY_RESOURCE.toString();
  private final String tagKey = Tag.TYPE.toString();

  private static final List<String> resourceTypes =
      Arrays.asList(SEQUENCE_TSFILE, UNSEQUENCE_TSFILE, FLUSHING_MEMTABLE, WORKING_MEMTABLE);

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (String type : resourceTypes) {
      metricService.getOrCreateHistogram(metric, MetricLevel.IMPORTANT, tagKey, type);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String type : resourceTypes) {
      metricService.remove(MetricType.HISTOGRAM, metric, tagKey, type);
    }
  }
}
