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
package org.apache.iotdb.db.metadata.rescon;

import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.atomic.AtomicLong;

public class SchemaStatisticsManager {

  private final AtomicLong totalSeriesNumber = new AtomicLong();

  private static class SchemaStatisticsHolder {

    private SchemaStatisticsHolder() {
      // allowed to do nothing
    }

    private static final SchemaStatisticsManager INSTANCE = new SchemaStatisticsManager();
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static SchemaStatisticsManager getInstance() {
    return SchemaStatisticsHolder.INSTANCE;
  }

  public void init() {
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            totalSeriesNumber,
            AtomicLong::get,
            Tag.NAME.toString(),
            "timeSeries");
  }

  public long getTotalSeriesNumber() {
    return totalSeriesNumber.get();
  }

  public void addTimeseries(int addedNum) {
    totalSeriesNumber.addAndGet(addedNum);
  }

  public void deleteTimeseries(int deletedNum) {
    totalSeriesNumber.addAndGet(-deletedNum);
  }

  public void clear() {
    this.totalSeriesNumber.getAndSet(0);
  }
}
