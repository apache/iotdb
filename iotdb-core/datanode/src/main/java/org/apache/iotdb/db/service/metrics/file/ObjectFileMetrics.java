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

package org.apache.iotdb.db.service.metrics.file;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ObjectFileMetrics implements IMetricSet {
  private static final String OBJECT = "object";
  private final AtomicInteger objFileNum = new AtomicInteger(0);
  private final AtomicLong objFileSize = new AtomicLong(0);

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        ObjectFileMetrics::getObjectFileSize,
        Tag.NAME.toString(),
        OBJECT);
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        ObjectFileMetrics::getObjectFileNum,
        Tag.NAME.toString(),
        OBJECT);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), OBJECT);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), OBJECT);
  }

  public int getObjectFileNum() {
    return objFileNum.get();
  }

  public long getObjectFileSize() {
    return objFileSize.get();
  }

  public void increaseObjectFileNum(int num) {
    objFileNum.addAndGet(num);
  }

  public void decreaseObjectFileNum(int num) {
    objFileNum.addAndGet(-num);
  }

  public void increaseObjectFileSize(long size) {
    objFileSize.addAndGet(size);
  }

  public void decreaseObjectFileSize(long size) {
    objFileSize.addAndGet(-size);
  }
}
