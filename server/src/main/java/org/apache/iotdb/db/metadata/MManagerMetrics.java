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

package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class MManagerMetrics implements IMetricSet {
  private MManager mManager;

  public MManagerMetrics(MManager mManager) {
    this.mManager = mManager;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            MManager::getNormalSeriesNumber,
            Tag.NAME.toString(),
            "timeSeries",
            Tag.TYPE.toString(),
            "normal");

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            MManager::getTemplateSeriesNumber,
            Tag.NAME.toString(),
            "timeSeries",
            Tag.TYPE.toString(),
            "template");

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            MManager::getDeviceNumber,
            Tag.NAME.toString(),
            "device",
            Tag.TYPE.toString(),
            "total");

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            MManager::getStorageGroupNumber,
            Tag.NAME.toString(),
            "storageGroup",
            Tag.TYPE.toString(),
            "total");

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            MManager::getTotalTemplateActivatedNumber,
            Tag.NAME.toString(),
            "device using template");

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.MEM.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            MManager::getTotalEstimatedMemoryUsage,
            Tag.NAME.toString(),
            "schema memory usage");

    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.MEM.toString(),
            MetricLevel.IMPORTANT,
            mManager,
            mManager ->
                IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchema()
                    - mManager.getTotalEstimatedMemoryUsage(),
            Tag.NAME.toString(),
            "schema memory remaining");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.QUANTITY.toString(),
            Tag.NAME.toString(),
            "timeSeries",
            Tag.TYPE.toString(),
            "normal");

    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.QUANTITY.toString(),
            Tag.NAME.toString(),
            "timeSeries",
            Tag.TYPE.toString(),
            "template");

    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.QUANTITY.toString(),
            Tag.NAME.toString(),
            "device",
            Tag.TYPE.toString(),
            "total");

    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.QUANTITY.toString(),
            Tag.NAME.toString(),
            "storageGroup",
            Tag.TYPE.toString(),
            "total");

    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.QUANTITY.toString(),
            Tag.NAME.toString(),
            "device using template");

    MetricService.getInstance()
        .remove(
            MetricType.GAUGE, Metric.MEM.toString(), Tag.NAME.toString(), "schema memory usage");

    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.MEM.toString(),
            Tag.NAME.toString(),
            "schema memory remaining");
  }
}
