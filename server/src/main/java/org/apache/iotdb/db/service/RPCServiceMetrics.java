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
package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.AbstractThriftServiceThread;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

public class RPCServiceMetrics implements IMetricSet {
  private AbstractThriftServiceThread thriftServiceThread;

  public RPCServiceMetrics(AbstractThriftServiceThread thriftServiceThread) {
    this.thriftServiceThread = thriftServiceThread;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.THRIFT_ACTIVE_THREADS.toString(),
        MetricLevel.CORE,
        thriftServiceThread,
        AbstractThriftServiceThread::getActiveThreadCount,
        Tag.NAME.toString(),
        ThreadName.CLIENT_RPC_SERVICE.getName());
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_ACTIVE_THREADS.toString(),
        Tag.NAME.toString(),
        ThreadName.CLIENT_RPC_SERVICE.getName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RPCServiceMetrics that = (RPCServiceMetrics) o;
    return Objects.equals(thriftServiceThread, that.thriftServiceThread);
  }

  @Override
  public int hashCode() {
    return Objects.hash(thriftServiceThread);
  }
}
