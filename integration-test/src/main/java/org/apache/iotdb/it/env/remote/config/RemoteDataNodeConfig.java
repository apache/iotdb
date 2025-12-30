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
package org.apache.iotdb.it.env.remote.config;

import org.apache.iotdb.itbase.env.DataNodeConfig;

import java.util.List;

public class RemoteDataNodeConfig implements DataNodeConfig {
  @Override
  public DataNodeConfig setMetricReporterType(List<String> metricReporterTypes) {
    return this;
  }

  @Override
  public DataNodeConfig setMetricPrometheusReporterUsername(String username) {
    return this;
  }

  @Override
  public DataNodeConfig setMetricPrometheusReporterPassword(String password) {
    return this;
  }

  @Override
  public DataNodeConfig setEnableRestService(boolean enableRestService) {
    return this;
  }

  @Override
  public DataNodeConfig setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    return this;
  }

  @Override
  public DataNodeConfig setLoadTsFileAnalyzeSchemaMemorySizeInBytes(
      long loadTsFileAnalyzeSchemaMemorySizeInBytes) {
    return this;
  }

  @Override
  public DataNodeConfig setCompactionScheduleInterval(long compactionScheduleInterval) {
    return this;
  }

  @Override
  public DataNodeConfig setEnableMQTTService(boolean enableMQTTService) {
    return this;
  }

  @Override
  public DataNodeConfig setMqttPayloadFormatter(String mqttPayloadFormatter) {
    return this;
  }

  @Override
  public DataNodeConfig setLoadLastCacheStrategy(String strategyName) {
    return this;
  }

  @Override
  public DataNodeConfig setCacheLastValuesForLoad(boolean cacheLastValuesForLoad) {
    return this;
  }

  @Override
  public DataNodeConfig setWalThrottleSize(long walThrottleSize) {
    return this;
  }

  @Override
  public DataNodeConfig setDeleteWalFilesPeriodInMs(long deleteWalFilesPeriodInMs) {
    return this;
  }

  @Override
  public DataNodeConfig setDataNodeMemoryProportion(String dataNodeMemoryProportion) {
    return this;
  }

  @Override
  public DataNodeConfig setQueryCostStatWindow(int queryCostStatWindow) {
    return this;
  }
}
