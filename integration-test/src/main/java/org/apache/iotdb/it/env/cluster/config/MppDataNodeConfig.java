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

package org.apache.iotdb.it.env.cluster.config;

import org.apache.iotdb.itbase.env.DataNodeConfig;

import java.io.IOException;
import java.util.List;

public class MppDataNodeConfig extends MppBaseConfig implements DataNodeConfig {

  public MppDataNodeConfig() {
    super();
  }

  public MppDataNodeConfig(String filePath) throws IOException {
    super(filePath);
  }

  @Override
  public MppBaseConfig emptyClone() {
    return new MppDataNodeConfig();
  }

  @Override
  public void updateProperties(MppBaseConfig persistentConfig) {
    if (persistentConfig instanceof MppDataNodeConfig) {
      super.updateProperties(persistentConfig);
    } else {
      throw new UnsupportedOperationException(
          "MppDataNodeConfig can't be override by an instance of "
              + persistentConfig.getClass().getCanonicalName());
    }
  }

  @Override
  public DataNodeConfig setMetricReporterType(List<String> metricReporterTypes) {
    properties.setProperty("dn_metric_reporter_list", String.join(",", metricReporterTypes));
    return this;
  }

  @Override
  public DataNodeConfig setMetricPrometheusReporterUsername(String username) {
    properties.setProperty("metric_prometheus_reporter_username", username);
    return this;
  }

  @Override
  public DataNodeConfig setMetricPrometheusReporterPassword(String password) {
    properties.setProperty("metric_prometheus_reporter_password", password);
    return this;
  }

  @Override
  public DataNodeConfig setEnableRestService(boolean enableRestService) {
    properties.setProperty("enable_rest_service", String.valueOf(enableRestService));
    return this;
  }

  @Override
  public DataNodeConfig setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    properties.setProperty("dn_connection_timeout_ms", String.valueOf(connectionTimeoutInMS));
    return this;
  }

  @Override
  public DataNodeConfig setLoadTsFileAnalyzeSchemaMemorySizeInBytes(
      long loadTsFileAnalyzeSchemaMemorySizeInBytes) {
    properties.setProperty(
        "load_tsfile_analyze_schema_memory_size_in_bytes",
        String.valueOf(loadTsFileAnalyzeSchemaMemorySizeInBytes));
    return this;
  }

  @Override
  public DataNodeConfig setCompactionScheduleInterval(long compactionScheduleInterval) {
    properties.setProperty(
        "compaction_schedule_interval_in_ms", String.valueOf(compactionScheduleInterval));
    return this;
  }

  @Override
  public DataNodeConfig setEnableMQTTService(boolean enableMQTTService) {
    setProperty("enable_mqtt_service", String.valueOf(enableMQTTService));
    return this;
  }

  @Override
  public DataNodeConfig setMqttPayloadFormatter(String mqttPayloadFormatter) {
    setProperty("mqtt_payload_formatter", String.valueOf(mqttPayloadFormatter));
    return this;
  }

  @Override
  public DataNodeConfig setLoadLastCacheStrategy(String strategyName) {
    setProperty("last_cache_operation_on_load", strategyName);
    return this;
  }

  @Override
  public DataNodeConfig setCacheLastValuesForLoad(boolean cacheLastValuesForLoad) {
    setProperty("cache_last_values_for_load", String.valueOf(cacheLastValuesForLoad));
    return this;
  }

  @Override
  public DataNodeConfig setWalThrottleSize(long walThrottleSize) {
    setProperty("wal_throttle_threshold_in_byte", String.valueOf(walThrottleSize));
    return this;
  }

  @Override
  public DataNodeConfig setDeleteWalFilesPeriodInMs(long deleteWalFilesPeriodInMs) {
    setProperty("delete_wal_files_period_in_ms", String.valueOf(deleteWalFilesPeriodInMs));
    return this;
  }

  @Override
  public DataNodeConfig setDataNodeMemoryProportion(String dataNodeMemoryProportion) {
    setProperty("datanode_memory_proportion", dataNodeMemoryProportion);
    return this;
  }

  @Override
  public DataNodeConfig setQueryCostStatWindow(int queryCostStatWindow) {
    setProperty("query_cost_stat_window", String.valueOf(queryCostStatWindow));
    return this;
  }
}
