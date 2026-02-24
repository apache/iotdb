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

package org.apache.iotdb.itbase.env;

import java.util.List;

/** This interface is used to handle properties in iotdb-datanode.properties. */
public interface DataNodeConfig {
  DataNodeConfig setMetricReporterType(List<String> metricReporterTypes);

  DataNodeConfig setMetricPrometheusReporterUsername(String username);

  DataNodeConfig setMetricPrometheusReporterPassword(String password);

  DataNodeConfig setEnableRestService(boolean enableRestService);

  DataNodeConfig setConnectionTimeoutInMS(int connectionTimeoutInMS);

  DataNodeConfig setLoadTsFileAnalyzeSchemaMemorySizeInBytes(
      long loadTsFileAnalyzeSchemaMemorySizeInBytes);

  DataNodeConfig setCompactionScheduleInterval(long compactionScheduleInterval);

  DataNodeConfig setEnableMQTTService(boolean enableMQTTService);

  DataNodeConfig setMqttPayloadFormatter(String mqttPayloadFormatter);

  DataNodeConfig setLoadLastCacheStrategy(String strategyName);

  DataNodeConfig setCacheLastValuesForLoad(boolean cacheLastValuesForLoad);

  DataNodeConfig setWalThrottleSize(long walThrottleSize);

  DataNodeConfig setDeleteWalFilesPeriodInMs(long deleteWalFilesPeriodInMs);

  DataNodeConfig setDataNodeMemoryProportion(String dataNodeMemoryProportion);

  DataNodeConfig setQueryCostStatWindow(int queryCostStatWindow);
}
