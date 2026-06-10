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

package org.apache.iotdb.metrics.core.i18n;

public final class MetricsCoreMessages {

  // --- IoTDBJmxReporter ---
  public static final String JMX_REGISTER_FAILED = "IoTDB 指标：无法注册 ";
  public static final String JMX_UNREGISTER_FAILED = "IoTDB 指标：无法注销：";
  public static final String JMX_REPORTER_ALREADY_START = "IoTDB 指标：JmxReporter 已启动！";
  public static final String JMX_REPORTER_START_FAILED =
      "IoTDB 指标：JmxReporter 启动失败，原因：";
  public static final String JMX_REPORTER_START = "IoTDB 指标：JmxReporter 已启动！";
  public static final String JMX_REPORTER_STOP_FAILED =
      "IoTDB 指标：JmxReporter 停止失败，原因：";
  public static final String JMX_REPORTER_STOP = "IoTDB 指标：JmxReporter 已停止！";

  // --- IoTDBMetricObjNameFactory ---
  public static final String JMX_UNABLE_TO_REGISTER = "IoTDB 指标：无法注册 {} {}";

  private MetricsCoreMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_DETECTED_ERROR_TAKING_SNAPSHOT_MAY_CAUSE_MISS_DURING_RECORDING_2BAE49C4 = "获取快照时检测到错误，可能导致本次记录缺失。";

}
