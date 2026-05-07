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

package org.apache.iotdb.commons.i18n;

public final class ServiceMessages {

  // ---- AbstractPeriodicalServiceWithAdvance ----
  public static final String INTERRUPTED_WAITING_DEVICE_VIEW_UPDATE =
      "等待下一次设备视图更新时被中断：{}";
  public static final String SERVICE_STARTED_SUCCESSFULLY = "{} 已成功启动。";
  public static final String SERVICE_STOPPED_SUCCESSFULLY = "{} 已成功停止。";

  // ---- AbstractThriftServiceThread ----
  public static final String STOP_COUNT_DOWN_LATCH_IS_NULL = "停止倒计时门闩为空";
  public static final String STOP_COUNT_DOWN_LATCH_IS = "停止倒计时门闩为 {}";
  public static final String CLOSE_THREAD_POOL_SERVER_AND_SERVER_SOCKET =
      "{}: 关闭 {} 的 TThreadPoolServer 和 TServerSocket";
  public static final String FAILED_TO_START_BECAUSE = "启动失败，原因：";
  public static final String FAILED_TO_START_SERVICE_BECAUSE =
      "%s: 启动 %s 失败，原因：";
  public static final String SERVICE_THREAD_BEGIN_TO_RUN =
      "{} 服务线程开始运行...";
  public static final String SERVICE_EXIT_BECAUSE = "%s: %s 退出，原因：";
  public static final String UNEXPECTED_SERVER_TYPE = "未预期的服务器类型 {}";
  public static final String FAILED_TO_LOAD_KEYSTORE_OR_TRUSTSTORE =
      "加载密钥库或信任库文件失败";
  public static final String KEYSTORE_OR_TRUSTSTORE_NOT_FOUND =
      "未找到密钥库或信任库文件";

  // ---- JMXService ----
  public static final String FAILED_TO_REGISTER_MBEAN = "注册 MBean {} 失败";
  public static final String FAILED_TO_UNREGISTER_MBEAN = "注销 MBean {} 失败";
  public static final String JMX_PORT_IS_UNDEFINED = "{} JMX 端口未定义";

  // ---- RegisterManager ----
  public static final String SERVICE_ALREADY_REGISTERED =
      "{} 已注册，跳过";
  public static final String SERVICE_STARTED_WITH_TIME =
      "{} 服务已成功启动，耗时 {} 毫秒。";
  public static final String SERVICE_DEREGISTERED = "{} 已注销";
  public static final String FAILED_TO_STOP_SERVICE_BECAUSE = "停止 {} 失败，原因：";
  public static final String DEREGISTER_ALL_SERVICE = "已注销所有服务。";

  // ---- StartupChecks ----
  public static final String START_JMX_LOCALLY = "本地启动 JMX。";
  public static final String JMX_PORT_MISSING_FROM_ENV =
      "{} 在 {}.sh 中缺失（Unix 或 OS X，如果使用 Windows，请检查 conf/{}.bat）";
  public static final String START_JMX_REMOTELY =
      "远程启动 JMX：JMX 已开启，监听远程连接端口 {}";
  public static final String JDK_VERSION_TOO_LOW =
      "要求 JDK 版本 >= %d，当前版本为 %d。";
  public static final String JDK_VERSION_IS = "JDK 版本为 {}。";
  public static final String JVM_VERSION_IS = "JVM 版本为 {} {}。";
  public static final String GRAALVM_NOT_RECOMMENDED =
      "您可能正在使用 GraalVM，强烈不建议使用。GraalVM 可能会在系统运行一段时间后引发异常问题，请检查您的 JVM 版本。";

  // ---- ThriftService ----
  public static final String START_LATCH_NULL_WHEN_GETTING_STATUS =
      "获取状态时启动门闩为空";
  public static final String START_STATUS_WHEN_GETTING_STATUS =
      "获取状态时启动状态为 {}";
  public static final String STOP_LATCH_NULL_WHEN_GETTING_STATUS =
      "获取状态时停止门闩为空";
  public static final String STOP_LATCH_WHEN_GETTING_STATUS =
      "获取状态时停止门闩为 {}";
  public static final String SERVICE_ALREADY_RUNNING =
      "{}: {} 已在运行中";
  public static final String START_SERVICE = "{}: 启动 {}...";
  public static final String START_SERVICE_SUCCESSFULLY =
      "{}: {} 启动成功，监听 IP {} 端口 {}";
  public static final String SERVICE_NOT_RUNNING = "{}: {} 未在运行";
  public static final String CLOSING_SERVICE = "{}: 正在关闭 {}...";
  public static final String CLOSE_SERVICE_SUCCESSFULLY = "{}: 关闭 {} 成功";
  public static final String CLOSE_SERVICE_FAILED = "{}: 关闭 {} 失败，原因：";

  // ---- GcTimeAlerter ----
  public static final String GC_ALERT_METRICS_TAKEN_TIME = "异常指标采集时间：{}";
  public static final String GC_ALERT_GC_TIME_PERCENTAGE = "GC 时间占比：{}%";
  public static final String GC_ALERT_ACCUMULATED_GC_TIME =
      "当前观察窗口内累计 GC 时间：{} 毫秒";
  public static final String GC_ALERT_OBSERVATION_WINDOW_FROM_TO =
      "观察窗口从：{} 到：{}";
  public static final String GC_ALERT_OBSERVATION_WINDOW_TIME =
      "观察窗口时长：{} 毫秒。";

  // ---- JvmGcMonitorMetrics ----
  public static final String JVM_GC_MONITOR_STOPPED =
      "JVM GC 定时监控已成功停止。";

  // ---- MetricService ----
  public static final String LOAD_METRIC_REPORTERS = "加载指标上报器，类型：{}";
  public static final String FAILED_TO_LOAD_REPORTER =
      "加载类型为 {} 的上报器失败";
  public static final String METRIC_SERVICE_START_TO_INIT = "指标服务开始初始化。";
  public static final String METRIC_SERVICE_START_SUCCESSFULLY =
      "指标服务启动成功。";
  public static final String METRIC_SERVICE_FAILED_TO_START =
      "指标服务启动 {} 失败，原因：";
  public static final String METRIC_SERVICE_TRY_TO_RESTART =
      "指标服务尝试重启。";
  public static final String METRIC_SERVICE_REBIND_METRIC_SET =
      "指标服务重新绑定指标集：{}";
  public static final String METRIC_SERVICE_RESTART_SUCCESSFULLY =
      "指标服务重启成功。";
  public static final String METRIC_SERVICE_TRY_TO_STOP = "指标服务尝试停止。";
  public static final String METRIC_SERVICE_STOP_SUCCESSFULLY =
      "指标服务停止成功。";
  public static final String METRIC_SERVICE_RELOAD_INTERNAL_REPORTER =
      "指标服务重新加载内部上报器。";
  public static final String METRIC_SERVICE_RELOAD_INTERNAL_REPORTER_SUCCESSFULLY =
      "指标服务重新加载内部上报器成功。";
  public static final String METRIC_SERVICE_RESTART_REPORTERS_SUCCESSFULLY =
      "指标服务重启上报器成功。";
  public static final String NOTHING_CHANGED_IN_METRIC_CONFIG =
      "指标配置未发生变更。";
  public static final String INTERNAL_REPORTER_FAILED_TO_START =
      "内部上报器启动失败！";

  // ---- CpuUsageMetrics ----
  public static final String CPU_USAGE_UPDATE_TIME = "CPU 使用率更新耗时 {} 纳秒";

  private ServiceMessages() {}
}
