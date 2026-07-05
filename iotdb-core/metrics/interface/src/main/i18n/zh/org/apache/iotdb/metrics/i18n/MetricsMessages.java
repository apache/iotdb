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

package org.apache.iotdb.metrics.i18n;

public final class MetricsMessages {

  // --- AbstractMetricService ---
  public static final String START_METRIC_SERVICE = "启动指标服务，级别：{}";

  // --- CompositeReporter ---
  public static final String REPORTER_START_FAILED = "启动 {} 上报器失败。";
  public static final String REPORTER_START_NOT_FOUND = "启动 {} 上报器失败，因为未找到该上报器。";
  public static final String REPORTER_STOP_FAILED = "停止 {} 上报器失败。";
  public static final String REPORTER_STOP_NOT_FOUND = "停止 {} 上报器失败，因为未找到该上报器。";

  // --- MetricConfig ---
  public static final String GET_PID_FAILED = "获取 pid 失败，原因：";

  // --- IoTDBSessionReporter ---
  public static final String IOTDB_SESSION_REPORTER_DB_FAILED =
      "IoTDBSessionReporter 检查或创建数据库失败。";
  public static final String IOTDB_SESSION_REPORTER_DB_FAILED_BECAUSE =
      "IoTDBSessionReporter 检查或创建数据库失败，原因：";
  public static final String IOTDB_SESSION_REPORTER_ALREADY_START =
      "IoTDBSessionReporter 已启动！";
  public static final String IOTDB_SESSION_REPORTER_START_FAILED =
      "IoTDBSessionReporter 启动失败，原因：";
  public static final String IOTDB_SESSION_REPORTER_STOP = "IoTDBSessionReporter 已停止！";
  public static final String IOTDB_SESSION_REPORTER_INSERT_FAILED =
      "IoTDBSessionReporter 插入记录失败，原因：";

  // --- PrometheusReporter ---
  public static final String PROMETHEUS_REPORTER_ALREADY_START = "PrometheusReporter 已启动！";
  public static final String PROMETHEUS_REPORTER_START_FAILED =
      "PrometheusReporter 启动失败，原因：";
  public static final String PROMETHEUS_UNEXPECTED_AUTH = "意外的认证字符串：{}";
  public static final String PROMETHEUS_REPORTER_STOP_FAILED =
      "Prometheus 上报器停止失败，原因：";
  public static final String PROMETHEUS_REPORTER_STOP = "PrometheusReporter 已停止！";
  public static final String KEYSTORE_OR_TRUSTSTORE_NULL = "Keystore 或 Truststore 为 null";

  // --- FileStoreUtils ---
  public static final String STORAGE_PATH_FAILED = "获取 {} 的存储路径失败，原因：";

  // --- WindowsNetMetricManager ---
  public static final String GET_INTERFACES_FAILED = "获取网络接口失败，退出码：{}";
  public static final String UPDATE_INTERFACES_ERROR = "更新网络接口时发生错误";
  public static final String GET_STATISTICS_FAILED = "获取统计信息失败，退出码：{}";
  public static final String UPDATE_STATISTICS_ERROR = "更新统计信息时发生错误";
  public static final String GET_CONNECTION_NUM_FAILED = "获取连接数失败，退出码：{}";
  public static final String UPDATE_CONNECTION_NUM_ERROR = "更新连接数时发生错误";

  // --- LinuxNetMetricManager ---
  public static final String CANNOT_FIND_PATH = "找不到 {}";
  public static final String READ_NET_STATUS_ERROR = "读取 {} 时发生异常";
  public static final String READ_NET_STATUS_FOR_NET_ERROR = "读取 {} 以获取网络状态时发生错误";
  public static final String FAILED_TO_GET_SOCKET_NUM = "获取 socket 数量失败";
  public static final String INTERRUPTED_WHILE_WAITING_SOCKET_NUM =
      "等待获取 socket 数量命令时被中断";
  public static final String FAILED_TO_PARSE_SOCKET_NUM =
      "从命令输出解析 socket 数量失败：'{}'";

  // --- LinuxDiskMetricsManager ---
  public static final String FAILED_TO_GET_SECTOR_SIZE = "获取磁盘 {} 的扇区大小失败";
  public static final String CANNOT_FIND_DISK_IO_STATUS_FILE =
      "找不到磁盘 IO 状态文件 {}";
  public static final String ERROR_UPDATING_DISK_IO_INFO = "更新磁盘 IO 信息时发生错误";
  public static final String CANNOT_FIND_PROCESS_IO_STATUS_FILE =
      "找不到进程 IO 状态文件 {}";
  public static final String ERROR_UPDATING_PROCESS_IO_INFO = "更新进程 IO 信息时发生错误";

  // --- WindowsDiskMetricsManager ---
  public static final String UNEXPECTED_WINDOWS_PROCESS_IO_FORMAT =
      "Windows 进程 IO 信息格式异常：{}";
  public static final String UNEXPECTED_WINDOWS_DISK_IO_FORMAT =
      "Windows 磁盘 IO 信息格式异常：{}";
  public static final String FAILED_TO_PARSE_LONG_WINDOWS_DISK =
      "从 Windows 磁盘指标解析 long 值失败：{}";
  public static final String FAILED_TO_PARSE_DOUBLE_WINDOWS_DISK =
      "从 Windows 磁盘指标解析 double 值失败：{}";
  public static final String FAILED_TO_COLLECT_WINDOWS_DISK_METRICS =
      "收集 Windows 磁盘指标失败，PowerShell 退出码：{}，命令 {}，输出 {}";
  public static final String FAILED_TO_EXECUTE_POWERSHELL =
      "执行 PowerShell 获取 Windows 磁盘指标失败";
  public static final String INTERRUPTED_COLLECTING_WINDOWS_DISK =
      "收集 Windows 磁盘指标时被中断";

  private MetricsMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_FAILED_LOAD_ARG_REPORTER_BECAUSE_ALREADY_EXISTED_4BAF2E58 = "无法加载 {} reporter，原因：已存在";
  public static final String EXCEPTION_FAILED_REMOVE_BECAUSE_MISMATCH_TYPE_044E55F6 = " 无法移除，原因：类型不匹配。";
  public static final String LOG_SIZE_METRIC_TAGS_SHOULD_EVEN_BUT_WAS_ODD_TAGS_ARG_201E6A2A = "metric tags 的数量应为偶数，但当前为奇数，tags：{}。";
  public static final String LOG_GC_NOTIFICATIONS_WILL_NOT_AVAILABLE_BECAUSE_MEMORYPOOLMXBEANS_86C3EB6B = "GC notifications 不可用，原因：MemoryPoolMXBeans ";
  public static final String LOG_NOT_PROVIDED_JVM_948976D1 = "未由 JVM 提供";
  public static final String LOG_GC_NOTIFICATIONS_WILL_NOT_AVAILABLE_BECAUSE_22FD13E0 = "GC notifications 不可用，原因：";
  public static final String LOG_COM_SUN_MANAGEMENT_GARBAGECOLLECTIONNOTIFICATIONINFO_NOT_PRESENT_FA43486E = "com.sun.management.GarbageCollectionNotificationInfo 不存在";
  public static final String LOG_FAILED_GET_MEMORY_BECAUSE_041BE661 = "无法获取 memory，原因：";
  public static final String LOG_INTERRUPTED_WAITING_MEMORY_COMMAND_CF538E10 = "等待 memory command 时被中断";
  public static final String LOG_FAILED_REMOVE_LOGBACKMETRICS_BECAUSE_9BE74246 = "无法移除 logBackMetrics，原因：";
  public static final String LOG_ARG_WINDOWS_DISK_METRICS_WILL_SKIPPED_ARG_MS_BEFORE_RETRYING_1F1EB4C4 = "{}。Windows disk metrics 将跳过 {} ms 后重试。";
  public static final String LOG_ARG_ARG_WINDOWS_DISK_METRICS_WILL_SKIPPED_ARG_MS_BEFORE_74269D0A = "{}：{}。Windows disk metrics 将跳过 {} ms 后重试。";
  public static final String LOG_FAILED_WINDOWS_DISK_METRICS_POWERSHELL_COMMAND_ARG_OUTPUT_ARG_16D24C0C = "Windows 磁盘指标 PowerShell 命令失败：{}，输出：{}";
  public static final String LOG_ARG_WINDOWS_DISK_METRICS_COLLECTION_STILL_RETRY_BACKOFF_C237EE87 = "{}。Windows disk metrics 采集仍处于重试退避中。";
  public static final String LOG_RECOVERED_WINDOWS_DISK_METRICS_COLLECTION_THROUGH_POWERSHELL_CIM_03B9110E = "已通过 PowerShell/CIM 恢复 Windows disk metrics 采集。";
  public static final String LOG_IOTDBSESSIONREPORTER_START_WRITE_ARG_ARG_E79CDDAE = "IoTDBSessionReporter 启动，写入 {}:{}";
  public static final String LOG_PROMETHEUSREPORTER_STARTED_USE_PORT_ARG_A688FFC8 = "PrometheusReporter 已启动，使用端口 {}";
  public static final String LOG_DETECTED_ERROR_TAKING_METRIC_TIMER_SNAPSHOT_WILL_DISCARD_METRIC_B7154169 = "获取 metric timer 快照时检测到错误，将丢弃该 metric";

}
