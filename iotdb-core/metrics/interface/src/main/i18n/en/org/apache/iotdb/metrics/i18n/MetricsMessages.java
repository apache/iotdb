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
  public static final String START_METRIC_SERVICE = "Start metric service at level: {}";

  // --- CompositeReporter ---
  public static final String REPORTER_START_FAILED = "Failed to start {} reporter.";
  public static final String REPORTER_START_NOT_FOUND = "Failed to start {} reporter because not find.";
  public static final String REPORTER_STOP_FAILED = "Failed to stop {} reporter.";
  public static final String REPORTER_STOP_NOT_FOUND = "Failed to stop {} reporter because not find.";

  // --- MetricConfig ---
  public static final String GET_PID_FAILED = "Failed to get pid, because ";

  // --- IoTDBSessionReporter ---
  public static final String IOTDB_SESSION_REPORTER_DB_FAILED =
      "IoTDBSessionReporter checkOrCreateDatabase failed.";
  public static final String IOTDB_SESSION_REPORTER_DB_FAILED_BECAUSE =
      "IoTDBSessionReporter checkOrCreateDatabase failed because ";
  public static final String IOTDB_SESSION_REPORTER_ALREADY_START =
      "IoTDBSessionReporter already start!";
  public static final String IOTDB_SESSION_REPORTER_START_FAILED =
      "IoTDBSessionReporter failed to start, because";
  public static final String IOTDB_SESSION_REPORTER_STOP = "IoTDBSessionReporter stop!";
  public static final String IOTDB_SESSION_REPORTER_INSERT_FAILED =
      "IoTDBSessionReporter failed to insert record, because ";

  // --- PrometheusReporter ---
  public static final String PROMETHEUS_REPORTER_ALREADY_START = "PrometheusReporter already start!";
  public static final String PROMETHEUS_REPORTER_START_FAILED =
      "PrometheusReporter failed to start, because ";
  public static final String PROMETHEUS_UNEXPECTED_AUTH = "Unexpected auth string: {}";
  public static final String PROMETHEUS_REPORTER_STOP_FAILED =
      "Prometheus Reporter failed to stop, because ";
  public static final String PROMETHEUS_REPORTER_STOP = "PrometheusReporter stop!";
  public static final String KEYSTORE_OR_TRUSTSTORE_NULL = "Keystore or Truststore is null";

  // --- FileStoreUtils ---
  public static final String STORAGE_PATH_FAILED = "Failed to get storage path of {}, because";

  // --- WindowsNetMetricManager ---
  public static final String GET_INTERFACES_FAILED = "Failed to get interfaces, exit code: {}";
  public static final String UPDATE_INTERFACES_ERROR = "Error updating interfaces";
  public static final String GET_STATISTICS_FAILED = "Failed to get statistics, exit code: {}";
  public static final String UPDATE_STATISTICS_ERROR = "Error updating statistics";
  public static final String GET_CONNECTION_NUM_FAILED =
      "Failed to get connection num, exit code: {}";
  public static final String UPDATE_CONNECTION_NUM_ERROR = "Error updating connection num";

  // --- LinuxNetMetricManager ---
  public static final String CANNOT_FIND_PATH = "Cannot find {}";
  public static final String READ_NET_STATUS_ERROR = "Meets exception when reading {}";
  public static final String READ_NET_STATUS_FOR_NET_ERROR =
      "Meets error when reading {} for net status";
  public static final String FAILED_TO_GET_SOCKET_NUM = "Failed to get socket num";
  public static final String INTERRUPTED_WHILE_WAITING_SOCKET_NUM =
      "Interrupted while waiting for socket num command";
  public static final String FAILED_TO_PARSE_SOCKET_NUM =
      "Failed to parse socket num from command output: '{}'";

  // --- LinuxDiskMetricsManager ---
  public static final String FAILED_TO_GET_SECTOR_SIZE = "Failed to get the sector size of {}";
  public static final String CANNOT_FIND_DISK_IO_STATUS_FILE =
      "Cannot find disk io status file {}";
  public static final String ERROR_UPDATING_DISK_IO_INFO =
      "Meets error while updating disk io info";
  public static final String CANNOT_FIND_PROCESS_IO_STATUS_FILE =
      "Cannot find process io status file {}";
  public static final String ERROR_UPDATING_PROCESS_IO_INFO =
      "Meets error while updating process io info";

  // --- WindowsDiskMetricsManager ---
  public static final String UNEXPECTED_WINDOWS_PROCESS_IO_FORMAT =
      "Unexpected windows process io info format: {}";
  public static final String UNEXPECTED_WINDOWS_DISK_IO_FORMAT =
      "Unexpected windows disk io info format: {}";
  public static final String FAILED_TO_PARSE_LONG_WINDOWS_DISK =
      "Failed to parse long value from windows disk metrics: {}";
  public static final String FAILED_TO_PARSE_DOUBLE_WINDOWS_DISK =
      "Failed to parse double value from windows disk metrics: {}";
  public static final String FAILED_TO_COLLECT_WINDOWS_DISK_METRICS =
      "Failed to collect windows disk metrics, powershell exit code: {}, command {}, output {}";
  public static final String FAILED_TO_EXECUTE_POWERSHELL =
      "Failed to execute powershell for windows disk metrics";
  public static final String INTERRUPTED_COLLECTING_WINDOWS_DISK =
      "Interrupted while collecting windows disk metrics";

  private MetricsMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_FAILED_LOAD_ARG_REPORTER_BECAUSE_ALREADY_EXISTED_4BAF2E58 = "Failed to load {} reporter because already existed";
  public static final String EXCEPTION_FAILED_REMOVE_BECAUSE_MISMATCH_TYPE_044E55F6 = " failed to remove because the mismatch of type. ";
  public static final String LOG_SIZE_METRIC_TAGS_SHOULD_EVEN_BUT_WAS_ODD_TAGS_ARG_201E6A2A = "The size of metric tags should be even, but was odd, tags: {}.";
  public static final String LOG_GC_NOTIFICATIONS_WILL_NOT_AVAILABLE_BECAUSE_MEMORYPOOLMXBEANS_86C3EB6B = "GC notifications will not be available because MemoryPoolMXBeans ";
  public static final String LOG_NOT_PROVIDED_JVM_948976D1 = "are not provided by the JVM";
  public static final String LOG_GC_NOTIFICATIONS_WILL_NOT_AVAILABLE_BECAUSE_22FD13E0 = "GC notifications will not be available because ";
  public static final String LOG_COM_SUN_MANAGEMENT_GARBAGECOLLECTIONNOTIFICATIONINFO_NOT_PRESENT_FA43486E = "com.sun.management.GarbageCollectionNotificationInfo is not present";
  public static final String LOG_FAILED_GET_MEMORY_BECAUSE_041BE661 = "Failed to get memory, because ";
  public static final String LOG_INTERRUPTED_WAITING_MEMORY_COMMAND_CF538E10 = "Interrupted while waiting for memory command";
  public static final String LOG_FAILED_REMOVE_LOGBACKMETRICS_BECAUSE_9BE74246 = "Failed to remove logBackMetrics, because ";
  public static final String LOG_ARG_WINDOWS_DISK_METRICS_WILL_SKIPPED_ARG_MS_BEFORE_RETRYING_1F1EB4C4 = "{}. Windows disk metrics will be skipped for {} ms before retrying.";
  public static final String LOG_ARG_ARG_WINDOWS_DISK_METRICS_WILL_SKIPPED_ARG_MS_BEFORE_74269D0A = "{}: {}. Windows disk metrics will be skipped for {} ms before retrying.";
  public static final String LOG_FAILED_WINDOWS_DISK_METRICS_POWERSHELL_COMMAND_ARG_OUTPUT_ARG_16D24C0C = "Failed windows disk metrics powershell command: {}, output: {}";
  public static final String LOG_ARG_WINDOWS_DISK_METRICS_COLLECTION_STILL_RETRY_BACKOFF_C237EE87 = "{}. Windows disk metrics collection is still in retry backoff.";
  public static final String LOG_RECOVERED_WINDOWS_DISK_METRICS_COLLECTION_THROUGH_POWERSHELL_CIM_03B9110E = "Recovered windows disk metrics collection through PowerShell/CIM.";
  public static final String LOG_IOTDBSESSIONREPORTER_START_WRITE_ARG_ARG_E79CDDAE = "IoTDBSessionReporter start, write to {}:{}";
  public static final String LOG_PROMETHEUSREPORTER_STARTED_USE_PORT_ARG_A688FFC8 = "PrometheusReporter started, use port {}";
  public static final String LOG_DETECTED_ERROR_TAKING_METRIC_TIMER_SNAPSHOT_WILL_DISCARD_METRIC_B7154169 = "Detected an error when taking metric timer snapshot, will discard this metric";

}
