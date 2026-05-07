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

  private MetricsMessages() {}
}
