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
      "Interrupted when waiting for the next device view update: {}";
  public static final String SERVICE_STARTED_SUCCESSFULLY = "{} is started successfully.";
  public static final String SERVICE_STOPPED_SUCCESSFULLY = "{} is stopped successfully.";

  // ---- AbstractThriftServiceThread ----
  public static final String STOP_COUNT_DOWN_LATCH_IS_NULL = "Stop Count Down latch is null";
  public static final String STOP_COUNT_DOWN_LATCH_IS = "Stop Count Down latch is {}";
  public static final String CLOSE_THREAD_POOL_SERVER_AND_SERVER_SOCKET =
      "{}: close TThreadPoolServer and TServerSocket for {}";
  public static final String FAILED_TO_START_BECAUSE = "failed to start, because ";
  public static final String FAILED_TO_START_SERVICE_BECAUSE =
      "%s: failed to start %s, because ";
  public static final String SERVICE_THREAD_BEGIN_TO_RUN =
      "The {} service thread begin to run...";
  public static final String SERVICE_EXIT_BECAUSE = "%s: %s exit, because ";
  public static final String UNEXPECTED_SERVER_TYPE = "Unexpected serverType {}";
  public static final String FAILED_TO_LOAD_KEYSTORE_OR_TRUSTSTORE =
      "Failed to load keystore or truststore file";
  public static final String KEYSTORE_OR_TRUSTSTORE_NOT_FOUND =
      "keystore or truststore file not found";

  // ---- JMXService ----
  public static final String FAILED_TO_REGISTER_MBEAN = "Failed to registerMBean {}";
  public static final String FAILED_TO_UNREGISTER_MBEAN = "Failed to unregisterMBean {}";
  public static final String JMX_PORT_IS_UNDEFINED = "{} JMX port is undefined";

  // ---- RegisterManager ----
  public static final String SERVICE_ALREADY_REGISTERED =
      "{} has already been registered. skip";
  public static final String SERVICE_STARTED_WITH_TIME =
      "The {} service is started successfully, which takes {} ms.";
  public static final String SERVICE_DEREGISTERED = "{} deregistered";
  public static final String FAILED_TO_STOP_SERVICE_BECAUSE = "Failed to stop {} because:";
  public static final String DEREGISTER_ALL_SERVICE = "deregister all service.";

  // ---- StartupChecks ----
  public static final String START_JMX_LOCALLY = "Start JMX locally.";
  public static final String JMX_PORT_MISSING_FROM_ENV =
      "{} missing from {}.sh(Unix or OS X, if you use Windows, check conf/{}.bat)";
  public static final String START_JMX_REMOTELY =
      "Start JMX remotely: JMX is enabled to receive remote connection on port {}";
  public static final String JDK_VERSION_TOO_LOW =
      "Requires JDK version >= %d, current version is %d.";
  public static final String JDK_VERSION_IS = "JDK version is {}.";
  public static final String JVM_VERSION_IS = "JVM version is {} {}.";
  public static final String GRAALVM_NOT_RECOMMENDED =
      "Perhaps you are using GraalVM, which is strongly not recommended. Using GraalVM may cause strange problems after the system runs for a while. Please check your JVM version.";

  // ---- ThriftService ----
  public static final String START_LATCH_NULL_WHEN_GETTING_STATUS =
      "Start latch is null when getting status";
  public static final String START_STATUS_WHEN_GETTING_STATUS =
      "Start status is {} when getting status";
  public static final String STOP_LATCH_NULL_WHEN_GETTING_STATUS =
      "Stop latch is null when getting status";
  public static final String STOP_LATCH_WHEN_GETTING_STATUS =
      "Stop latch is {} when getting status";
  public static final String SERVICE_ALREADY_RUNNING =
      "{}: {} has been already running now";
  public static final String START_SERVICE = "{}: start {}...";
  public static final String START_SERVICE_SUCCESSFULLY =
      "{}: start {} successfully, listening on ip {} port {}";
  public static final String SERVICE_NOT_RUNNING = "{}: {} isn't running now";
  public static final String CLOSING_SERVICE = "{}: closing {}...";
  public static final String CLOSE_SERVICE_SUCCESSFULLY = "{}: close {} successfully";
  public static final String CLOSE_SERVICE_FAILED = "{}: close {} failed because: ";

  // ---- GcTimeAlerter ----
  public static final String GC_ALERT_METRICS_TAKEN_TIME = "Error metrics taken time: {}";
  public static final String GC_ALERT_GC_TIME_PERCENTAGE = "Gc Time Percentage: {}%";
  public static final String GC_ALERT_ACCUMULATED_GC_TIME =
      "Accumulated GC time within current observation window: {} ms";
  public static final String GC_ALERT_OBSERVATION_WINDOW_FROM_TO =
      "The observation window is from: {} to: {}";
  public static final String GC_ALERT_OBSERVATION_WINDOW_TIME =
      "The observation window time is: {} ms.";

  // ---- JvmGcMonitorMetrics ----
  public static final String JVM_GC_MONITOR_STOPPED =
      "JVM GC scheduled monitor is stopped successfully.";

  // ---- MetricService ----
  public static final String LOAD_METRIC_REPORTERS = "Load metric reporters, type: {}";
  public static final String FAILED_TO_LOAD_REPORTER =
      "Failed to load reporter which type is {}";
  public static final String METRIC_SERVICE_START_TO_INIT = "MetricService start to init.";
  public static final String METRIC_SERVICE_START_SUCCESSFULLY =
      "MetricService start successfully.";
  public static final String METRIC_SERVICE_FAILED_TO_START =
      "MetricService failed to start {} because: ";
  public static final String METRIC_SERVICE_TRY_TO_RESTART =
      "MetricService try to restart.";
  public static final String METRIC_SERVICE_REBIND_METRIC_SET =
      "MetricService rebind metricSet: {}";
  public static final String METRIC_SERVICE_RESTART_SUCCESSFULLY =
      "MetricService restart successfully.";
  public static final String METRIC_SERVICE_TRY_TO_STOP = "MetricService try to stop.";
  public static final String METRIC_SERVICE_STOP_SUCCESSFULLY =
      "MetricService stop successfully.";
  public static final String METRIC_SERVICE_RELOAD_INTERNAL_REPORTER =
      "MetricService reload internal reporter.";
  public static final String METRIC_SERVICE_RELOAD_INTERNAL_REPORTER_SUCCESSFULLY =
      "MetricService reload internal reporter successfully.";
  public static final String METRIC_SERVICE_RESTART_REPORTERS_SUCCESSFULLY =
      "MetricService restart reporters successfully.";
  public static final String NOTHING_CHANGED_IN_METRIC_CONFIG =
      "There are nothing change in metric config.";
  public static final String INTERNAL_REPORTER_FAILED_TO_START =
      "Internal Reporter failed to start!";

  // ---- CpuUsageMetrics ----
  public static final String CPU_USAGE_UPDATE_TIME = "Time for update cpu usage is {} ns";

  private ServiceMessages() {}
}
