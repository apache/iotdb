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
  public static final String JMX_REGISTER_FAILED = "IoTDB Metric: Unable to register ";
  public static final String JMX_UNREGISTER_FAILED = "IoTDB Metric: Unable to unregister: ";
  public static final String JMX_REPORTER_ALREADY_START = "IoTDB Metric: JmxReporter already start!";
  public static final String JMX_REPORTER_START_FAILED =
      "IoTDB Metric: JmxReporter failed to start, because ";
  public static final String JMX_REPORTER_START = "IoTDB Metric: JmxReporter start!";
  public static final String JMX_REPORTER_STOP_FAILED =
      "IoTDB Metric: JmxReporter failed to stop, because ";
  public static final String JMX_REPORTER_STOP = "IoTDB Metric: JmxReporter stop!";

  // --- IoTDBMetricObjNameFactory ---
  public static final String JMX_UNABLE_TO_REGISTER = "IoTDB Metric: Unable to register {} {}";

  private MetricsCoreMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_DETECTED_ERROR_TAKING_SNAPSHOT_MAY_CAUSE_MISS_DURING_RECORDING_2BAE49C4 = "Detected an error while taking snapshot, may cause a miss during this recording.";

}
