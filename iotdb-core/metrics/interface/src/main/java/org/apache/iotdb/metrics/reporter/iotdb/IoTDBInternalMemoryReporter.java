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

package org.apache.iotdb.metrics.reporter.iotdb;

import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.ReporterType;

import java.util.Map;

public class IoTDBInternalMemoryReporter extends IoTDBInternalReporter {
  @Override
  public InternalReporterType getType() {
    return InternalReporterType.MEMORY;
  }

  @Override
  protected void writeMetricToIoTDB(Map<String, Object> valueMap, String prefix, long time) {
    // do nothing
  }

  @Override
  protected void writeMetricsToIoTDB(Map<String, Map<String, Object>> valueMap, long time) {
    // do nothing
  }

  @Override
  public boolean start() {
    return true;
  }

  @Override
  public boolean stop() {
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.IOTDB;
  }
}
