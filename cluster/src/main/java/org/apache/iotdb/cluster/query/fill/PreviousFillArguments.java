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

package org.apache.iotdb.cluster.query.fill;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Set;

/**
 * PreviousFillParameter records necessary parameters of a previous fill over a single timeseries,
 * avoiding the corresponding method call having too many arguments and increasing flexibility.
 */
public class PreviousFillArguments {
  private PartialPath path;
  private TSDataType dataType;
  private long queryTime;
  private long beforeRange;
  private Set<String> deviceMeasurements;

  public PreviousFillArguments(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      long beforeRange,
      Set<String> deviceMeasurements) {
    this.path = path;
    this.dataType = dataType;
    this.queryTime = queryTime;
    this.beforeRange = beforeRange;
    this.deviceMeasurements = deviceMeasurements;
  }

  public PartialPath getPath() {
    return path;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public long getQueryTime() {
    return queryTime;
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  public Set<String> getDeviceMeasurements() {
    return deviceMeasurements;
  }
}
