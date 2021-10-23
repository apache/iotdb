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
package org.apache.iotdb.influxdb.protocol.dto;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class IoTDBPoint {

  private final String deviceId;
  private final long time;
  private final List<String> measurements;
  private final List<TSDataType> types;
  private final List<Object> values;

  public IoTDBPoint(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values) {
    this.deviceId = deviceId;
    this.time = time;
    this.measurements = measurements;
    this.types = types;
    this.values = values;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public long getTime() {
    return time;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public List<TSDataType> getTypes() {
    return types;
  }

  public List<Object> getValues() {
    return values;
  }
}
