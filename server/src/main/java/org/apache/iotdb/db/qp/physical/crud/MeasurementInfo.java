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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class MeasurementInfo {

  public MeasurementInfo() {}

  public MeasurementInfo(String measurement) {
    this.measurement = measurement;
  }

  private String measurement;

  // select s1, s2 as speed from root, then s2 -> speed
  private String measurementAlias;

  // to record the datatype of the column in the result set
  private TSDataType columnDataType;

  public void setMeasurement(String measurement) {
    this.measurement = measurement;
  }

  public String getMeasurement() {
    return measurement;
  }

  public void setMeasurementAlias(String measurementAlias) {
    this.measurementAlias = measurementAlias;
  }

  public String getMeasurementAlias() {
    return measurementAlias;
  }

  public void setColumnDataType(TSDataType columnDataType) {
    this.columnDataType = columnDataType;
  }

  public TSDataType getColumnDataType() {
    return columnDataType;
  }
}
