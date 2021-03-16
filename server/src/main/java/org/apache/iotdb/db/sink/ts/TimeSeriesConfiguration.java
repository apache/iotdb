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

package org.apache.iotdb.db.sink.ts;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.sink.api.Configuration;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TimeSeriesConfiguration extends Configuration {

  private final PartialPath device;
  private final String[] measurements;
  private final TSDataType[] dataTypes;

  public TimeSeriesConfiguration(
      String id, String device, String[] measurements, TSDataType[] dataTypes)
      throws IllegalPathException {
    super(id);
    this.device = new PartialPath(device);
    this.measurements = measurements;
    this.dataTypes = dataTypes;
  }

  public PartialPath getDevice() {
    return device;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TimeSeriesConfiguration)) {
      return false;
    }

    TimeSeriesConfiguration that = (TimeSeriesConfiguration) o;

    return new EqualsBuilder()
        .appendSuper(super.equals(o))
        .append(device, that.device)
        .append(measurements, that.measurements)
        .append(dataTypes, that.dataTypes)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .appendSuper(super.hashCode())
        .append(device)
        .append(measurements)
        .append(dataTypes)
        .toHashCode();
  }
}
