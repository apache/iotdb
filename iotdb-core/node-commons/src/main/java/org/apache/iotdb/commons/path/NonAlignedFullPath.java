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

package org.apache.iotdb.commons.path;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Objects;

public class NonAlignedFullPath implements IFullPath {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NonAlignedFullPath.class);

  private final IDeviceID deviceID;

  private final IMeasurementSchema measurementSchema;

  public NonAlignedFullPath(IDeviceID deviceID, IMeasurementSchema measurementSchema) {
    this.deviceID = deviceID;
    this.measurementSchema = measurementSchema;
  }

  @Override
  public IDeviceID getDeviceId() {
    return deviceID;
  }

  @Override
  public TSDataType getSeriesType() {
    return measurementSchema.getType();
  }

  public IMeasurementSchema getMeasurementSchema() {
    return measurementSchema;
  }

  public String getMeasurement() {
    return measurementSchema.getMeasurementName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NonAlignedFullPath that = (NonAlignedFullPath) o;
    return Objects.equals(deviceID, that.deviceID)
        && Objects.equals(measurementSchema, that.measurementSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID, measurementSchema);
  }

  @Override
  public String toString() {
    return "NonAlignedFullPath{"
        + "deviceID="
        + deviceID
        + ", measurementSchema="
        + measurementSchema
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + deviceID.ramBytesUsed()
        + RamUsageEstimator.sizeOf(measurementSchema.getMeasurementName());
  }
}
