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

import java.util.List;
import java.util.Objects;

public class AlignedFullPath implements IFullPath {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedFullPath.class);

  public static final String VECTOR_PLACEHOLDER = "";

  private final IDeviceID deviceID;

  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;

  public AlignedFullPath(
      IDeviceID deviceID, List<String> measurementList, List<IMeasurementSchema> schemaList) {
    this.deviceID = deviceID;
    this.measurementList = measurementList;
    this.schemaList = schemaList;
  }

  @Override
  public IDeviceID getDeviceId() {
    return deviceID;
  }

  @Override
  public TSDataType getSeriesType() {
    return TSDataType.VECTOR;
  }

  public List<String> getMeasurementList() {
    return measurementList;
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public int getColumnNum() {
    return measurementList.size();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + deviceID.ramBytesUsed()
        + measurementList.stream().mapToLong(RamUsageEstimator::sizeOf).sum() * 2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlignedFullPath that = (AlignedFullPath) o;
    return Objects.equals(deviceID, that.deviceID)
        && Objects.equals(measurementList, that.measurementList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID, measurementList);
  }
}
