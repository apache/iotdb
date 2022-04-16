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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.write;

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public abstract class InsertNode extends WritePlanNode {

  /**
   * if use id table, this filed is id form of device path <br>
   * if not, this filed is device path<br>
   */
  protected PartialPath devicePath;

  protected boolean isAligned;
  protected MeasurementSchema[] measurementSchemas;
  protected String[] measurements;
  protected TSDataType[] dataTypes;
  // TODO(INSERT) need to change it to a function handle to update last time value
  //  protected IMeasurementMNode[] measurementMNodes;

  /**
   * device id reference, for reuse device id in both id table and memtable <br>
   * used in memtable
   */
  protected IDeviceID deviceID;

  /** Physical address of data region after splitting */
  RegionReplicaSet dataRegionReplicaSet;

  protected InsertNode(PlanNodeId id) {
    super(id);
  }

  protected InsertNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      MeasurementSchema[] measurementSchemas,
      TSDataType[] dataTypes) {
    super(id);
    this.devicePath = devicePath;
    this.isAligned = isAligned;
    this.measurementSchemas = measurementSchemas;
    this.dataTypes = dataTypes;

    this.measurements = new String[measurementSchemas.length];
    for (int i = 0; i < measurementSchemas.length; i++) {
      if (measurementSchemas[i] != null) {
        measurements[i] = measurementSchemas[i].getMeasurementId();
      }
    }
  }

  public RegionReplicaSet getDataRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public void setDataRegionReplicaSet(RegionReplicaSet dataRegionReplicaSet) {
    this.dataRegionReplicaSet = dataRegionReplicaSet;
  }

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public void setDevicePath(PartialPath devicePath) {
    this.devicePath = devicePath;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public MeasurementSchema[] getMeasurementSchemas() {
    return measurementSchemas;
  }

  public void setMeasurementSchemas(MeasurementSchema[] measurementSchemas) {
    this.measurementSchemas = measurementSchemas;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public RegionReplicaSet getRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new NotImplementedException("serializeAttributes of InsertNode is not implemented");
  }

  protected int countFailedMeasurements() {
    int result = 0;
    for (MeasurementSchema measurement : measurementSchemas) {
      if (measurement == null) {
        result++;
      }
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InsertNode that = (InsertNode) o;
    return isAligned == that.isAligned
        && Objects.equals(devicePath, that.devicePath)
        && Arrays.equals(measurementSchemas, that.measurementSchemas)
        && Arrays.equals(measurements, that.measurements)
        && Arrays.equals(dataTypes, that.dataTypes)
        && Objects.equals(deviceID, that.deviceID)
        && Objects.equals(dataRegionReplicaSet, that.dataRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(super.hashCode(), devicePath, isAligned, deviceID, dataRegionReplicaSet);
    result = 31 * result + Arrays.hashCode(measurementSchemas);
    result = 31 * result + Arrays.hashCode(measurements);
    result = 31 * result + Arrays.hashCode(dataTypes);
    return result;
  }
}
