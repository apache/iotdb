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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public abstract class InsertNode extends SearchNode implements ComparableConsensusRequest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * if use id table, this filed is id form of device path <br>
   * if not, this filed is device path<br>
   */
  protected PartialPath devicePath;

  protected boolean isAligned;
  protected MeasurementSchema[] measurementSchemas;
  protected String[] measurements;
  protected TSDataType[] dataTypes;

  protected int failedMeasurementNumber = 0;

  /**
   * device id reference, for reuse device id in both id table and memtable <br>
   * used in memtable
   */
  protected IDeviceID deviceID;

  protected boolean isGeneratedByRemoteConsensusLeader = false;

  /** Physical address of data region after splitting */
  protected TRegionReplicaSet dataRegionReplicaSet;

  protected ProgressIndex progressIndex;

  private static final DeviceIDFactory deviceIDFactory = DeviceIDFactory.getInstance();

  protected InsertNode(PlanNodeId id) {
    super(id);
  }

  protected InsertNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes) {
    super(id);
    this.devicePath = devicePath;
    this.isAligned = isAligned;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
  }

  public TRegionReplicaSet getDataRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public void setDataRegionReplicaSet(TRegionReplicaSet dataRegionReplicaSet) {
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

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public TSDataType getDataType(int index) {
    return dataTypes[index];
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  public IDeviceID getDeviceID() {
    if (deviceID == null) {
      deviceID = deviceIDFactory.getDeviceID(devicePath);
    }
    return deviceID;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public boolean isGeneratedByRemoteConsensusLeader() {
    switch (config.getDataRegionConsensusProtocolClass()) {
      case ConsensusFactory.IOT_CONSENSUS:
      case ConsensusFactory.IOT_CONSENSUS_V2:
      case ConsensusFactory.FAST_IOT_CONSENSUS:
      case ConsensusFactory.RATIS_CONSENSUS:
        return isGeneratedByRemoteConsensusLeader;
      case ConsensusFactory.SIMPLE_CONSENSUS:
        return false;
    }
    return false;
  }

  @Override
  public void markAsGeneratedByRemoteConsensusLeader() {
    isGeneratedByRemoteConsensusLeader = true;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new NotImplementedException("serializeAttributes of InsertNode is not implemented");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new NotImplementedException("serializeAttributes of InsertNode is not implemented");
  }

  // region Serialization methods for WAL
  /** Serialized size of measurement schemas, ignoring failed time series */
  protected int serializeMeasurementSchemasSize() {
    int byteLen = 0;
    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      byteLen += WALWriteUtils.sizeToWrite(measurementSchemas[i]);
    }
    return byteLen;
  }

  /** Serialize measurement schemas, ignoring failed time series */
  protected void serializeMeasurementSchemasToWAL(IWALByteBufferView buffer) {
    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      WALWriteUtils.write(measurementSchemas[i], buffer);
    }
  }

  /**
   * Deserialize measurement schemas. Make sure the measurement schemas and measurements have been
   * created before calling this
   */
  protected void deserializeMeasurementSchemas(DataInputStream stream) throws IOException {
    for (int i = 0; i < measurements.length; i++) {
      measurementSchemas[i] = MeasurementSchema.deserializeFrom(stream);
      measurements[i] = measurementSchemas[i].getMeasurementId();
      dataTypes[i] = measurementSchemas[i].getType();
    }
  }

  protected void deserializeMeasurementSchemas(ByteBuffer buffer) {
    for (int i = 0; i < measurements.length; i++) {
      measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
      measurements[i] = measurementSchemas[i].getMeasurementId();
    }
  }

  // endregion

  public TRegionReplicaSet getRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public abstract long getMinTime();

  // region partial insert
  @TestOnly
  public void markFailedMeasurement(int index) {
    throw new UnsupportedOperationException();
  }

  public boolean hasValidMeasurements() {
    for (Object o : measurements) {
      if (o != null) {
        return true;
      }
    }
    return false;
  }

  public void setFailedMeasurementNumber(int failedMeasurementNumber) {
    this.failedMeasurementNumber = failedMeasurementNumber;
  }

  public int getFailedMeasurementNumber() {
    return failedMeasurementNumber;
  }

  public boolean allMeasurementFailed() {
    if (measurements != null) {
      return failedMeasurementNumber >= measurements.length;
    }
    return true;
  }

  // endregion

  // region progress index

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  // endregion

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
