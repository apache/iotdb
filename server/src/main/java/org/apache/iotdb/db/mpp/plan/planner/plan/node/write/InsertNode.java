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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public abstract class InsertNode extends WritePlanNode implements ComparableConsensusRequest {

  /** this insert node doesn't need to participate in iot consensus */
  public static final long NO_CONSENSUS_INDEX = ConsensusReqReader.DEFAULT_SEARCH_INDEX;

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

  /**
   * this index is used by wal search, its order should be protected by the upper layer, and the
   * value should start from 1
   */
  protected long searchIndex = NO_CONSENSUS_INDEX;

  /** Physical address of data region after splitting */
  protected TRegionReplicaSet dataRegionReplicaSet;

  protected ProgressIndex progressIndex;

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
    return deviceID;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public long getSearchIndex() {
    return searchIndex;
  }

  /** Search index should start from 1 */
  public void setSearchIndex(long searchIndex) {
    this.searchIndex = searchIndex;
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
      if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
        byteLen += WALWriteUtils.sizeToWrite(measurementSchemas[i]);
      } else {
        byteLen += ReadWriteIOUtils.sizeToWrite(measurements[i]);
        // datatype size
        byteLen++;
      }
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

      // serialize measurementId only for standalone version for better write performance
      if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
        WALWriteUtils.write(measurementSchemas[i], buffer);
      } else {
        WALWriteUtils.write(measurements[i], buffer);
        WALWriteUtils.write(dataTypes[i], buffer);
      }
    }
  }

  /**
   * Deserialize measurement schemas. Make sure the measurement schemas and measurements have been
   * created before calling this
   */
  protected void deserializeMeasurementSchemas(DataInputStream stream) throws IOException {
    for (int i = 0; i < measurements.length; i++) {
      if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
        measurementSchemas[i] = MeasurementSchema.deserializeFrom(stream);
        measurements[i] = measurementSchemas[i].getMeasurementId();
        dataTypes[i] = measurementSchemas[i].getType();
      } else {
        measurements[i] = ReadWriteIOUtils.readString(stream);
        dataTypes[i] = TSDataType.deserialize(ReadWriteIOUtils.readByte(stream));
      }
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

  /**
   * Notice: Call this method ONLY when using IOT_CONSENSUS, other consensus protocol cannot
   * distinguish whether the insertNode sync from leader by this method.
   * isSyncFromLeaderWhenUsingIoTConsensus == true means this node is a follower
   */
  public boolean isSyncFromLeaderWhenUsingIoTConsensus() {
    return searchIndex == ConsensusReqReader.DEFAULT_SEARCH_INDEX;
  }

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
  // endregion

  // region consensus index

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
