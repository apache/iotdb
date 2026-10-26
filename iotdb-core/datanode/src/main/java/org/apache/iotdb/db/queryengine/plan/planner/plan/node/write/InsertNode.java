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
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALReadUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

  protected long memorySize;

  private static final DeviceIDFactory deviceIDFactory = DeviceIDFactory.getInstance();

  protected InsertNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public final SearchNode merge(List<SearchNode> searchNodes) {
    if (searchNodes.isEmpty()) {
      throw new IllegalArgumentException("insertNodes should never be empty");
    }
    if (searchNodes.size() == 1) {
      return searchNodes.get(0);
    }
    List<InsertNode> insertNodes =
        searchNodes.stream()
            .map(searchNode -> (InsertNode) searchNode)
            .collect(Collectors.toList());
    InsertNode result = mergeInsertNode(insertNodes);
    result.setSearchIndex(insertNodes.get(0).getSearchIndex());
    result.setDevicePath(insertNodes.get(0).getDevicePath());
    return result;
  }

  public abstract InsertNode mergeInsertNode(List<InsertNode> insertNodes);

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
    return dataTypes == null || index < 0 || index >= dataTypes.length ? null : dataTypes[index];
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

  public boolean isDeviceIDExists() {
    return deviceID != null;
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

  protected static int serializeString(String value, ByteBuffer buffer) {
    if (value == null) {
      return ReadWriteIOUtils.write(-1, buffer);
    }
    byte[] bytes = value.getBytes(TSFileConfig.STRING_CHARSET);
    int len = ReadWriteIOUtils.write(bytes.length, buffer);
    buffer.put(bytes);
    return len + bytes.length;
  }

  protected static int serializeString(String value, DataOutputStream stream) throws IOException {
    if (value == null) {
      return ReadWriteIOUtils.write(-1, stream);
    }
    byte[] bytes = value.getBytes(TSFileConfig.STRING_CHARSET);
    int len = ReadWriteIOUtils.write(bytes.length, stream);
    stream.write(bytes);
    return len + bytes.length;
  }

  protected static String deserializeString(ByteBuffer buffer) {
    int strLength = ReadWriteIOUtils.readInt(buffer);
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes);
    return new String(bytes, TSFileConfig.STRING_CHARSET);
  }

  protected static void serializeMeasurementSchema(
      MeasurementSchema measurementSchema, ByteBuffer buffer) {
    serializeString(measurementSchema.getMeasurementId(), buffer);
    ReadWriteIOUtils.write(measurementSchema.getTypeInByte(), buffer);
    ReadWriteIOUtils.write(measurementSchema.getEncodingType().serialize(), buffer);
    ReadWriteIOUtils.write(measurementSchema.getCompressor().serialize(), buffer);
    serializeProps(measurementSchema.getProps(), buffer);
  }

  protected static void serializeMeasurementSchema(
      MeasurementSchema measurementSchema, DataOutputStream stream) throws IOException {
    serializeString(measurementSchema.getMeasurementId(), stream);
    ReadWriteIOUtils.write(measurementSchema.getTypeInByte(), stream);
    ReadWriteIOUtils.write(measurementSchema.getEncodingType().serialize(), stream);
    ReadWriteIOUtils.write(measurementSchema.getCompressor().serialize(), stream);
    serializeProps(measurementSchema.getProps(), stream);
  }

  protected static MeasurementSchema deserializeMeasurementSchema(ByteBuffer buffer) {
    String measurementId = deserializeString(buffer);
    byte type = ReadWriteIOUtils.readByte(buffer);
    byte encoding = ReadWriteIOUtils.readByte(buffer);
    byte compressor = ReadWriteIOUtils.readByte(buffer);
    Map<String, String> props = deserializeProps(buffer);
    return new MeasurementSchema(measurementId, type, encoding, compressor, props);
  }

  private static void serializeProps(Map<String, String> props, ByteBuffer buffer) {
    if (props == null) {
      ReadWriteIOUtils.write(0, buffer);
      return;
    }
    ReadWriteIOUtils.write(props.size(), buffer);
    for (Map.Entry<String, String> entry : props.entrySet()) {
      serializeString(entry.getKey(), buffer);
      serializeString(entry.getValue(), buffer);
    }
  }

  private static void serializeProps(Map<String, String> props, DataOutputStream stream)
      throws IOException {
    if (props == null) {
      ReadWriteIOUtils.write(0, stream);
      return;
    }
    ReadWriteIOUtils.write(props.size(), stream);
    for (Map.Entry<String, String> entry : props.entrySet()) {
      serializeString(entry.getKey(), stream);
      serializeString(entry.getValue(), stream);
    }
  }

  private static Map<String, String> deserializeProps(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size <= 0) {
      return null;
    }
    Map<String, String> props = new HashMap<>();
    for (int i = 0; i < size; i++) {
      props.put(deserializeString(buffer), deserializeString(buffer));
    }
    return props;
  }

  // region Serialization methods for WAL
  /** Serialized size of measurement schemas, ignoring failed time series */
  protected int serializeMeasurementSchemasSize() {
    int byteLen = 0;
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      if (shouldSerializeMeasurementToWAL(i)) {
        byteLen += WALWriteUtils.sizeToWrite(measurementSchemas[i]);
      }
    }
    return byteLen;
  }

  /** Serialize measurement schemas, ignoring failed time series */
  protected void serializeMeasurementSchemasToWAL(IWALByteBufferView buffer) {
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      if (shouldSerializeMeasurementToWAL(i)) {
        WALWriteUtils.write(measurementSchemas[i], buffer);
      }
    }
  }

  /**
   * Deserialize measurement schemas. Make sure the measurement schemas and measurements have been
   * created before calling this
   */
  protected void deserializeMeasurementSchemas(DataInputStream stream) throws IOException {
    for (int i = 0; i < measurements.length; i++) {
      measurementSchemas[i] = WALReadUtils.readMeasurementSchema(stream);
      measurements[i] = measurementSchemas[i].getMeasurementId();
      dataTypes[i] = measurementSchemas[i].getType();
    }
  }

  protected void deserializeMeasurementSchemas(ByteBuffer buffer) {
    for (int i = 0; i < measurements.length; i++) {
      measurementSchemas[i] = WALReadUtils.readMeasurementSchema(buffer);
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
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      if (!isMeasurementFailed(i)) {
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

  protected int getValidMeasurementNumber() {
    int validMeasurementNumber = 0;
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      if (shouldSerializeMeasurement(i)) {
        validMeasurementNumber++;
      }
    }
    return validMeasurementNumber;
  }

  protected int getValidMeasurementNumberForWAL() {
    int validMeasurementNumber = 0;
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      if (shouldSerializeMeasurementToWAL(i)) {
        validMeasurementNumber++;
      }
    }
    return validMeasurementNumber;
  }

  protected boolean shouldSerializeMeasurement(final int index) {
    return !isMeasurementFailed(index);
  }

  protected boolean shouldSerializeMeasurementToWAL(final int index) {
    return shouldSerializeMeasurement(index)
        && measurementSchemas != null
        && index < measurementSchemas.length
        && measurementSchemas[index] != null;
  }

  public boolean isMeasurementFailed(int index) {
    return measurements == null
        || index < 0
        || index >= measurements.length
        || measurements[index] == null;
  }

  public boolean allMeasurementFailed() {
    return measurements == null || !hasValidMeasurements();
  }

  public String[] getRawMeasurements() {
    String[] measurements = getMeasurements();
    MeasurementSchema[] measurementSchemas = getMeasurementSchemas();
    String[] rawMeasurements = measurements;
    for (int i = 0; i < measurements.length; i++) {
      if (measurementSchemas != null
          && i < measurementSchemas.length
          && measurementSchemas[i] != null) {
        // get raw measurement rather than alias
        String rawMeasurement = measurementSchemas[i].getMeasurementId();
        if (!Objects.equals(rawMeasurement, measurements[i])) {
          if (rawMeasurements == measurements) {
            rawMeasurements = Arrays.copyOf(measurements, measurements.length);
          }
          rawMeasurements[i] = rawMeasurement;
        }
      }
    }
    return rawMeasurements;
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

  @Override
  public long getMemorySize() {
    if (memorySize == 0) {
      memorySize = InsertNodeMemoryEstimator.sizeOf(this);
    }
    return memorySize;
  }
}
