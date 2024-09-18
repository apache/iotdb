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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.IntToLongFunction;

import static org.apache.iotdb.db.utils.CommonUtils.isAlive;

public class InsertTabletNode extends InsertNode implements WALEntryValue {

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  protected long[] times; // times should be sorted. It is done in the session API.

  protected BitMap[] bitMaps;
  protected Object[] columns;

  protected int rowCount = 0;

  // When this plan is sub-plan split from another InsertTabletNode, this indicates the original
  // positions of values in
  // this plan. For example, if the plan contains 5 timestamps, and range = [1,4,10,12], then it
  // means that the first 3
  // timestamps in this plan are from range[1,4) of the parent plan, and the last 2 timestamps are
  // from range[10,12)
  // of the parent plan.
  // this is usually used to back-propagate exceptions to the parent plan without losing their
  // proper positions.
  protected List<Integer> range;

  public InsertTabletNode(PlanNodeId id) {
    super(id);
  }

  @TestOnly
  public InsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns,
      int rowCount) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.times = times;
    this.bitMaps = bitMaps;
    this.columns = columns;
    this.rowCount = rowCount;
  }

  public InsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      MeasurementSchema[] measurementSchemas,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns,
      int rowCount) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.measurementSchemas = measurementSchemas;
    this.times = times;
    this.bitMaps = bitMaps;
    this.columns = columns;
    this.rowCount = rowCount;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  public BitMap[] getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.bitMaps = bitMaps;
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public List<Integer> getRange() {
    return range;
  }

  public void setRange(List<Integer> range) {
    this.range = range;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.INSERT_TABLET;
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    // only single device in single database
    if (times.length == 0) {
      return Collections.emptyList();
    }

    final Map<IDeviceID, PartitionSplitInfo> deviceIDSplitInfoMap = collectSplitRanges();
    final Map<TRegionReplicaSet, List<Integer>> splitMap =
        splitByReplicaSet(deviceIDSplitInfoMap, analysis);

    return doSplit(splitMap);
  }

  private Map<IDeviceID, PartitionSplitInfo> collectSplitRanges() {
    long upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[0]);
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[0]);
    int startLoc = 0; // included
    IDeviceID currDeviceId = getDeviceID(0);

    Map<IDeviceID, PartitionSplitInfo> deviceIDSplitInfoMap = new HashMap<>();

    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      IDeviceID nextDeviceId = getDeviceID(i);
      if (times[i] >= upperBoundOfTimePartition || !currDeviceId.equals(nextDeviceId)) {
        final PartitionSplitInfo splitInfo =
            deviceIDSplitInfoMap.computeIfAbsent(
                currDeviceId, deviceID1 -> new PartitionSplitInfo());
        // a new range.
        splitInfo.ranges.add(startLoc); // included
        splitInfo.ranges.add(i); // excluded
        splitInfo.timePartitionSlots.add(timePartitionSlot);
        // next init
        startLoc = i;
        upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[i]);
        timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[i]);
        currDeviceId = nextDeviceId;
      }
    }

    PartitionSplitInfo splitInfo =
        deviceIDSplitInfoMap.computeIfAbsent(currDeviceId, deviceID1 -> new PartitionSplitInfo());
    // the final range
    splitInfo.ranges.add(startLoc); // included
    splitInfo.ranges.add(times.length); // excluded
    splitInfo.timePartitionSlots.add(timePartitionSlot);

    return deviceIDSplitInfoMap;
  }

  private Map<TRegionReplicaSet, List<Integer>> splitByReplicaSet(
      Map<IDeviceID, PartitionSplitInfo> deviceIDSplitInfoMap, IAnalysis analysis) {
    Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();

    for (Entry<IDeviceID, PartitionSplitInfo> entry : deviceIDSplitInfoMap.entrySet()) {
      final IDeviceID deviceID = entry.getKey();
      final PartitionSplitInfo splitInfo = entry.getValue();
      final List<TRegionReplicaSet> replicaSets =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  deviceID, splitInfo.timePartitionSlots, analysis.getDatabaseName());
      splitInfo.replicaSets = replicaSets;
      // collect redirectInfo
      analysis.addEndPointToRedirectNodeList(
          replicaSets
              .get(replicaSets.size() - 1)
              .getDataNodeLocations()
              .get(0)
              .getClientRpcEndPoint());
      for (int i = 0; i < replicaSets.size(); i++) {
        List<Integer> subRanges =
            splitMap.computeIfAbsent(replicaSets.get(i), x -> new ArrayList<>());
        subRanges.add(splitInfo.ranges.get(2 * i));
        subRanges.add(splitInfo.ranges.get(2 * i + 1));
      }
    }
    return splitMap;
  }

  private List<WritePlanNode> doSplit(Map<TRegionReplicaSet, List<Integer>> splitMap) {
    List<WritePlanNode> result = new ArrayList<>();

    if (splitMap.size() == 1) {
      final Entry<TRegionReplicaSet, List<Integer>> entry = splitMap.entrySet().iterator().next();
      if (entry.getValue().size() == 2) {
        // Avoid using system arraycopy when there is no need to split
        setRange(entry.getValue());
        setDataRegionReplicaSet(entry.getKey());
        result.add(this);
        return result;
      }
    }

    for (Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      result.add(generateOneSplit(entry));
    }
    return result;
  }

  protected InsertTabletNode getEmptySplit(int count) {
    long[] subTimes = new long[count];
    Object[] values = initTabletValues(dataTypes.length, count, dataTypes);
    BitMap[] newBitMaps = this.bitMaps == null ? null : initBitmaps(dataTypes.length, count);
    return new InsertTabletNode(
        getPlanNodeId(),
        targetPath,
        isAligned,
        measurements,
        dataTypes,
        measurementSchemas,
        subTimes,
        newBitMaps,
        values,
        subTimes.length);
  }

  private WritePlanNode generateOneSplit(Map.Entry<TRegionReplicaSet, List<Integer>> entry) {
    List<Integer> locs;
    // generate a new times and values
    locs = entry.getValue();
    int count = 0;
    for (int i = 0; i < locs.size(); i += 2) {
      int start = locs.get(i);
      int end = locs.get(i + 1);
      count += end - start;
    }

    final InsertTabletNode subNode = getEmptySplit(count);
    int destLoc = 0;

    for (int i = 0; i < locs.size(); i += 2) {
      int start = locs.get(i);
      int end = locs.get(i + 1);
      final int length = end - start;

      System.arraycopy(times, start, subNode.times, destLoc, length);
      for (int k = 0; k < subNode.columns.length; k++) {
        if (dataTypes[k] != null) {
          System.arraycopy(columns[k], start, subNode.columns[k], destLoc, length);
        }
        if (subNode.bitMaps != null && this.bitMaps[k] != null) {
          BitMap.copyOfRange(this.bitMaps[k], start, subNode.bitMaps[k], destLoc, length);
        }
      }
      destLoc += length;
    }
    subNode.setFailedMeasurementNumber(getFailedMeasurementNumber());
    subNode.setRange(locs);
    subNode.setDataRegionReplicaSet(entry.getKey());
    return subNode;
  }

  @TestOnly
  public List<TTimePartitionSlot> getTimePartitionSlots() {
    List<TTimePartitionSlot> result = new ArrayList<>();
    long upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[0]);
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[0]);
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= upperBoundOfTimePartition) {
        result.add(timePartitionSlot);
        // next init
        upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[i]);
        timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[i]);
      }
    }
    result.add(timePartitionSlot);
    return result;
  }

  protected Object[] initTabletValues(int columnSize, int rowSize, TSDataType[] dataTypes) {
    Object[] values = new Object[columnSize];
    for (int i = 0; i < values.length; i++) {
      if (dataTypes[i] != null) {
        switch (dataTypes[i]) {
          case TEXT:
          case BLOB:
          case STRING:
            values[i] = new Binary[rowSize];
            break;
          case FLOAT:
            values[i] = new float[rowSize];
            break;
          case INT32:
          case DATE:
            values[i] = new int[rowSize];
            break;
          case TIMESTAMP:
          case INT64:
            values[i] = new long[rowSize];
            break;
          case DOUBLE:
            values[i] = new double[rowSize];
            break;
          case BOOLEAN:
            values[i] = new boolean[rowSize];
            break;
        }
      }
    }
    return values;
  }

  protected BitMap[] initBitmaps(int columnSize, int rowSize) {
    BitMap[] bitMaps = new BitMap[columnSize];
    for (int i = 0; i < columnSize; i++) {
      bitMaps[i] = new BitMap(rowSize);
    }
    return bitMaps;
  }

  @Override
  public void markFailedMeasurement(int index) {
    if (measurements[index] == null) {
      return;
    }
    measurements[index] = null;
    dataTypes[index] = null;
    columns[index] = null;
  }

  @Override
  public long getMinTime() {
    return times[0];
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    subSerialize(stream);
  }

  void subSerialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    writeMeasurementsOrSchemas(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer);
    writeBitMaps(buffer);
    writeValues(buffer);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), buffer);
  }

  void subSerialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(targetPath.getFullPath(), stream);
    writeMeasurementsOrSchemas(stream);
    writeDataTypes(stream);
    writeTimes(stream);
    writeBitMaps(stream);
    writeValues(stream);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), stream);
  }

  /** Serialize measurements or measurement schemas, ignoring failed time series */
  private void writeMeasurementsOrSchemas(ByteBuffer buffer) {
    ReadWriteIOUtils.write(measurements.length - getFailedMeasurementNumber(), buffer);
    ReadWriteIOUtils.write((byte) (measurementSchemas != null ? 1 : 0), buffer);

    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize measurement schemas when exist
      if (measurementSchemas != null) {
        measurementSchemas[i].serializeTo(buffer);
      } else {
        ReadWriteIOUtils.write(measurements[i], buffer);
      }
    }
  }

  /** Serialize measurements or measurement schemas, ignoring failed time series */
  private void writeMeasurementsOrSchemas(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(measurements.length - getFailedMeasurementNumber(), stream);
    ReadWriteIOUtils.write((byte) (measurementSchemas != null ? 1 : 0), stream);

    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize measurement schemas when exist
      if (measurementSchemas != null) {
        measurementSchemas[i].serializeTo(stream);
      } else {
        ReadWriteIOUtils.write(measurements[i], stream);
      }
    }
  }

  /** Serialize data types, ignoring failed time series */
  private void writeDataTypes(ByteBuffer buffer) {
    for (int i = 0; i < dataTypes.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      dataTypes[i].serializeTo(buffer);
    }
  }

  /** Serialize data types, ignoring failed time series */
  private void writeDataTypes(DataOutputStream stream) throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      dataTypes[i].serializeTo(stream);
    }
  }

  private void writeTimes(ByteBuffer buffer) {
    ReadWriteIOUtils.write(rowCount, buffer);
    for (long time : times) {
      ReadWriteIOUtils.write(time, buffer);
    }
  }

  private void writeTimes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(rowCount, stream);
    for (long time : times) {
      ReadWriteIOUtils.write(time, stream);
    }
  }

  /** Serialize bitmaps, ignoring failed time series */
  private void writeBitMaps(ByteBuffer buffer) {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(bitMaps != null), buffer);
    if (bitMaps != null) {
      for (int i = 0; i < bitMaps.length; i++) {
        // ignore failed partial insert
        if (measurements[i] == null) {
          continue;
        }

        if (bitMaps[i] == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), buffer);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), buffer);
          buffer.put(bitMaps[i].getByteArray());
        }
      }
    }
  }

  /** Serialize bitmaps, ignoring failed time series */
  private void writeBitMaps(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(bitMaps != null), stream);
    if (bitMaps != null) {
      for (int i = 0; i < bitMaps.length; i++) {
        // ignore failed partial insert
        if (measurements[i] == null) {
          continue;
        }

        if (bitMaps[i] == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), stream);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), stream);
          stream.write(bitMaps[i].getByteArray());
        }
      }
    }
  }

  /** Serialize values, ignoring failed time series */
  private void writeValues(ByteBuffer buffer) {
    for (int i = 0; i < columns.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer);
    }
  }

  /** Serialize values, ignoring failed time series */
  private void writeValues(DataOutputStream stream) throws IOException {
    for (int i = 0; i < columns.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], stream);
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, ByteBuffer buffer) {
    switch (dataType) {
      case INT32:
      case DATE:
        int[] intValues = (int[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(intValues[j], buffer);
        }
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(longValues[j], buffer);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(floatValues[j], buffer);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(doubleValues[j], buffer);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(boolValues[j]), buffer);
        }
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = 0; j < rowCount; j++) {
          if (binaryValues[j] != null) {
            ReadWriteIOUtils.write(binaryValues[j], buffer);
          } else {
            ReadWriteIOUtils.write(0, buffer);
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, DataOutputStream stream)
      throws IOException {
    switch (dataType) {
      case INT32:
      case DATE:
        int[] intValues = (int[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(intValues[j], stream);
        }
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(longValues[j], stream);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(floatValues[j], stream);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(doubleValues[j], stream);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = 0; j < rowCount; j++) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(boolValues[j]), stream);
        }
        break;
      case STRING:
      case TEXT:
      case BLOB:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = 0; j < rowCount; j++) {
          if (binaryValues[j] != null) {
            ReadWriteIOUtils.write(binaryValues[j], stream);
          } else {
            ReadWriteIOUtils.write(0, stream);
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  public static InsertTabletNode deserialize(ByteBuffer byteBuffer) {
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  public void subDeserialize(ByteBuffer buffer) {
    try {
      targetPath = readTargetPath(buffer);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertTabletNode", e);
    }

    int measurementSize = buffer.getInt();
    measurements = new String[measurementSize];

    boolean hasSchema = buffer.get() == 1;
    if (hasSchema) {
      this.measurementSchemas = new MeasurementSchema[measurementSize];
      for (int i = 0; i < measurementSize; i++) {
        measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
        measurements[i] = measurementSchemas[i].getMeasurementId();
      }
    } else {
      for (int i = 0; i < measurementSize; i++) {
        measurements[i] = ReadWriteIOUtils.readString(buffer);
      }
    }

    dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = TSDataType.deserialize(buffer.get());
    }

    rowCount = buffer.getInt();
    times = new long[rowCount];
    times = QueryDataSetUtils.readTimesFromBuffer(buffer, rowCount);

    boolean hasBitMaps = BytesUtils.byteToBool(buffer.get());
    if (hasBitMaps) {
      bitMaps =
          QueryDataSetUtils.readBitMapsFromBuffer(buffer, measurementSize, rowCount).orElse(null);
    }
    columns =
        QueryDataSetUtils.readTabletValuesFromBuffer(buffer, dataTypes, measurementSize, rowCount);
    isAligned = buffer.get() == 1;
  }

  // region serialize & deserialize methods for WAL

  /** Serialized size for wal */
  @Override
  public int serializedSize() {
    return serializedSize(0, rowCount);
  }

  /** Serialized size for wal */
  public int serializedSize(int start, int end) {
    return Short.BYTES + subSerializeSize(start, end);
  }

  int subSerializeSize(int start, int end) {
    int size = 0;
    size += Long.BYTES;
    size += ReadWriteIOUtils.sizeToWrite(targetPath.getFullPath());
    // measurements size
    size += Integer.BYTES;
    size += serializeMeasurementSchemasSize();
    // times size
    size += Integer.BYTES;
    size += Long.BYTES * (end - start);
    // bitmaps size
    size += Byte.BYTES;
    if (bitMaps != null) {
      for (int i = 0; i < bitMaps.length; i++) {
        // ignore failed partial insert
        if (measurements[i] == null) {
          continue;
        }

        size += Byte.BYTES;
        if (bitMaps[i] != null) {
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMaps[i], start, partBitMap, 0, len);
          size += partBitMap.getByteArray().length;
        }
      }
    }
    // values size
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] != null) {
        size += getColumnSize(dataTypes[i], columns[i], start, end);
      }
    }
    // isAlign
    size += Byte.BYTES;
    // column category

    return size;
  }

  private int getColumnSize(TSDataType dataType, Object column, int start, int end) {
    int size = 0;
    switch (dataType) {
      case INT32:
      case DATE:
        size += Integer.BYTES * (end - start);
        break;
      case INT64:
      case TIMESTAMP:
        size += Long.BYTES * (end - start);
        break;
      case FLOAT:
        size += Float.BYTES * (end - start);
        break;
      case DOUBLE:
        size += Double.BYTES * (end - start);
        break;
      case BOOLEAN:
        size += Byte.BYTES * (end - start);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          size += ReadWriteIOUtils.sizeToWrite(binaryValues[j]);
        }
        break;
    }
    return size;
  }

  /**
   * Compared with {@link this#serialize(ByteBuffer)}, more info: search index and data types, less
   * info: isNeedInferType
   */
  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, 0, rowCount);
  }

  public void serializeToWAL(IWALByteBufferView buffer, int start, int end) {
    buffer.putShort(getType().getNodeType());
    subSerialize(buffer, start, end);
  }

  void subSerialize(IWALByteBufferView buffer, int start, int end) {
    buffer.putLong(searchIndex);
    WALWriteUtils.write(targetPath.getFullPath(), buffer);
    // data types are serialized in measurement schemas
    writeMeasurementSchemas(buffer);
    writeTimes(buffer, start, end);
    writeBitMaps(buffer, start, end);
    writeValues(buffer, start, end);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  /** Serialize measurement schemas, ignoring failed time series */
  protected void writeMeasurementSchemas(IWALByteBufferView buffer) {
    buffer.putInt(measurements.length - getFailedMeasurementNumber());
    serializeMeasurementSchemasToWAL(buffer);
  }

  protected void writeTimes(IWALByteBufferView buffer, int start, int end) {
    buffer.putInt(end - start);
    for (int i = start; i < end; i++) {
      buffer.putLong(times[i]);
    }
  }

  /** Serialize bitmaps, ignoring failed time series */
  protected void writeBitMaps(IWALByteBufferView buffer, int start, int end) {
    buffer.put(BytesUtils.boolToByte(bitMaps != null));
    if (bitMaps != null) {
      for (int i = 0; i < bitMaps.length; i++) {
        // ignore failed partial insert
        if (measurements[i] == null) {
          continue;
        }

        if (bitMaps[i] == null) {
          buffer.put(BytesUtils.boolToByte(false));
        } else {
          buffer.put(BytesUtils.boolToByte(true));
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMaps[i], start, partBitMap, 0, len);
          buffer.put(partBitMap.getByteArray());
        }
      }
    }
  }

  /** Serialize values, ignoring failed time series */
  protected void writeValues(IWALByteBufferView buffer, int start, int end) {
    for (int i = 0; i < columns.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  private void serializeColumn(
      TSDataType dataType, Object column, IWALByteBufferView buffer, int start, int end) {
    switch (dataType) {
      case INT32:
      case DATE:
        int[] intValues = (int[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(intValues[j]);
        }
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) column;
        for (int j = start; j < end; j++) {
          buffer.putLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = start; j < end; j++) {
          buffer.putFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = start; j < end; j++) {
          buffer.putDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = start; j < end; j++) {
          buffer.put(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case STRING:
      case TEXT:
      case BLOB:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          if (binaryValues[j] != null) {
            buffer.putInt(binaryValues[j].getLength());
            buffer.put(binaryValues[j].getValues());
          } else {
            buffer.putInt(0);
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  /** Deserialize from wal */
  public static InsertTabletNode deserializeFromWAL(DataInputStream stream) throws IOException {
    // we do not store plan node id in wal entry
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserializeFromWAL(stream);
    return insertNode;
  }

  protected void subDeserializeFromWAL(DataInputStream stream) throws IOException {
    searchIndex = stream.readLong();
    try {
      targetPath = readTargetPath(stream);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertTabletNode", e);
    }

    int measurementSize = stream.readInt();
    measurements = new String[measurementSize];
    measurementSchemas = new MeasurementSchema[measurementSize];
    dataTypes = new TSDataType[measurementSize];
    deserializeMeasurementSchemas(stream);

    rowCount = stream.readInt();
    times = new long[rowCount];
    times = QueryDataSetUtils.readTimesFromStream(stream, rowCount);

    boolean hasBitMaps = BytesUtils.byteToBool(stream.readByte());
    if (hasBitMaps) {
      bitMaps =
          QueryDataSetUtils.readBitMapsFromStream(stream, measurementSize, rowCount).orElse(null);
    }
    columns =
        QueryDataSetUtils.readTabletValuesFromStream(stream, dataTypes, measurementSize, rowCount);
    isAligned = stream.readByte() == 1;
  }

  public static InsertTabletNode deserializeFromWAL(ByteBuffer buffer) {
    // we do not store plan node id in wal entry
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserializeFromWAL(buffer);
    return insertNode;
  }

  protected void subDeserializeFromWAL(ByteBuffer buffer) {
    searchIndex = buffer.getLong();
    try {
      targetPath = readTargetPath(buffer);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertTabletNode", e);
    }

    int measurementSize = buffer.getInt();
    measurements = new String[measurementSize];
    measurementSchemas = new MeasurementSchema[measurementSize];
    deserializeMeasurementSchemas(buffer);

    // data types are serialized in measurement schemas
    dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = measurementSchemas[i].getType();
    }

    rowCount = buffer.getInt();
    times = new long[rowCount];
    times = QueryDataSetUtils.readTimesFromBuffer(buffer, rowCount);

    boolean hasBitMaps = BytesUtils.byteToBool(buffer.get());
    if (hasBitMaps) {
      bitMaps =
          QueryDataSetUtils.readBitMapsFromBuffer(buffer, measurementSize, rowCount).orElse(null);
    }
    columns =
        QueryDataSetUtils.readTabletValuesFromBuffer(buffer, dataTypes, measurementSize, rowCount);
    isAligned = buffer.get() == 1;
  }

  // endregion

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), rowCount, range);
    result = 31 * result + Arrays.hashCode(times);
    result = 31 * result + Arrays.hashCode(bitMaps);
    result = 31 * result + Arrays.deepHashCode(columns);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    InsertTabletNode that = (InsertTabletNode) o;
    return rowCount == that.rowCount
        && Arrays.equals(times, that.times)
        && Arrays.equals(bitMaps, that.bitMaps)
        && equals(that.columns)
        && Objects.equals(range, that.range);
  }

  private boolean equals(Object[] columns) {
    if (this.columns == columns) {
      return true;
    }

    if (columns == null || this.columns == null || columns.length != this.columns.length) {
      return false;
    }

    for (int i = 0; i < columns.length; i++) {
      if (dataTypes[i] != null) {
        switch (dataTypes[i]) {
          case INT32:
          case DATE:
            if (!Arrays.equals((int[]) this.columns[i], (int[]) columns[i])) {
              return false;
            }
            break;
          case INT64:
          case TIMESTAMP:
            if (!Arrays.equals((long[]) this.columns[i], (long[]) columns[i])) {
              return false;
            }
            break;
          case FLOAT:
            if (!Arrays.equals((float[]) this.columns[i], (float[]) columns[i])) {
              return false;
            }
            break;
          case DOUBLE:
            if (!Arrays.equals((double[]) this.columns[i], (double[]) columns[i])) {
              return false;
            }
            break;
          case BOOLEAN:
            if (!Arrays.equals((boolean[]) this.columns[i], (boolean[]) columns[i])) {
              return false;
            }
            break;
          case TEXT:
          case BLOB:
          case STRING:
            if (!Arrays.equals((Binary[]) this.columns[i], (Binary[]) columns[i])) {
              return false;
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(DATATYPE_UNSUPPORTED, dataTypes[i]));
        }
      } else if (!columns[i].equals(columns)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  public TimeValuePair composeLastTimeValuePair(int measurementIndex) {
    if (measurementIndex >= columns.length) {
      return null;
    }

    // get non-null value
    int lastIdx = rowCount - 1;
    if (bitMaps != null && bitMaps[measurementIndex] != null) {
      BitMap bitMap = bitMaps[measurementIndex];
      while (lastIdx >= 0) {
        if (!bitMap.isMarked(lastIdx)) {
          break;
        }
        lastIdx--;
      }
    }
    if (lastIdx < 0) {
      return null;
    }

    TsPrimitiveType value;
    switch (dataTypes[measurementIndex]) {
      case INT32:
      case DATE:
        int[] intValues = (int[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsInt(intValues[lastIdx]);
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsLong(longValues[lastIdx]);
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsFloat(floatValues[lastIdx]);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsDouble(doubleValues[lastIdx]);
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsBoolean(boolValues[lastIdx]);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsBinary(binaryValues[lastIdx]);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[measurementIndex]));
    }
    return new TimeValuePair(times[lastIdx], value);
  }

  public IDeviceID getDeviceID(int rowIdx) {
    if (deviceID != null) {
      return deviceID;
    }
    deviceID = DeviceIDFactory.getInstance().getDeviceID(targetPath);
    return deviceID;
  }

  private static class PartitionSplitInfo {

    // for each List in split, they are range1.start, range1.end, range2.start, range2.end, ...
    private List<Integer> ranges = new ArrayList<>();
    private List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
    private List<TRegionReplicaSet> replicaSets;
  }

  /**
   * Split the tablet of the given range according to Table deviceID.
   *
   * @param start inclusive
   * @param end exclusive
   * @return each the number in the pair is the end offset of a device
   */
  public List<Pair<IDeviceID, Integer>> splitByDevice(int start, int end) {
    return Collections.singletonList(new Pair<>(getDeviceID(), end));
  }

  /**
   * @param results insertion result of each row
   * @param rowTTLGetter the ttl associated with each row
   * @return the position of the first alive row
   * @throws OutOfTTLException if all rows have expired the TTL
   */
  public int checkTTL(TSStatus[] results, IntToLongFunction rowTTLGetter) throws OutOfTTLException {
    return checkTTLInternal(results, rowTTLGetter, true);
  }

  protected int checkTTLInternal(
      TSStatus[] results, IntToLongFunction rowTTLGetter, boolean breakOnFirstAlive)
      throws OutOfTTLException {

    /*
     * assume that batch has been sorted by client
     */
    int loc = 0;
    long ttl = 0;
    int firstAliveLoc = -1;
    while (loc < getRowCount()) {
      ttl = rowTTLGetter.applyAsLong(loc);
      long currTime = getTimes()[loc];
      // skip points that do not satisfy TTL
      if (!isAlive(currTime, ttl)) {
        results[loc] =
            RpcUtils.getStatus(
                TSStatusCode.OUT_OF_TTL,
                String.format(
                    "Insertion time [%s] is less than ttl time bound [%s]",
                    DateTimeUtils.convertLongToDate(currTime),
                    DateTimeUtils.convertLongToDate(CommonDateTimeUtils.currentTime() - ttl)));
      } else {
        if (firstAliveLoc == -1) {
          firstAliveLoc = loc;
        }
        if (breakOnFirstAlive) {
          break;
        }
      }
      loc++;
    }

    if (firstAliveLoc == -1) {
      // no alive data
      throw new OutOfTTLException(
          getTimes()[getTimes().length - 1], (CommonDateTimeUtils.currentTime() - ttl));
    }
    return firstAliveLoc;
  }
}
