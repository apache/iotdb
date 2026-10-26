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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALReadUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.BitMapUtils;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
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
import java.util.Objects;

public class InsertTabletNode extends InsertNode implements WALEntryValue {

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  private long[] times; // times should be sorted. It is done in the session API.

  private BitMap[] bitMaps;
  private Object[] columns;

  private int rowCount = 0;

  // When this plan is sub-plan split from another InsertTabletNode, this indicates the original
  // positions of values in
  // this plan. For example, if the plan contains 5 timestamps, and range = [1,4,10,12], then it
  // means that the first 3
  // timestamps in this plan are from range[1,4) of the parent plan, and the last 2 timestamps are
  // from range[10,12)
  // of the parent plan.
  // this is usually used to back-propagate exceptions to the parent plan without losing their
  // proper positions.
  private List<Integer> range;

  public InsertTabletNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public InsertNode mergeInsertNode(List<InsertNode> insertNodes) {
    List<Integer> index = new ArrayList<>();
    List<InsertTabletNode> insertTabletNodes = new ArrayList<>();
    for (int i = 0; i < insertNodes.size(); i++) {
      insertTabletNodes.add((InsertTabletNode) insertNodes.get(i));
      index.add(i);
    }
    return new InsertMultiTabletsNode(this.getPlanNodeId(), index, insertTabletNodes);
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
  public List<PlanNode> getChildren() {
    return null;
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
    List<WritePlanNode> result = new ArrayList<>();
    if (times.length == 0) {
      return Collections.emptyList();
    }
    long upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[0]);
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[0]);
    int startLoc = 0; // included

    List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
    // for each List in split, they are range1.start, range1.end, range2.start, range2.end, ...
    List<Integer> ranges = new ArrayList<>();
    for (int i = 1; i < rowCount; i++) { // times are sorted in session API.
      if (times[i] >= upperBoundOfTimePartition) {
        // a new range.
        ranges.add(startLoc); // included
        ranges.add(i); // excluded
        timePartitionSlots.add(timePartitionSlot);
        // next init
        startLoc = i;
        upperBoundOfTimePartition = TimePartitionUtils.getTimePartitionUpperBound(times[i]);
        timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[i]);
      }
    }

    // the final range
    ranges.add(startLoc); // included
    ranges.add(rowCount); // excluded
    timePartitionSlots.add(timePartitionSlot);

    // data region for each time partition
    List<TRegionReplicaSet> dataRegionReplicaSets =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(devicePath.getFullPath(), timePartitionSlots);

    // collect redirectInfo
    analysis.addEndPointToRedirectNodeList(
        dataRegionReplicaSets
            .get(dataRegionReplicaSets.size() - 1)
            .getDataNodeLocations()
            .get(0)
            .getClientRpcEndPoint());

    Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    for (int i = 0; i < dataRegionReplicaSets.size(); i++) {
      List<Integer> sub_ranges =
          splitMap.computeIfAbsent(dataRegionReplicaSets.get(i), x -> new ArrayList<>());
      sub_ranges.add(ranges.get(2 * i));
      sub_ranges.add(ranges.get(2 * i + 1));
    }

    List<Integer> locs;
    for (Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      // generate a new times and values
      locs = entry.getValue();
      // Avoid using system arraycopy when there is no need to split
      if (splitMap.size() == 1 && locs.size() == 2) {
        setRange(locs);
        setDataRegionReplicaSet(entry.getKey());
        result.add(this);
        return result;
      }
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        int count = end - start;
        long[] subTimes = new long[count];
        int destLoc = 0;
        final int columnSize = getColumnArrayLength();
        Object[] values = initTabletValues(columnSize, count, dataTypes);
        BitMap[] bitMaps = initBitmapsForSplit(columnSize, count);
        System.arraycopy(times, start, subTimes, destLoc, end - start);
        for (int k = 0; k < values.length; k++) {
          if (hasColumnForSplit(k)) {
            System.arraycopy(columns[k], start, values[k], destLoc, end - start);
          }
          final BitMap sourceBitMap = getBitMapIfPresent(k);
          if (bitMaps != null && bitMaps[k] != null && sourceBitMap != null) {
            BitMap.copyOfRange(sourceBitMap, start, bitMaps[k], destLoc, end - start);
          }
        }
        InsertTabletNode subNode =
            new InsertTabletNode(
                getPlanNodeId(),
                devicePath,
                isAligned,
                measurements,
                dataTypes,
                measurementSchemas,
                subTimes,
                bitMaps,
                values,
                subTimes.length);
        subNode.setFailedMeasurementNumber(getFailedMeasurementNumber());
        subNode.setRange(locs);
        subNode.setDataRegionReplicaSet(entry.getKey());
        subNode.bitMaps = BitMapUtils.compactBitMaps(subNode.bitMaps, subNode.rowCount);
        result.add(subNode);
      }
    }
    return result;
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

  private Object[] initTabletValues(int columnSize, int rowSize, TSDataType[] dataTypes) {
    Object[] values = new Object[columnSize];
    for (int i = 0; i < values.length; i++) {
      if (dataTypes != null && i < dataTypes.length && dataTypes[i] != null) {
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

  private BitMap[] initBitmaps(int columnSize, int rowSize) {
    BitMap[] bitMaps = new BitMap[columnSize];
    for (int i = 0; i < columnSize; i++) {
      bitMaps[i] = new BitMap(rowSize);
    }
    return bitMaps;
  }

  protected BitMap[] initBitmapsForSplit(int columnSize, int rowSize) {
    if (this.bitMaps == null) {
      return null;
    }

    final int sourceRowCount = rowCount > 0 ? rowCount : times == null ? 0 : times.length;
    final BitMap[] splitBitMaps = new BitMap[columnSize];
    boolean hasBitMap = false;
    for (int i = 0; i < columnSize && i < this.bitMaps.length; ++i) {
      if (hasColumnForSplit(i) && this.bitMaps[i] != null && !this.bitMaps[i].isAllUnmarked()) {
        splitBitMaps[i] = new BitMap(rowSize);
        hasBitMap = true;
      }
    }
    return hasBitMap ? splitBitMaps : null;
  }

  private int getColumnArrayLength() {
    int length = 0;
    if (measurements != null) {
      length = Math.max(length, measurements.length);
    }
    if (measurementSchemas != null) {
      length = Math.max(length, measurementSchemas.length);
    }
    if (dataTypes != null) {
      length = Math.max(length, dataTypes.length);
    }
    if (columns != null) {
      length = Math.max(length, columns.length);
    }
    if (bitMaps != null) {
      length = Math.max(length, bitMaps.length);
    }
    return length;
  }

  private boolean hasColumnForSplit(int index) {
    return dataTypes != null
        && index >= 0
        && index < dataTypes.length
        && dataTypes[index] != null
        && columns != null
        && index < columns.length
        && columns[index] != null
        && (measurements == null || index < measurements.length && measurements[index] != null)
        && (measurementSchemas == null
            || index < measurementSchemas.length && measurementSchemas[index] != null);
  }

  private BitMap getBitMapIfPresent(final int index) {
    return bitMaps != null && index >= 0 && index < bitMaps.length ? bitMaps[index] : null;
  }

  @Override
  public void markFailedMeasurement(int index) {
    if (measurements == null
        || index < 0
        || index >= measurements.length
        || measurements[index] == null) {
      return;
    }
    measurements[index] = null;
    if (dataTypes != null && index < dataTypes.length) {
      dataTypes[index] = null;
    }
    if (columns != null && index < columns.length) {
      columns[index] = null;
    }
  }

  @Override
  public long getMinTime() {
    return times[0];
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_TABLET.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INSERT_TABLET.serialize(stream);
    subSerialize(stream);
  }

  void subSerialize(ByteBuffer buffer) {
    serializeString(devicePath.getFullPath(), buffer);
    writeMeasurementsOrSchemas(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer);
    writeBitMaps(buffer);
    writeValues(buffer);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), buffer);
  }

  void subSerialize(DataOutputStream stream) throws IOException {
    serializeString(devicePath.getFullPath(), stream);
    writeMeasurementsOrSchemas(stream);
    writeDataTypes(stream);
    writeTimes(stream);
    writeBitMaps(stream);
    writeValues(stream);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), stream);
  }

  /** Serialize measurements or measurement schemas, ignoring failed time series */
  private void writeMeasurementsOrSchemas(ByteBuffer buffer) {
    ReadWriteIOUtils.write(getValidMeasurementNumber(), buffer);
    ReadWriteIOUtils.write((byte) (measurementSchemas != null ? 1 : 0), buffer);

    for (int i = 0; measurements != null && i < measurements.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurement(i)) {
        continue;
      }
      // serialize measurement schemas when exist
      if (measurementSchemas != null) {
        serializeMeasurementSchema(measurementSchemas[i], buffer);
      } else {
        serializeString(measurements[i], buffer);
      }
    }
  }

  /** Serialize measurements or measurement schemas, ignoring failed time series */
  private void writeMeasurementsOrSchemas(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getValidMeasurementNumber(), stream);
    ReadWriteIOUtils.write((byte) (measurementSchemas != null ? 1 : 0), stream);

    for (int i = 0; measurements != null && i < measurements.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurement(i)) {
        continue;
      }
      // serialize measurement schemas when exist
      if (measurementSchemas != null) {
        serializeMeasurementSchema(measurementSchemas[i], stream);
      } else {
        serializeString(measurements[i], stream);
      }
    }
  }

  /** Serialize data types, ignoring failed time series */
  private void writeDataTypes(ByteBuffer buffer) {
    for (int i = 0; dataTypes != null && i < dataTypes.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurement(i)) {
        continue;
      }
      dataTypes[i].serializeTo(buffer);
    }
  }

  /** Serialize data types, ignoring failed time series */
  private void writeDataTypes(DataOutputStream stream) throws IOException {
    for (int i = 0; dataTypes != null && i < dataTypes.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurement(i)) {
        continue;
      }
      dataTypes[i].serializeTo(stream);
    }
  }

  private void writeTimes(final ByteBuffer buffer) {
    ReadWriteIOUtils.write(rowCount, buffer);
    for (int i = 0; i < rowCount; ++i) {
      ReadWriteIOUtils.write(times[i], buffer);
    }
  }

  private void writeTimes(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(rowCount, stream);
    for (int i = 0; i < rowCount; ++i) {
      ReadWriteIOUtils.write(times[i], stream);
    }
  }

  /** Serialize bitmaps, ignoring failed time series */
  private void writeBitMaps(ByteBuffer buffer) {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(bitMaps != null), buffer);
    if (bitMaps != null) {
      for (int i = 0; measurements != null && i < measurements.length; i++) {
        // ignore failed partial insert and incomplete columns
        if (!shouldSerializeMeasurement(i)) {
          continue;
        }

        final BitMap bitMap = getBitMapIfPresent(i);
        if (bitMap == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), buffer);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), buffer);
          buffer.put(bitMap.getByteArray(), 0, rowCount / 8 + 1);
        }
      }
    }
  }

  /** Serialize bitmaps, ignoring failed time series */
  private void writeBitMaps(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(bitMaps != null), stream);
    if (bitMaps != null) {
      for (int i = 0; measurements != null && i < measurements.length; i++) {
        // ignore failed partial insert and incomplete columns
        if (!shouldSerializeMeasurement(i)) {
          continue;
        }

        final BitMap bitMap = getBitMapIfPresent(i);
        if (bitMap == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), stream);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), stream);
          stream.write(bitMap.getByteArray(), 0, rowCount / 8 + 1);
        }
      }
    }
  }

  /** Serialize values, ignoring failed time series */
  private void writeValues(ByteBuffer buffer) {
    for (int i = 0; columns != null && i < columns.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurement(i)) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer);
    }
  }

  /** Serialize values, ignoring failed time series */
  private void writeValues(DataOutputStream stream) throws IOException {
    for (int i = 0; columns != null && i < columns.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurement(i)) {
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
          if (binaryValues[j] != null && binaryValues[j].getValues() != null) {
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
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = 0; j < rowCount; j++) {
          if (binaryValues[j] != null && binaryValues[j].getValues() != null) {
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
      devicePath =
          DataNodeDevicePathCache.getInstance().getPartialPath((deserializeString(buffer)));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertTabletNode", e);
    }

    int measurementSize = buffer.getInt();
    measurements = new String[measurementSize];

    boolean hasSchema = buffer.get() == 1;
    if (hasSchema) {
      this.measurementSchemas = new MeasurementSchema[measurementSize];
      for (int i = 0; i < measurementSize; i++) {
        measurementSchemas[i] = deserializeMeasurementSchema(buffer);
        measurements[i] = measurementSchemas[i].getMeasurementId();
      }
    } else {
      for (int i = 0; i < measurementSize; i++) {
        measurements[i] = deserializeString(buffer);
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
    size += WALWriteUtils.sizeToWrite(devicePath.getFullPath());
    // measurements size
    size += Integer.BYTES;
    size += serializeMeasurementSchemasSize();
    // times size
    size += Integer.BYTES;
    size += Long.BYTES * (end - start);
    // bitmaps size
    size += Byte.BYTES;
    if (bitMaps != null) {
      for (int i = 0; measurements != null && i < measurements.length; i++) {
        // ignore failed partial insert and incomplete columns
        if (!shouldSerializeMeasurementToWAL(i)) {
          continue;
        }

        size += Byte.BYTES;
        final BitMap bitMap = getBitMapIfPresent(i);
        if (bitMap != null) {
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
          size += partBitMap.getByteArray().length;
        }
      }
    }
    // values size
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      if (shouldSerializeMeasurementToWAL(i)) {
        size += getColumnSize(dataTypes[i], columns[i], start, end);
      }
    }

    size += Byte.BYTES;
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
    buffer.putShort(PlanNodeType.INSERT_TABLET.getNodeType());
    subSerialize(buffer, start, end);
  }

  void subSerialize(IWALByteBufferView buffer, int start, int end) {
    buffer.putLong(searchIndex);
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    // data types are serialized in measurement schemas
    writeMeasurementSchemas(buffer);
    writeTimes(buffer, start, end);
    writeBitMaps(buffer, start, end);
    writeValues(buffer, start, end);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  /** Serialize measurement schemas, ignoring failed time series */
  private void writeMeasurementSchemas(IWALByteBufferView buffer) {
    buffer.putInt(getValidMeasurementNumberForWAL());
    serializeMeasurementSchemasToWAL(buffer);
  }

  private void writeTimes(IWALByteBufferView buffer, int start, int end) {
    buffer.putInt(end - start);
    for (int i = start; i < end; i++) {
      buffer.putLong(times[i]);
    }
  }

  /** Serialize bitmaps, ignoring failed time series */
  private void writeBitMaps(IWALByteBufferView buffer, int start, int end) {
    buffer.put(BytesUtils.boolToByte(bitMaps != null));
    if (bitMaps != null) {
      for (int i = 0; measurements != null && i < measurements.length; i++) {
        // ignore failed partial insert and incomplete columns
        if (!shouldSerializeMeasurementToWAL(i)) {
          continue;
        }

        final BitMap bitMap = getBitMapIfPresent(i);
        if (bitMap == null) {
          buffer.put(BytesUtils.boolToByte(false));
        } else {
          buffer.put(BytesUtils.boolToByte(true));
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
          buffer.put(partBitMap.getByteArray());
        }
      }
    }
  }

  /** Serialize values, ignoring failed time series */
  private void writeValues(IWALByteBufferView buffer, int start, int end) {
    for (int i = 0; measurements != null && i < measurements.length; i++) {
      // ignore failed partial insert and incomplete columns
      if (!shouldSerializeMeasurementToWAL(i)) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  @Override
  protected boolean shouldSerializeMeasurement(final int index) {
    return measurements != null
        && index >= 0
        && index < measurements.length
        && measurements[index] != null
        && (measurementSchemas == null
            || index < measurementSchemas.length && measurementSchemas[index] != null)
        && dataTypes != null
        && index < dataTypes.length
        && dataTypes[index] != null
        && columns != null
        && index < columns.length
        && columns[index] != null;
  }

  @Override
  protected boolean shouldSerializeMeasurementToWAL(final int index) {
    return shouldSerializeMeasurement(index)
        && measurementSchemas != null
        && index < measurementSchemas.length
        && measurementSchemas[index] != null;
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
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          if (binaryValues[j] != null && binaryValues[j].getValues() != null) {
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

  private void subDeserializeFromWAL(DataInputStream stream) throws IOException {
    searchIndex = stream.readLong();
    try {
      devicePath =
          DataNodeDevicePathCache.getInstance().getPartialPath(WALReadUtils.readString(stream));
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

  private void subDeserializeFromWAL(ByteBuffer buffer) {
    searchIndex = buffer.getLong();
    try {
      devicePath =
          DataNodeDevicePathCache.getInstance().getPartialPath((WALReadUtils.readString(buffer)));
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
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
      final TSDataType dataType = getDataType(i);
      if (dataType != null) {
        switch (dataType) {
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
            throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
        }
      } else if (!Objects.equals(this.columns[i], columns[i])) {
        return false;
      }
    }

    return true;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  public TimeValuePair composeLastTimeValuePair(
      int measurementIndex, int startOffset, int endOffset) {
    if (!canComposeLastTimeValuePair(measurementIndex)) {
      return null;
    }

    // get non-null value
    int lastIdx = Math.min(endOffset - 1, rowCount - 1);
    final BitMap bitMap = getBitMapIfPresent(measurementIndex);
    if (bitMap != null) {
      while (lastIdx >= startOffset) {
        if (!bitMap.isMarked(lastIdx)) {
          break;
        }
        lastIdx--;
      }
    }
    if (lastIdx < startOffset) {
      return null;
    }
    return composeTimeValuePair(measurementIndex, lastIdx);
  }

  protected TimeValuePair composeLastTimeValuePair(
      final int measurementIndex,
      final TSStatus[] results,
      final int startOffset,
      final int endOffset) {
    if (results == null) {
      return composeLastTimeValuePair(measurementIndex, startOffset, endOffset);
    }
    if (!canComposeLastTimeValuePair(measurementIndex)) {
      return null;
    }

    final BitMap bitMap = getBitMapIfPresent(measurementIndex);
    int lastIdx = Math.min(endOffset - 1, rowCount - 1);
    while (lastIdx >= startOffset) {
      if (results[lastIdx] != null
          && results[lastIdx].getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        lastIdx--;
        continue;
      }
      if (bitMap != null && bitMap.isMarked(lastIdx)) {
        lastIdx--;
        continue;
      }
      break;
    }
    return lastIdx < startOffset ? null : composeTimeValuePair(measurementIndex, lastIdx);
  }

  private boolean canComposeLastTimeValuePair(final int measurementIndex) {
    return measurements != null
        && measurementIndex >= 0
        && measurementIndex < measurements.length
        && measurements[measurementIndex] != null
        && columns != null
        && measurementIndex < columns.length
        && columns[measurementIndex] != null
        && dataTypes != null
        && measurementIndex < dataTypes.length
        && dataTypes[measurementIndex] != null;
  }

  private TimeValuePair composeTimeValuePair(final int measurementIndex, final int rowIndex) {
    TsPrimitiveType value;
    switch (dataTypes[measurementIndex]) {
      case INT32:
      case DATE:
        int[] intValues = (int[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsInt(intValues[rowIndex]);
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsLong(longValues[rowIndex]);
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsFloat(floatValues[rowIndex]);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsDouble(doubleValues[rowIndex]);
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsBoolean(boolValues[rowIndex]);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) columns[measurementIndex];
        value = new TsPrimitiveType.TsBinary(binaryValues[rowIndex]);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[measurementIndex]));
    }
    return new TimeValuePair(times[rowIndex], value);
  }

  public void updateLastCache(final String databaseName) {
    updateLastCache(databaseName, null);
  }

  public void updateLastCache(final String databaseName, final TSStatus[] results) {
    final TimeValuePair[] timeValuePairs = new TimeValuePair[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      timeValuePairs[i] = composeLastTimeValuePair(i, results, 0, rowCount);
    }
    DataNodeSchemaCache.getInstance()
        .updateLastCacheIfExists(
            databaseName, devicePath, measurements, timeValuePairs, isAligned, measurementSchemas);
  }
}
