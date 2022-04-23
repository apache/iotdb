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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
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

  // when this plan is sub-plan split from another InsertTabletPlan, this indicates the original
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

  public InsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      MeasurementSchema[] measurementSchemas,
      TSDataType[] dataTypes,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns,
      int rowCount) {
    super(id, devicePath, isAligned, measurementSchemas, dataTypes);
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
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return null;
  }

  @Override
  public int serializedSize() {
    return serializedSize(0, rowCount);
  }

  public int serializedSize(int start, int end) {
    int size = 0;
    size += Short.BYTES;
    return size + subSerializeSize(start, end);
  }

  int subSerializeSize(int start, int end) {
    int size = 0;
    size += ReadWriteIOUtils.sizeToWrite(devicePath.getFullPath());
    // measurements size
    size += Integer.BYTES;

    size += serializeMeasurementSchemaSize();

    // data types size
    size += Integer.BYTES;
    for (int i = 0; i < dataTypes.length; i++) {
      if (measurements[i] != null) {
        size += Byte.BYTES;
      }
    }
    // times size
    size += Integer.BYTES;
    size += Long.BYTES * (end - start);
    // bitmaps size
    size += Byte.BYTES;
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        size += Byte.BYTES;
        if (bitMap != null) {
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
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
    size += Long.BYTES;
    size += Byte.BYTES;
    return size;
  }

  private int getColumnSize(TSDataType dataType, Object column, int start, int end) {
    int size = 0;
    switch (dataType) {
      case INT32:
        size += Integer.BYTES * (end - start);
        break;
      case INT64:
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
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          size += ReadWriteIOUtils.sizeToWrite(binaryValues[j]);
        }
        break;
    }
    return size;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_TABLET.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  void subSerialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(devicePath.getFullPath(), buffer);
    writeMeasurements(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer);
    writeBitMaps(buffer);
    writeValues(buffer);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurements(ByteBuffer buffer) {
    buffer.putInt(measurementSchemas.length - countFailedMeasurements());
    for (MeasurementSchema measurement : measurementSchemas) {
      if (measurement != null) {
        measurement.serializeTo(buffer);
      }
    }
  }

  private void writeDataTypes(ByteBuffer buffer) {
    for (TSDataType dataType : dataTypes) {
      if (dataType == null) {
        continue;
      }
      dataType.serializeTo(buffer);
    }
  }

  private void writeTimes(ByteBuffer buffer) {
    buffer.putInt(rowCount);
    for (long time : times) {
      buffer.putLong(time);
    }
  }

  private void writeBitMaps(ByteBuffer buffer) {
    buffer.put(BytesUtils.boolToByte(bitMaps != null));
    if (bitMaps != null) {
      for (int i = 0; i < measurements.length; i++) {
        // check failed measurement
        if (measurements[i] != null) {
          BitMap bitMap = bitMaps[i];
          if (bitMap == null) {
            buffer.put(BytesUtils.boolToByte(false));
          } else {
            buffer.put(BytesUtils.boolToByte(true));
            buffer.put(bitMap.getByteArray());
          }
        }
      }
    }
  }

  private void writeValues(ByteBuffer buffer) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer);
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, ByteBuffer buffer) {
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = 0; j < rowCount; j++) {
          buffer.putInt(intValues[j]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for (int j = 0; j < rowCount; j++) {
          buffer.putLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = 0; j < rowCount; j++) {
          buffer.putFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = 0; j < rowCount; j++) {
          buffer.putDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = 0; j < rowCount; j++) {
          buffer.put(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = 0; j < rowCount; j++) {
          buffer.putInt(binaryValues[j].getLength());
          buffer.put(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, 0, rowCount);
  }

  public void serializeToWAL(IWALByteBufferView buffer, int start, int end) {
    buffer.putShort((short) PlanNodeType.INSERT_TABLET.ordinal());
    subSerialize(buffer, start, end);
  }

  void subSerialize(IWALByteBufferView buffer, int start, int end) {
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    writeMeasurements(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer, start, end);
    writeBitMaps(buffer, start, end);
    writeValues(buffer, start, end);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurements(IWALByteBufferView buffer) {
    buffer.putInt(measurementSchemas.length - countFailedMeasurements());
    serializeMeasurementSchemaToWAL(buffer);
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
      if (dataTypes[i] != null) {
        switch (dataTypes[i]) {
          case INT32:
            if (!Arrays.equals((int[]) this.columns[i], (int[]) columns[i])) {
              return false;
            }
            break;
          case INT64:
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
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), rowCount, range);
    result = 31 * result + Arrays.hashCode(times);
    result = 31 * result + Arrays.hashCode(bitMaps);
    result = 31 * result + Arrays.hashCode(columns);
    return result;
  }

  private void writeDataTypes(IWALByteBufferView buffer) {
    for (TSDataType dataType : dataTypes) {
      if (dataType == null) {
        continue;
      }
      WALWriteUtils.write(dataType, buffer);
    }
  }

  private void writeTimes(IWALByteBufferView buffer, int start, int end) {
    buffer.putInt(end - start);
    for (int i = start; i < end; i++) {
      buffer.putLong(times[i]);
    }
  }

  private void writeBitMaps(IWALByteBufferView buffer, int start, int end) {
    buffer.put(BytesUtils.boolToByte(bitMaps != null));
    if (bitMaps != null) {
      for (int i = 0; i < measurements.length; i++) {
        // check failed measurement
        if (measurements[i] != null) {
          BitMap bitMap = bitMaps[i];
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
  }

  private void writeValues(IWALByteBufferView buffer, int start, int end) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  private void serializeColumn(
      TSDataType dataType, Object column, IWALByteBufferView buffer, int start, int end) {
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(intValues[j]);
        }
        break;
      case INT64:
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
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(binaryValues[j].getLength());
          buffer.put(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    // only single device in single storage group
    List<WritePlanNode> result = new ArrayList<>();
    if (times.length == 0) {
      return Collections.emptyList();
    }
    long startTime =
        (times[0] / StorageEngine.getTimePartitionInterval())
            * StorageEngine.getTimePartitionInterval(); // included
    long endTime = startTime + StorageEngine.getTimePartitionInterval(); // excluded
    TTimePartitionSlot timePartitionSlot = StorageEngine.getTimePartitionSlot(times[0]);
    int startLoc = 0; // included

    List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
    // for each List in split, they are range1.start, range1.end, range2.start, range2.end, ...
    List<Integer> ranges = new ArrayList<>();
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        // a new range.
        ranges.add(startLoc); // included
        ranges.add(i); // excluded
        timePartitionSlots.add(timePartitionSlot);
        // next init
        startLoc = i;
        startTime = endTime;
        endTime =
            (times[i] / StorageEngine.getTimePartitionInterval() + 1)
                * StorageEngine.getTimePartitionInterval();
        timePartitionSlot = StorageEngine.getTimePartitionSlot(times[i]);
      }
    }

    // the final range
    ranges.add(startLoc); // included
    ranges.add(times.length); // excluded
    timePartitionSlots.add(timePartitionSlot);

    // data region for each time partition
    List<TRegionReplicaSet> dataRegionReplicaSets =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(devicePath.getFullPath(), timePartitionSlots);

    Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    for (int i = 0; i < dataRegionReplicaSets.size(); i++) {
      List<Integer> sub_ranges =
          splitMap.computeIfAbsent(dataRegionReplicaSets.get(i), x -> new ArrayList<>());
      sub_ranges.add(ranges.get(i));
      sub_ranges.add(ranges.get(i + 1));
    }

    List<Integer> locs;
    for (Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      // generate a new times and values
      locs = entry.getValue();
      int count = 0;
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        count += end - start;
      }
      long[] subTimes = new long[count];
      int destLoc = 0;
      Object[] values = initTabletValues(dataTypes.length, count, dataTypes);
      BitMap[] bitMaps = this.bitMaps == null ? null : initBitmaps(dataTypes.length, count);
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        System.arraycopy(times, start, subTimes, destLoc, end - start);
        for (int k = 0; k < values.length; k++) {
          System.arraycopy(columns[k], start, values[k], destLoc, end - start);
          if (bitMaps != null && this.bitMaps[k] != null) {
            BitMap.copyOfRange(this.bitMaps[k], start, bitMaps[k], destLoc, end - start);
          }
        }
        destLoc += end - start;
      }
      InsertTabletNode subNode =
          new InsertTabletNode(
              getPlanNodeId(),
              devicePath,
              isAligned,
              measurementSchemas,
              dataTypes,
              subTimes,
              bitMaps,
              values,
              subTimes.length);
      subNode.setRange(locs);
      subNode.setDataRegionReplicaSet(entry.getKey());
      result.add(subNode);
    }
    return result;
  }

  private Object[] initTabletValues(int columnSize, int rowSize, TSDataType[] dataTypes) {
    Object[] values = new Object[columnSize];
    for (int i = 0; i < values.length; i++) {
      switch (dataTypes[i]) {
        case TEXT:
          values[i] = new Binary[rowSize];
          break;
        case FLOAT:
          values[i] = new float[rowSize];
          break;
        case INT32:
          values[i] = new int[rowSize];
          break;
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
    return values;
  }

  private BitMap[] initBitmaps(int columnSize, int rowSize) {
    BitMap[] bitMaps = new BitMap[columnSize];
    for (int i = 0; i < columnSize; i++) {
      bitMaps[i] = new BitMap(rowSize);
    }
    return bitMaps;
  }

  public static InsertTabletNode deserialize(ByteBuffer byteBuffer) {
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    try {
      insertNode.subDeserialize(byteBuffer);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertRowNode", e);
    }
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  private void subDeserialize(ByteBuffer buffer) throws IllegalPathException {
    this.devicePath = new PartialPath(ReadWriteIOUtils.readString(buffer));

    int measurementSize = buffer.getInt();
    this.measurements = new String[measurementSize];
    this.measurementSchemas = new MeasurementSchema[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
      measurements[i] = measurementSchemas[i].getMeasurementId();
    }

    this.dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = TSDataType.deserialize(buffer.get());
    }

    int rows = buffer.getInt();
    rowCount = rows;
    this.times = new long[rows];
    times = QueryDataSetUtils.readTimesFromBuffer(buffer, rows);

    boolean hasBitMaps = BytesUtils.byteToBool(buffer.get());
    if (hasBitMaps) {
      bitMaps = QueryDataSetUtils.readBitMapsFromBuffer(buffer, measurementSize, rows);
    }
    columns =
        QueryDataSetUtils.readTabletValuesFromBuffer(buffer, dataTypes, measurementSize, rows);
    this.isAligned = buffer.get() == 1;
  }

  public static InsertTabletNode deserialize(DataInputStream stream)
      throws IllegalPathException, IOException {
    // This method is used for deserialize from wal
    // we do not store plan node id in wal entry
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserialize(stream);
    return insertNode;
  }

  private void subDeserialize(DataInputStream stream) throws IllegalPathException, IOException {
    this.devicePath = new PartialPath(ReadWriteIOUtils.readString(stream));

    int measurementSize = stream.readInt();
    this.measurements = new String[measurementSize];
    this.measurementSchemas = new MeasurementSchema[measurementSize];
    deserializeMeasurementSchema(stream);

    this.dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = TSDataType.deserialize(stream.readByte());
    }

    int rows = stream.readInt();
    rowCount = rows;
    this.times = new long[rows];
    times = QueryDataSetUtils.readTimesFromStream(stream, rows);

    boolean hasBitMaps = BytesUtils.byteToBool(stream.readByte());
    if (hasBitMaps) {
      bitMaps = QueryDataSetUtils.readBitMapsFromStream(stream, measurementSize, rows);
    }
    columns =
        QueryDataSetUtils.readTabletValuesFromStream(stream, dataTypes, measurementSize, rows);
    this.isAligned = stream.readByte() == 1;
  }
}
