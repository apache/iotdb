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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
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
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public boolean validateAndSetSchema(SchemaTree schemaTree) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));
    if (deviceSchemaInfo.isAligned() != isAligned) {
      return false;
    }
    measurementSchemas =
        deviceSchemaInfo.getMeasurementSchemaList().toArray(new MeasurementSchema[0]);

    // validate whether data types are matched
    return selfCheckDataTypes();
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    // only single device in single storage group
    List<WritePlanNode> result = new ArrayList<>();
    if (times.length == 0) {
      return Collections.emptyList();
    }
    long startTime =
        (times[0] / StorageEngineV2.getTimePartitionInterval())
            * StorageEngineV2.getTimePartitionInterval(); // included
    long endTime = startTime + StorageEngineV2.getTimePartitionInterval(); // excluded
    TTimePartitionSlot timePartitionSlot = StorageEngineV2.getTimePartitionSlot(times[0]);
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
            (times[i] / StorageEngineV2.getTimePartitionInterval() + 1)
                * StorageEngineV2.getTimePartitionInterval();
        timePartitionSlot = StorageEngineV2.getTimePartitionSlot(times[i]);
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
      sub_ranges.add(ranges.get(2 * i));
      sub_ranges.add(ranges.get(2 * i + 1));
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
              measurements,
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

  @TestOnly
  public List<TTimePartitionSlot> getTimePartitionSlots() {
    List<TTimePartitionSlot> result = new ArrayList<>();
    long startTime =
        (times[0] / StorageEngineV2.getTimePartitionInterval())
            * StorageEngineV2.getTimePartitionInterval(); // included
    long endTime = startTime + StorageEngineV2.getTimePartitionInterval(); // excluded
    TTimePartitionSlot timePartitionSlot = StorageEngineV2.getTimePartitionSlot(times[0]);
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        result.add(timePartitionSlot);
        // next init
        endTime =
            (times[i] / StorageEngineV2.getTimePartitionInterval() + 1)
                * StorageEngineV2.getTimePartitionInterval();
        timePartitionSlot = StorageEngineV2.getTimePartitionSlot(times[i]);
      }
    }
    result.add(timePartitionSlot);
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

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    if (measurements[index] == null) {
      return;
    }

    if (failedMeasurementIndex2Info == null) {
      failedMeasurementIndex2Info = new HashMap<>();
    }

    FailedMeasurementInfo failedMeasurementInfo =
        new FailedMeasurementInfo(measurements[index], dataTypes[index], columns[index], cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    dataTypes[index] = null;
    columns[index] = null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_TABLET.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  void subSerialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(devicePath.getFullPath(), buffer);
    writeMeasurementsOrSchemas(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer);
    writeBitMaps(buffer);
    writeValues(buffer);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurementsOrSchemas(ByteBuffer buffer) {
    buffer.putInt(measurements.length - getFailedMeasurementNumber());

    buffer.put((byte) (measurementSchemas != null ? 1 : 0));
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

  private void writeDataTypes(ByteBuffer buffer) {
    for (int i = 0; i < dataTypes.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      dataTypes[i].serializeTo(buffer);
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
      for (int i = 0; i < bitMaps.length; i++) {
        // ignore failed partial insert
        if (measurements[i] == null) {
          continue;
        }

        if (bitMaps[i] == null) {
          buffer.put(BytesUtils.boolToByte(false));
        } else {
          buffer.put(BytesUtils.boolToByte(true));
          buffer.put(bitMaps[i].getByteArray());
        }
      }
    }
  }

  private void writeValues(ByteBuffer buffer) {
    for (int i = 0; i < columns.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
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

  public static InsertTabletNode deserialize(ByteBuffer byteBuffer) {
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  public void subDeserialize(ByteBuffer buffer) {
    try {
      devicePath = new PartialPath(ReadWriteIOUtils.readString(buffer));
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
      bitMaps = QueryDataSetUtils.readBitMapsFromBuffer(buffer, measurementSize, rowCount);
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
    size += ReadWriteIOUtils.sizeToWrite(devicePath.getFullPath());
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
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, 0, rowCount);
  }

  public void serializeToWAL(IWALByteBufferView buffer, int start, int end) {
    buffer.putShort((short) PlanNodeType.INSERT_TABLET.ordinal());
    subSerialize(buffer, start, end);
  }

  void subSerialize(IWALByteBufferView buffer, int start, int end) {
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    // data types are serialized in measurement schemas
    writeMeasurementSchemas(buffer);
    writeTimes(buffer, start, end);
    writeBitMaps(buffer, start, end);
    writeValues(buffer, start, end);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurementSchemas(IWALByteBufferView buffer) {
    buffer.putInt(measurements.length - getFailedMeasurementNumber());
    serializeMeasurementSchemasToWAL(buffer);
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

  private void writeValues(IWALByteBufferView buffer, int start, int end) {
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

  /** Deserialize from wal */
  public static InsertTabletNode deserialize(DataInputStream stream)
      throws IllegalPathException, IOException {
    // we do not store plan node id in wal entry
    InsertTabletNode insertNode = new InsertTabletNode(new PlanNodeId(""));
    insertNode.subDeserialize(stream);
    return insertNode;
  }

  private void subDeserialize(DataInputStream stream) throws IllegalPathException, IOException {
    devicePath = new PartialPath(ReadWriteIOUtils.readString(stream));

    int measurementSize = stream.readInt();
    measurements = new String[measurementSize];
    measurementSchemas = new MeasurementSchema[measurementSize];
    deserializeMeasurementSchemas(stream);

    // data types are serialized in measurement schemas
    dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = measurementSchemas[i].getType();
    }

    rowCount = stream.readInt();
    times = new long[rowCount];
    times = QueryDataSetUtils.readTimesFromStream(stream, rowCount);

    boolean hasBitMaps = BytesUtils.byteToBool(stream.readByte());
    if (hasBitMaps) {
      bitMaps = QueryDataSetUtils.readBitMapsFromStream(stream, measurementSize, rowCount);
    }
    columns =
        QueryDataSetUtils.readTabletValuesFromStream(stream, dataTypes, measurementSize, rowCount);
    isAligned = stream.readByte() == 1;
  }
  // endregion

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), rowCount, range);
    result = 31 * result + Arrays.hashCode(times);
    result = 31 * result + Arrays.hashCode(bitMaps);
    result = 31 * result + Arrays.hashCode(columns);
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
}
