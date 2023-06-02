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
package org.apache.iotdb.tsfile.write.record;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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

/**
 * A tablet data of one device, the tablet contains multiple measurements of this device that share
 * the same time column.
 *
 * <p>for example: device root.sg1.d1
 *
 * <p>time, m1, m2, m3 1, 1, 2, 3 2, 1, 2, 3 3, 1, 2, 3
 *
 * <p>Notice: The tablet should not have empty cell, please use BitMap to denote null value
 */
public class Tablet {

  private static final int DEFAULT_SIZE = 1024;
  private static final String NOT_SUPPORT_DATATYPE = "Data type %s is not supported.";

  /** deviceId of this tablet */
  public String deviceId;

  /** the list of measurement schemas for creating the tablet */
  private List<MeasurementSchema> schemas;

  /** measurementId->indexOf(measurementSchema) */
  private final Map<String, Integer> measurementIndex;

  /** timestamps in this tablet */
  public long[] timestamps;
  /** each object is a primitive type array, which represents values of one measurement */
  public Object[] values;
  /** each bitmap represents the existence of each value in the current column. */
  public BitMap[] bitMaps;
  /** the number of rows to include in this tablet */
  public int rowSize;
  /** the maximum number of rows for this tablet */
  private final int maxRowNumber;

  /**
   * Return a tablet with default specified row number. This is the standard constructor (all Tablet
   * should be the same size).
   *
   * @param deviceId the name of the device specified to be written in
   * @param schemas the list of measurement schemas for creating the tablet, only measurementId and
   *     type take effects
   */
  public Tablet(String deviceId, List<MeasurementSchema> schemas) {
    this(deviceId, schemas, DEFAULT_SIZE);
  }

  /**
   * Return a tablet with the specified number of rows (maxBatchSize). Only call this constructor
   * directly for testing purposes. Tablet should normally always be default size.
   *
   * @param deviceId the name of the device specified to be written in
   * @param schemas the list of measurement schemas for creating the row batch, only measurementId
   *     and type take effects
   * @param maxRowNumber the maximum number of rows for this tablet
   */
  public Tablet(String deviceId, List<MeasurementSchema> schemas, int maxRowNumber) {
    this.deviceId = deviceId;
    this.schemas = new ArrayList<>(schemas);
    this.maxRowNumber = maxRowNumber;
    measurementIndex = new HashMap<>();
    constructMeasurementIndexMap();

    createColumns();

    reset();
  }

  /**
   * Return a tablet with specified timestamps and values. Only call this constructor directly for
   * Trigger.
   *
   * @param deviceId the name of the device specified to be written in
   * @param schemas the list of measurement schemas for creating the row batch, only measurementId
   *     and type take effects
   * @param timestamps given timestamps
   * @param values given values
   * @param bitMaps given bitmaps
   * @param maxRowNumber the maximum number of rows for this tablet
   */
  public Tablet(
      String deviceId,
      List<MeasurementSchema> schemas,
      long[] timestamps,
      Object[] values,
      BitMap[] bitMaps,
      int maxRowNumber) {
    this.deviceId = deviceId;
    this.schemas = schemas;
    this.timestamps = timestamps;
    this.values = values;
    this.bitMaps = bitMaps;
    this.maxRowNumber = maxRowNumber;
    // rowSize == maxRowNumber in this case
    this.rowSize = maxRowNumber;
    measurementIndex = new HashMap<>();
    constructMeasurementIndexMap();
  }

  private void constructMeasurementIndexMap() {
    int indexInSchema = 0;
    for (MeasurementSchema schema : schemas) {
      measurementIndex.put(schema.getMeasurementId(), indexInSchema);
      indexInSchema++;
    }
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public void setSchemas(List<MeasurementSchema> schemas) {
    this.schemas = schemas;
  }

  public void initBitMaps() {
    this.bitMaps = new BitMap[schemas.size()];
    for (int column = 0; column < schemas.size(); column++) {
      this.bitMaps[column] = new BitMap(getMaxRowNumber());
    }
  }

  public void addTimestamp(int rowIndex, long timestamp) {
    timestamps[rowIndex] = timestamp;
  }

  public void addValue(String measurementId, int rowIndex, Object value) {
    int indexOfSchema = measurementIndex.get(measurementId);
    MeasurementSchema measurementSchema = schemas.get(indexOfSchema);
    addValueOfDataType(measurementSchema.getType(), rowIndex, indexOfSchema, value);
  }

  private void addValueOfDataType(
      TSDataType dataType, int rowIndex, int indexOfSchema, Object value) {

    if (value == null) {
      // init the bitMap to mark null value
      if (bitMaps == null) {
        bitMaps = new BitMap[values.length];
      }
      if (bitMaps[indexOfSchema] == null) {
        bitMaps[indexOfSchema] = new BitMap(maxRowNumber);
      }
      // mark the null value position
      bitMaps[indexOfSchema].mark(rowIndex);
    }
    switch (dataType) {
      case TEXT:
        {
          Binary[] sensor = (Binary[]) values[indexOfSchema];
          if (value instanceof Binary) {
            sensor[rowIndex] = (Binary) value;
          } else {
            sensor[rowIndex] = value != null ? new Binary((String) value) : Binary.EMPTY_VALUE;
          }
          break;
        }
      case FLOAT:
        {
          float[] sensor = (float[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (float) value : Float.MIN_VALUE;
          break;
        }
      case INT32:
        {
          int[] sensor = (int[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (int) value : Integer.MIN_VALUE;
          break;
        }
      case INT64:
        {
          long[] sensor = (long[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (long) value : Long.MIN_VALUE;
          break;
        }
      case DOUBLE:
        {
          double[] sensor = (double[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (double) value : Double.MIN_VALUE;
          break;
        }
      case BOOLEAN:
        {
          boolean[] sensor = (boolean[]) values[indexOfSchema];
          sensor[rowIndex] = value != null && (boolean) value;
          break;
        }
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
  }

  public List<MeasurementSchema> getSchemas() {
    return schemas;
  }

  /** Return the maximum number of rows for this tablet */
  public int getMaxRowNumber() {
    return maxRowNumber;
  }

  /** Reset Tablet to the default state - set the rowSize to 0 and reset bitMaps */
  public void reset() {
    rowSize = 0;
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        if (bitMap != null) {
          bitMap.reset();
        }
      }
    }
  }

  private void createColumns() {
    // create timestamp column
    timestamps = new long[maxRowNumber];

    // calculate total value column size
    int valueColumnsSize = schemas.size();

    // value column
    values = new Object[valueColumnsSize];
    int columnIndex = 0;
    for (MeasurementSchema schema : schemas) {
      TSDataType dataType = schema.getType();
      values[columnIndex] = createValueColumnOfDataType(dataType);
      columnIndex++;
    }
  }

  private Object createValueColumnOfDataType(TSDataType dataType) {

    Object valueColumn;
    switch (dataType) {
      case INT32:
        valueColumn = new int[maxRowNumber];
        break;
      case INT64:
        valueColumn = new long[maxRowNumber];
        break;
      case FLOAT:
        valueColumn = new float[maxRowNumber];
        break;
      case DOUBLE:
        valueColumn = new double[maxRowNumber];
        break;
      case BOOLEAN:
        valueColumn = new boolean[maxRowNumber];
        break;
      case TEXT:
        valueColumn = new Binary[maxRowNumber];
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
    return valueColumn;
  }

  public int getTimeBytesSize() {
    return rowSize * 8;
  }

  /** @return total bytes of values */
  public int getTotalValueOccupation() {
    int valueOccupation = 0;
    int columnIndex = 0;
    for (MeasurementSchema schema : schemas) {
      valueOccupation += calOccupationOfOneColumn(schema.getType(), columnIndex);
      columnIndex++;
    }
    // add bitmap size if the tablet has bitMaps
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        // marker byte
        valueOccupation++;
        if (bitMap != null && !bitMap.isAllUnmarked()) {
          valueOccupation += rowSize / Byte.SIZE + 1;
        }
      }
    }
    return valueOccupation;
  }

  private int calOccupationOfOneColumn(TSDataType dataType, int columnIndex) {
    int valueOccupation = 0;
    switch (dataType) {
      case BOOLEAN:
        valueOccupation += rowSize;
        break;
      case INT32:
      case FLOAT:
        valueOccupation += rowSize * 4;
        break;
      case INT64:
      case DOUBLE:
        valueOccupation += rowSize * 8;
        break;
      case TEXT:
        valueOccupation += rowSize * 4;
        Binary[] binaries = (Binary[]) values[columnIndex];
        for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
          valueOccupation += binaries[rowIndex].getLength();
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
    return valueOccupation;
  }

  /** serialize Tablet */
  public ByteBuffer serialize() throws IOException {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(deviceId, stream);
    ReadWriteIOUtils.write(rowSize, stream);
    writeMeasurementSchemas(stream);
    writeTimes(stream);
    writeBitMaps(stream);
    writeValues(stream);
  }

  /** Serialize measurement schemas */
  private void writeMeasurementSchemas(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(schemas != null), stream);
    if (schemas != null) {
      ReadWriteIOUtils.write(schemas.size(), stream);
      for (MeasurementSchema schema : schemas) {
        if (schema == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), stream);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), stream);
          schema.serializeTo(stream);
        }
      }
    }
  }

  private void writeTimes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(timestamps != null), stream);
    if (timestamps != null) {
      for (int i = 0; i < rowSize; i++) {
        ReadWriteIOUtils.write(timestamps[i], stream);
      }
    }
  }

  /** Serialize bitmaps */
  private void writeBitMaps(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(bitMaps != null), stream);
    if (bitMaps != null) {
      int size = (schemas == null ? 0 : schemas.size());
      for (int i = 0; i < size; i++) {
        if (bitMaps[i] == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), stream);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), stream);
          ReadWriteIOUtils.write(new Binary(bitMaps[i].getByteArray()), stream);
        }
      }
    }
  }

  /** Serialize values */
  private void writeValues(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(values != null), stream);
    if (values != null) {
      int size = (schemas == null ? 0 : schemas.size());
      for (int i = 0; i < size; i++) {
        serializeColumn(schemas.get(i).getType(), values[i], stream);
      }
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, DataOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(column != null), stream);

    if (column != null) {
      switch (dataType) {
        case INT32:
          int[] intValues = (int[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(intValues[j], stream);
          }
          break;
        case INT64:
          long[] longValues = (long[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(longValues[j], stream);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(floatValues[j], stream);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(doubleValues[j], stream);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(boolValues[j] ? 1 : 0, stream);
          }
          break;
        case TEXT:
          Binary[] binaryValues = (Binary[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(BytesUtils.boolToByte(binaryValues[j] != null), stream);
            if (binaryValues[j] != null) {
              ReadWriteIOUtils.write(binaryValues[j], stream);
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }
  }

  /** Deserialize Tablet */
  public static Tablet deserialize(ByteBuffer byteBuffer) {
    String deviceId = ReadWriteIOUtils.readString(byteBuffer);
    int rowSize = ReadWriteIOUtils.readInt(byteBuffer);

    // deserialize schemas
    int schemaSize = 0;
    List<MeasurementSchema> schemas = new ArrayList<>();
    boolean isSchemasNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isSchemasNotNull) {
      schemaSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < schemaSize; i++) {
        boolean hasSchema = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
        if (hasSchema) {
          schemas.add(MeasurementSchema.deserializeFrom(byteBuffer));
        }
      }
    }

    // deserialize times
    long[] times = new long[rowSize];
    boolean isTimesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isTimesNotNull) {
      for (int i = 0; i < rowSize; i++) {
        times[i] = ReadWriteIOUtils.readLong(byteBuffer);
      }
    }

    // deserialize bitmaps
    BitMap[] bitMaps = new BitMap[schemaSize];
    boolean isBitMapsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isBitMapsNotNull) {
      bitMaps = readBitMapsFromBuffer(byteBuffer, schemaSize, rowSize);
    }

    // deserialize values
    TSDataType[] dataTypes =
        schemas.stream().map(MeasurementSchema::getType).toArray(TSDataType[]::new);
    Object[] values = new Object[schemaSize];
    boolean isValuesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isValuesNotNull) {
      values = readTabletValuesFromBuffer(byteBuffer, dataTypes, schemaSize, rowSize);
    }

    Tablet tablet = new Tablet(deviceId, schemas, times, values, bitMaps, rowSize);
    tablet.constructMeasurementIndexMap();
    return tablet;
  }

  /** deserialize bitmaps */
  public static BitMap[] readBitMapsFromBuffer(ByteBuffer byteBuffer, int columns, int rowSize) {
    BitMap[] bitMaps = new BitMap[columns];
    for (int i = 0; i < columns; i++) {
      boolean hasBitMap = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (hasBitMap) {
        final Binary valueBinary = ReadWriteIOUtils.readBinary(byteBuffer);
        bitMaps[i] = new BitMap(rowSize, valueBinary.getValues());
      }
    }
    return bitMaps;
  }

  /**
   * @param byteBuffer data values
   * @param columns column number
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Object[] readTabletValuesFromBuffer(
      ByteBuffer byteBuffer, TSDataType[] types, int columns, int rowSize) {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      boolean isValueColumnsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));

      if (isValueColumnsNotNull) {
        switch (types[i]) {
          case BOOLEAN:
            boolean[] boolValues = new boolean[rowSize];
            for (int index = 0; index < rowSize; index++) {
              boolValues[index] = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
            }
            values[i] = boolValues;
            break;
          case INT32:
            int[] intValues = new int[rowSize];
            for (int index = 0; index < rowSize; index++) {
              intValues[index] = ReadWriteIOUtils.readInt(byteBuffer);
            }
            values[i] = intValues;
            break;
          case INT64:
            long[] longValues = new long[rowSize];
            for (int index = 0; index < rowSize; index++) {
              longValues[index] = ReadWriteIOUtils.readLong(byteBuffer);
            }
            values[i] = longValues;
            break;
          case FLOAT:
            float[] floatValues = new float[rowSize];
            for (int index = 0; index < rowSize; index++) {
              floatValues[index] = ReadWriteIOUtils.readFloat(byteBuffer);
            }
            values[i] = floatValues;
            break;
          case DOUBLE:
            double[] doubleValues = new double[rowSize];
            for (int index = 0; index < rowSize; index++) {
              doubleValues[index] = ReadWriteIOUtils.readDouble(byteBuffer);
            }
            values[i] = doubleValues;
            break;
          case TEXT:
            Binary[] binaryValues = new Binary[rowSize];
            for (int index = 0; index < rowSize; index++) {
              boolean isNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
              if (isNotNull) {
                binaryValues[index] = ReadWriteIOUtils.readBinary(byteBuffer);
              }
            }
            values[i] = binaryValues;
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "data type %s is not supported when convert data at client", types[i]));
        }
      }
    }
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tablet that = (Tablet) o;

    boolean flag =
        that.rowSize == rowSize
            && Objects.equals(that.deviceId, deviceId)
            && Arrays.equals(that.timestamps, timestamps)
            && Arrays.equals(that.bitMaps, bitMaps)
            && Objects.equals(that.schemas, schemas)
            && Objects.equals(that.measurementIndex, measurementIndex);
    if (!flag) {
      return false;
    }

    // assert values
    Object[] thatValues = that.values;
    if (thatValues.length != values.length) {
      return false;
    }
    for (int i = 0, n = values.length; i < n; i++) {
      switch (schemas.get(i).getType()) {
        case INT32:
          if (!Arrays.equals((int[]) thatValues[i], (int[]) values[i])) {
            return false;
          }
          break;
        case INT64:
          if (!Arrays.equals((long[]) thatValues[i], (long[]) values[i])) {
            return false;
          }
          break;
        case FLOAT:
          if (!Arrays.equals((float[]) thatValues[i], (float[]) values[i])) {
            return false;
          }
          break;
        case DOUBLE:
          if (!Arrays.equals((double[]) thatValues[i], (double[]) values[i])) {
            return false;
          }
          break;
        case BOOLEAN:
          if (!Arrays.equals((boolean[]) thatValues[i], (boolean[]) values[i])) {
            return false;
          }
          break;
        case TEXT:
          if (!Arrays.equals((Binary[]) thatValues[i], (Binary[]) values[i])) {
            return false;
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schemas.get(i).getType()));
      }
    }

    return true;
  }
}
