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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A tablet data of one device, the tablet contains multiple measurements of this device that share
 * the same time column.
 *
 * <p>for example: device root.sg1.d1
 *
 * <p>time, m1, m2, m3 1, 1, 2, 3 2, 1, 2, 3 3, 1, 2, 3
 *
 * <p>Notice: The tablet should not have empty cell
 */
public class Tablet {

  private static final int DEFAULT_SIZE = 1024;
  private static final String NOT_SUPPORT_DATATYPE = "Data type %s is not supported.";

  /** deviceId of this tablet */
  public String deviceId;

  /** the list of measurement schemas for creating the tablet */
  private List<IMeasurementSchema> schemas;

  /** measurementId->indexOf(measurementSchema) */
  private Map<String, Integer> measurementIndexInSchema;

  /** measurementId->indexOf(values) */
  private Map<String, Integer> measurementIndexInValues;

  /** timestamps in this tablet */
  public long[] timestamps;
  /** each object is a primitive type array, which represents values of one measurement */
  public Object[] values;
  /** each bitmap represents the existence of each value in the current column. */
  public BitMap[] bitMaps;
  /** the number of rows to include in this tablet */
  public int rowSize;
  /** the maximum number of rows for this tablet */
  private int maxRowNumber;

  /**
   * Return a tablet with default specified row number. This is the standard constructor (all Tablet
   * should be the same size).
   *
   * @param deviceId the name of the device specified to be written in
   * @param schemas the list of measurement schemas for creating the tablet, only measurementId and
   *     type take effects
   */
  public Tablet(String deviceId, List<IMeasurementSchema> schemas) {
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
  public Tablet(String deviceId, List<IMeasurementSchema> schemas, int maxRowNumber) {
    this.deviceId = deviceId;
    this.schemas = new ArrayList<>(schemas);
    this.maxRowNumber = maxRowNumber;
    measurementIndexInSchema = new HashMap<>();
    measurementIndexInValues = new HashMap<>();

    int indexInValues = 0;
    int indexInSchema = 0;
    for (IMeasurementSchema schema : schemas) {
      if (schema.getType() == TSDataType.VECTOR) {
        for (String measurementId : schema.getValueMeasurementIdList()) {
          measurementIndexInValues.put(measurementId, indexInValues);
          measurementIndexInSchema.put(measurementId, indexInSchema);
          indexInValues++;
        }
      } else {
        measurementIndexInValues.put(schema.getMeasurementId(), indexInValues);
        measurementIndexInSchema.put(schema.getMeasurementId(), indexInSchema);
        indexInValues++;
      }
      indexInSchema++;
    }

    createColumns();

    reset();
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public void addTimestamp(int rowIndex, long timestamp) {
    timestamps[rowIndex] = timestamp;
  }

  // (s1, s2)  s3
  public void addValue(String measurementId, int rowIndex, Object value) {
    int indexOfValues = measurementIndexInValues.get(measurementId);
    int indexOfSchema = measurementIndexInSchema.get(measurementId);
    IMeasurementSchema measurementSchema = schemas.get(indexOfSchema);

    if (measurementSchema.getType().equals(TSDataType.VECTOR)) {
      int indexInVector = measurementSchema.getMeasurementIdColumnIndex(measurementId);
      TSDataType dataType = measurementSchema.getValueTSDataTypeList().get(indexInVector);
      addValueOfDataType(dataType, rowIndex, indexOfValues, value);
    } else {
      addValueOfDataType(measurementSchema.getType(), rowIndex, indexOfValues, value);
    }
  }

  private void addValueOfDataType(
      TSDataType dataType, int rowIndex, int indexOfValue, Object value) {

    if (value == null) {
      // init the bitMap to mark null value
      if (bitMaps == null) {
        bitMaps = new BitMap[values.length];
      }
      if (bitMaps[indexOfValue] == null) {
        bitMaps[indexOfValue] = new BitMap(maxRowNumber);
      }
      // mark the null value position
      bitMaps[indexOfValue].mark(rowIndex);
    }
    switch (dataType) {
      case TEXT:
        {
          Binary[] sensor = (Binary[]) values[indexOfValue];
          sensor[rowIndex] = value != null ? (Binary) value : Binary.EMPTY_VALUE;
          break;
        }
      case FLOAT:
        {
          float[] sensor = (float[]) values[indexOfValue];
          sensor[rowIndex] = value != null ? (float) value : Float.MIN_VALUE;
          break;
        }
      case INT32:
        {
          int[] sensor = (int[]) values[indexOfValue];
          sensor[rowIndex] = value != null ? (int) value : Integer.MIN_VALUE;
          break;
        }
      case INT64:
        {
          long[] sensor = (long[]) values[indexOfValue];
          sensor[rowIndex] = value != null ? (long) value : Long.MIN_VALUE;
          break;
        }
      case DOUBLE:
        {
          double[] sensor = (double[]) values[indexOfValue];
          sensor[rowIndex] = value != null ? (double) value : Double.MIN_VALUE;
          break;
        }
      case BOOLEAN:
        {
          boolean[] sensor = (boolean[]) values[indexOfValue];
          sensor[rowIndex] = value != null && (boolean) value;
          break;
        }
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
  }

  public List<IMeasurementSchema> getSchemas() {
    return schemas;
  }

  /** Return the maximum number of rows for this tablet */
  public int getMaxRowNumber() {
    return maxRowNumber;
  }

  /** Reset Tablet to the default state - set the rowSize to 0 */
  public void reset() {
    rowSize = 0;
  }

  private void createColumns() {
    // create timestamp column
    timestamps = new long[maxRowNumber];

    // calculate total value column size
    int valueColumnsSize = 0;
    for (IMeasurementSchema schema : schemas) {
      if (schema instanceof VectorMeasurementSchema) {
        valueColumnsSize += schema.getValueMeasurementIdList().size();
      } else {
        valueColumnsSize++;
      }
    }

    // value column
    values = new Object[valueColumnsSize];
    int columnIndex = 0;
    for (IMeasurementSchema schema : schemas) {
      TSDataType dataType = schema.getType();
      if (dataType.equals(TSDataType.VECTOR)) {
        columnIndex = buildVectorColumns((VectorMeasurementSchema) schema, columnIndex);
      } else {
        values[columnIndex] = createValueColumnOfDataType(dataType);
        columnIndex++;
      }
    }
  }

  private int buildVectorColumns(VectorMeasurementSchema schema, int idx) {
    for (int i = 0; i < schema.getValueMeasurementIdList().size(); i++) {
      TSDataType dataType = schema.getValueTSDataTypeList().get(i);
      values[idx] = createValueColumnOfDataType(dataType);
      idx++;
    }
    return idx;
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
    for (int i = 0; i < schemas.size(); i++) {
      IMeasurementSchema schema = schemas.get(i);
      if (schema instanceof MeasurementSchema) {
        valueOccupation += calOccupationOfOneColumn(schema.getType(), columnIndex);
        columnIndex++;
      } else {
        for (int j = 0; j < schema.getValueTSDataTypeList().size(); j++) {
          TSDataType dataType = schema.getValueTSDataTypeList().get(j);
          valueOccupation += calOccupationOfOneColumn(dataType, columnIndex);
          columnIndex++;
        }
      }
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
}
