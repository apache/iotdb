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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.*;

public class NonAlignedTablet {

  private static final int DEFAULT_SIZE = 1024;
  private static final String NOT_SUPPORT_DATATYPE = "Data type %s is not supported.";

  /** deviceId of this tablet */
  public String deviceId;

  /** the list of measurement schemas for creating the tablet */
  private List<MeasurementSchema> schemas;

  /** measurementId->indexOf(measurementSchema) */
  private final Map<String, Integer> measurementIndex;

  /** timestamps in this tablet */
  public long[][] timestamps;
  /** each object is a primitive type array, which represents values of one measurement */
  public Object[] values;

  /** the number of rows for each sensor to include in this tablet */
  public int[] rowSize;
  /** the max number of rows in each sensor */
  public int maxRowSize;
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
  public NonAlignedTablet(String deviceId, List<MeasurementSchema> schemas) {
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
  public NonAlignedTablet(String deviceId, List<MeasurementSchema> schemas, int maxRowNumber) {
    this.deviceId = deviceId;
    this.schemas = new ArrayList<>(schemas);
    this.maxRowNumber = maxRowNumber;
    measurementIndex = new HashMap<>();

    int indexInSchema = 0;
    for (MeasurementSchema schema : schemas) {
      if (schema.getType() == TSDataType.VECTOR) {
        for (String measurementId : schema.getSubMeasurementsList()) {
          measurementIndex.put(measurementId, indexInSchema);
        }
      } else {
        measurementIndex.put(schema.getMeasurementId(), indexInSchema);
      }
      indexInSchema++;
    }

    createColumns();

    reset();
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public void setSchemas(List<MeasurementSchema> schemas) {
    this.schemas = schemas;
  }

  public void addValue(String measurementId, long time, Object value) {
    if (value == null) {
      return;
    }
    int indexOfSchema = measurementIndex.get(measurementId);
    MeasurementSchema measurementSchema = schemas.get(indexOfSchema);
    addValueOfDataType(
        measurementSchema.getType(), rowSize[indexOfSchema]++, indexOfSchema, time, value);
    maxRowSize = Math.max(maxRowSize, rowSize[indexOfSchema]);
  }

  private void addValueOfDataType(
      TSDataType dataType, int rowIndex, int indexOfSchema, long time, Object value) {
    timestamps[indexOfSchema][rowIndex] = time;
    switch (dataType) {
      case TEXT:
        {
          Binary[] sensor = (Binary[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (Binary) value : Binary.EMPTY_VALUE;
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
    maxRowSize = 0;
    if (rowSize == null) {
      rowSize = new int[schemas.size()];
    } else {
      Arrays.fill(rowSize, 0);
    }
  }

  private void createColumns() {
    // create timestamp column
    timestamps = new long[schemas.size()][maxRowNumber];

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

  //    public int getTimeBytesSize() {
  //        return rowSize * 8;
  //    }
  //
  //    /**
  //     * @return total bytes of values
  //     */
  //    public int getTotalValueOccupation() {
  //        int valueOccupation = 0;
  //        int columnIndex = 0;
  //        for (MeasurementSchema schema : schemas) {
  //            valueOccupation += calOccupationOfOneColumn(schema.getType(), columnIndex);
  //            columnIndex++;
  //        }
  //        // add bitmap size if the tablet has bitMaps
  //        if (bitMaps != null) {
  //            for (BitMap bitMap : bitMaps) {
  //                // marker byte
  //                valueOccupation++;
  //                if (bitMap != null && !bitMap.isAllUnmarked()) {
  //                    valueOccupation += rowSize / Byte.SIZE + 1;
  //                }
  //            }
  //        }
  //        return valueOccupation;
  //    }

  //    private int calOccupationOfOneColumn(TSDataType dataType, int columnIndex) {
  //        int valueOccupation = 0;
  //        switch (dataType) {
  //            case BOOLEAN:
  //                valueOccupation += rowSize;
  //                break;
  //            case INT32:
  //            case FLOAT:
  //                valueOccupation += rowSize * 4;
  //                break;
  //            case INT64:
  //            case DOUBLE:
  //                valueOccupation += rowSize * 8;
  //                break;
  //            case TEXT:
  //                valueOccupation += rowSize * 4;
  //                Binary[] binaries = (Binary[]) values[columnIndex];
  //                for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
  //                    valueOccupation += binaries[rowIndex].getLength();
  //                }
  //                break;
  //            default:
  //                throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE,
  // dataType));
  //        }
  //        return valueOccupation;
  //    }
}
