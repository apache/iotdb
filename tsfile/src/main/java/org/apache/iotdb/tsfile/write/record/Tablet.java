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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * A tablet data of one device, the tablet contains multiple measurements of this device that share
 * the same time column.
 * <p>
 * for example:  device root.sg1.d1
 * <p>
 * time, m1, m2, m3 1,  1,  2,  3 2,  1,  2,  3 3,  1,  2,  3
 * <p>
 * Notice: The tablet should not have empty cell
 */
public class Tablet {

  private static final int DEFAULT_SIZE = 1024;
  private static final String NOT_SUPPORT_DATATYPE = "Data type %s is not supported.";

  /**
   * deviceId of this tablet
   */
  public String deviceId;

  /**
   * the list of measurement schemas for creating the tablet
   */
  private List<MeasurementSchema> schemas;

  /**
   * measurementId->indexOf(measurementSchema)
   */
  private Map<String, Integer> measurementIndex;

  /**
   * timestamps in this tablet
   */
  public long[] timestamps;
  /**
   * each object is a primitive type array, which represents values of one measurement
   */
  public Object[] values;
  /**
   * the number of rows to include in this tablet
   */
  public int rowSize;
  /**
   * the maximum number of rows for this tablet
   */
  private int maxRowNumber;

  /**
   * Return a tablet with default specified row number. This is the standard constructor (all Tablet
   * should be the same size).
   *
   * @param deviceId the name of the device specified to be written in
   * @param schemas  the list of measurement schemas for creating the tablet, only measurementId and
   *                 type take effects
   */
  public Tablet(String deviceId, List<MeasurementSchema> schemas) {
    this(deviceId, schemas, DEFAULT_SIZE);
  }

  /**
   * Return a tablet with the specified number of rows (maxBatchSize). Only call this constructor
   * directly for testing purposes. Tablet should normally always be default size.
   *
   * @param deviceId     the name of the device specified to be written in
   * @param schemas      the list of measurement schemas for creating the row batch, only
   *                     measurementId and type take effects
   * @param maxRowNumber the maximum number of rows for this tablet
   */
  public Tablet(String deviceId, List<MeasurementSchema> schemas, int maxRowNumber) {
    this.deviceId = deviceId;
    this.schemas = schemas;
    this.maxRowNumber = maxRowNumber;
    measurementIndex = new HashMap<>();

    for (int i = 0; i < schemas.size(); i++) {
      measurementIndex.put(schemas.get(i).getMeasurementId(), i);
    }

    createColumns();

    reset();
  }

  public void addTimestamp(int rowIndex, long timestamp) {
    timestamps[rowIndex] = timestamp;
  }

  public void addValue(String measurementId, int rowIndex, Object value) {
    int indexOfValue = measurementIndex.get(measurementId);
    MeasurementSchema measurementSchema = schemas.get(indexOfValue);

    switch (measurementSchema.getType()) {
      case TEXT: {
        Binary[] sensor = (Binary[]) values[indexOfValue];
        sensor[rowIndex] = (Binary) value;
        break;
      }
      case FLOAT: {
        float[] sensor = (float[]) values[indexOfValue];
        sensor[rowIndex] = (float) value;
        break;
      }
      case INT32: {
        int[] sensor = (int[]) values[indexOfValue];
        sensor[rowIndex] = (int) value;
        break;
      }
      case INT64: {
        long[] sensor = (long[]) values[indexOfValue];
        sensor[rowIndex] = (long) value;
        break;
      }
      case DOUBLE: {
        double[] sensor = (double[]) values[indexOfValue];
        sensor[rowIndex] = (double) value;
        break;
      }
      case BOOLEAN: {
        boolean[] sensor = (boolean[]) values[indexOfValue];
        sensor[rowIndex] = (boolean) value;
        break;
      }
      default:
        throw new UnSupportedDataTypeException(
            String.format(NOT_SUPPORT_DATATYPE, measurementSchema.getType()));
    }
  }

  public List<MeasurementSchema> getSchemas() {
    return schemas;
  }

  /**
   * Return the maximum number of rows for this tablet
   */
  public int getMaxRowNumber() {
    return maxRowNumber;
  }

  /**
   * Reset Tablet to the default state - set the rowSize to 0
   */
  public void reset() {
    rowSize = 0;
  }

  private void createColumns() {
    // create timestamp column
    timestamps = new long[maxRowNumber];
    values = new Object[schemas.size()];
    // create value columns
    for (int i = 0; i < schemas.size(); i++) {
      TSDataType dataType = schemas.get(i).getType();
      switch (dataType) {
        case INT32:
          values[i] = new int[maxRowNumber];
          break;
        case INT64:
          values[i] = new long[maxRowNumber];
          break;
        case FLOAT:
          values[i] = new float[maxRowNumber];
          break;
        case DOUBLE:
          values[i] = new double[maxRowNumber];
          break;
        case BOOLEAN:
          values[i] = new boolean[maxRowNumber];
          break;
        case TEXT:
          values[i] = new Binary[maxRowNumber];
          break;
        default:
          throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
      }
    }
  }

  public int getTimeBytesSize() {
    return rowSize * 8;
  }

  /**
   * @return total bytes of values
   */
  public int getValueBytesSize() {
    /**
     * total byte size that values occupies
     */
    int valueOccupation = 0;
    for (int i = 0; i < schemas.size(); i++) {
      switch (schemas.get(i).getType()) {
        case BOOLEAN:
          valueOccupation += rowSize;
          break;
        case INT32:
          valueOccupation += rowSize * 4;
          break;
        case INT64:
          valueOccupation += rowSize * 8;
          break;
        case FLOAT:
          valueOccupation += rowSize * 4;
          break;
        case DOUBLE:
          valueOccupation += rowSize * 8;
          break;
        case TEXT:
          valueOccupation += rowSize * 4;
          for (Binary value : (Binary[]) values[i]) {
            valueOccupation += value.getLength();
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(NOT_SUPPORT_DATATYPE, schemas.get(i).getType()));
      }
    }
    return valueOccupation;
  }
}
