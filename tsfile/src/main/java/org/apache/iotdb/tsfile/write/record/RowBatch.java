/**
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

import java.util.List;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Multiple time series of one device that share a time column
 */
public class RowBatch {

  private static final int DEFAULT_SIZE = 1024;

  /**
   * deviceId of this row batch
   */
  public String deviceId;
  /**
   * the list of measurement schemas for creating the row batch
   */
  public List<MeasurementSchema> measurements;

  /**
   * timestamps in this row batch
   */
  public long[] timestamps;
  /**
   * each object is a primitive type array, which represents values of one measurement
   */
  public Object[] values;
  /**
   * the number of rows to include in this row batch
   */
  public int batchSize;
  /**
   * the maximum number of rows for this row batch
   */
  private int maxBatchSize;

  /**
   * total byte size that values occupies
   */
  private int valueOccupation = -1;

  /**
   * Return a row batch with default specified row number.
   * This is the standard constructor (all RowBatch should be the same size).
   *
   * @param deviceId the name of the device specified to be written in
   * @param measurements the list of measurement schemas for creating the row batch
   */
  public RowBatch(String deviceId, List<MeasurementSchema> measurements) {
    this(deviceId, measurements, DEFAULT_SIZE);
  }

  /**
   * Return a row batch with the specified number of rows (maxBatchSize).
   * Only call this constructor directly for testing purposes.
   * RowBatch should normally always be default size.
   *
   * @param deviceId the name of the device specified to be written in
   * @param measurements the list of measurement schemas for creating the row batch
   * @param maxBatchSize the maximum number of rows for this row batch
   */
  public RowBatch(String deviceId, List<MeasurementSchema> measurements, int maxBatchSize) {
    this.deviceId = deviceId;
    this.measurements = measurements;
    this.maxBatchSize = maxBatchSize;

    createColumns();

    reset();
  }

  /**
   * Return the maximum number of rows for this row batch
   */
  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  /**
   * Reset RowBatch to the default state - set the batchSize to 0
   */
  public void reset() {
    batchSize = 0;
  }

  private void createColumns() {
    // create timestamp column
    timestamps = new long[maxBatchSize];
    values = new Object[measurements.size()];
    // create value columns
    for (int i = 0; i < measurements.size(); i++) {
      TSDataType dataType = measurements.get(i).getType();
      switch (dataType) {
        case INT32:
          values[i] = new int[maxBatchSize];
          break;
        case INT64:
          values[i] = new long[maxBatchSize];
          break;
        case FLOAT:
          values[i] = new float[maxBatchSize];
          break;
        case DOUBLE:
          values[i] = new double[maxBatchSize];
          break;
        case BOOLEAN:
          values[i] = new boolean[maxBatchSize];
          break;
        case TEXT:
          values[i] = new Binary[maxBatchSize];
          break;
        default:
          throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", dataType));
      }
    }
  }

  public int getTimeBytesSize() {
    return batchSize * 8;
  }

  /**
   * @return total bytes of values
   */
  public int getValueBytesSize() {
    if (valueOccupation != -1) {
      return valueOccupation;
    }
    valueOccupation = 0;
    for (int i = 0; i < measurements.size(); i++) {
      switch (measurements.get(i).getType()) {
        case BOOLEAN:
          valueOccupation += batchSize;
          break;
        case INT32:
          valueOccupation += batchSize * 4;
          break;
        case INT64:
          valueOccupation += batchSize * 8;
          break;
        case FLOAT:
          valueOccupation += batchSize * 4;
          break;
        case DOUBLE:
          valueOccupation += batchSize * 8;
          break;
        case TEXT:
          for (Binary value: (Binary[]) values[i]) {
            valueOccupation += value.getLength();
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", measurements.get(i).getType()));
      }
    }
    return valueOccupation;
  }
}
