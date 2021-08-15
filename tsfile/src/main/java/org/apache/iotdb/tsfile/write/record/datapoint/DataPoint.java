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

package org.apache.iotdb.tsfile.write.record.datapoint;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * This is a abstract class representing a data point. DataPoint consists of a measurement id and a
 * data type. subclass of DataPoint need override method {@code write(long time, IChunkWriter
 * writer)} .Every subclass has its data type and overrides a setting method for its data type.
 */
public abstract class DataPoint {

  /** value type of this DataPoint. */
  protected final TSDataType type;
  /** measurementId of this DataPoint. */
  protected final String measurementId;

  /**
   * constructor of DataPoint.
   *
   * @param type value type of this DataPoint
   * @param measurementId measurementId of this DataPoint
   */
  public DataPoint(TSDataType type, String measurementId) {
    this.type = type;
    this.measurementId = measurementId;
  }

  /**
   * Construct one data point with data type and value.
   *
   * @param dataType data type
   * @param measurementId measurement id
   * @param value value in string format
   * @return data point class according to data type
   */
  public static DataPoint getDataPoint(TSDataType dataType, String measurementId, String value) {
    DataPoint dataPoint = null;
    try {
      switch (dataType) {
        case INT32:
          dataPoint = new IntDataPoint(measurementId, Integer.valueOf(value));
          break;
        case INT64:
          dataPoint = new LongDataPoint(measurementId, Long.valueOf(value));
          break;
        case FLOAT:
          dataPoint = new FloatDataPoint(measurementId, Float.valueOf(value));
          break;
        case DOUBLE:
          dataPoint = new DoubleDataPoint(measurementId, Double.valueOf(value));
          break;
        case BOOLEAN:
          dataPoint = new BooleanDataPoint(measurementId, Boolean.valueOf(value));
          break;
        case TEXT:
          dataPoint = new StringDataPoint(measurementId, new Binary(value));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    } catch (Exception e) {
      throw new UnSupportedDataTypeException(
          String.format(
              "Data type of %s is %s, but input value is %s", measurementId, dataType, value));
    }

    return dataPoint;
  }

  /**
   * write this DataPoint by a SeriesWriter.
   *
   * @param time timestamp
   * @param writer writer
   * @throws IOException exception in IO
   */
  public abstract void writeTo(long time, IChunkWriter writer) throws IOException;

  public String getMeasurementId() {
    return measurementId;
  }

  public abstract Object getValue();

  public TSDataType getType() {
    return type;
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer(" ");
    sc.addTail("{measurement id:", measurementId, "type:", type, "value:", getValue(), "}");
    return sc.toString();
  }

  public void setInteger(int value) {
    throw new UnsupportedOperationException("set Integer not support in DataPoint");
  }

  public void setLong(long value) {
    throw new UnsupportedOperationException("set Long not support in DataPoint");
  }

  public void setBoolean(boolean value) {
    throw new UnsupportedOperationException("set Boolean not support in DataPoint");
  }

  public void setFloat(float value) {
    throw new UnsupportedOperationException("set Float not support in DataPoint");
  }

  public void setDouble(double value) {
    throw new UnsupportedOperationException("set Double not support in DataPoint");
  }

  public void setString(Binary value) {
    throw new UnsupportedOperationException("set String not support in DataPoint");
  }

  public void setBigDecimal(BigDecimal value) {
    throw new UnsupportedOperationException("set BigDecimal not support in DataPoint");
  }
}
