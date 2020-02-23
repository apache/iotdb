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

package org.apache.iotdb.db.query.aggregation;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public abstract class AggregateResult {

  protected TSDataType dataType;

  private boolean booleanValue;
  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  private Binary binaryValue;

  private boolean hasResult;

  /**
   * construct.
   *
   * @param dataType result data type.
   */
  public AggregateResult(TSDataType dataType) {
    this.dataType = dataType;
    this.hasResult = false;
  }

  public abstract Object getResult();

  /**
   * Calculate the aggregation using Statistics
   *
   * @param statistics chunkStatistics or pageStatistics
   */
  public abstract void updateResultFromStatistics(Statistics statistics)
      throws QueryProcessException;

  /**
   * Aggregate results cannot be calculated using Statistics directly, using the data in each page
   *
   * @param dataInThisPage the data in Page
   */
  public abstract void updateResultFromPageData(BatchData dataInThisPage) throws IOException;
  /**
   * Aggregate results cannot be calculated using Statistics directly, using the data in each page
   *
   * @param dataInThisPage the data in Page
   * @param bound calculate points whose time < bound
   */
  public abstract void updateResultFromPageData(BatchData dataInThisPage, long bound)
      throws IOException;

  /**
   * <p> This method calculates the aggregation using common timestamps of the cross series
   * filter. </p>
   *
   * @throws IOException TsFile data read error
   */
  public abstract void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException;

  /**
   * Judge if aggregation results have been calculated. In other words, if the aggregated result
   * does not need to compute the remaining data, it returns true.
   *
   * @return If the aggregation result has been calculated return true, else return false.
   */
  public abstract boolean isCalculatedAggregationResult();

  public void reset() {
    hasResult = false;
  }

  protected Object getValue() {
    switch (dataType) {
      case BOOLEAN:
        return booleanValue;
      case DOUBLE:
        return doubleValue;
      case TEXT:
        return binaryValue;
      case FLOAT:
        return floatValue;
      case INT32:
        return intValue;
      case INT64:
        return longValue;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  /**
   * set an object.
   *
   * @param v object value
   */
  protected void setValue(Object v) {
    hasResult = true;
    switch (dataType) {
      case BOOLEAN:
        booleanValue = (Boolean) v;
        break;
      case DOUBLE:
        doubleValue = (Double) v;
        break;
      case TEXT:
        binaryValue = (Binary) v;
        break;
      case FLOAT:
        floatValue = (Float) v;
        break;
      case INT32:
        intValue = (Integer) v;
        break;
      case INT64:
        longValue = (Long) v;
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  protected boolean getBooleanValue() {
    return booleanValue;
  }

  protected void setBooleanValue(boolean booleanValue) {
    this.hasResult = true;
    this.booleanValue = booleanValue;
  }

  protected int getIntValue() {
    return intValue;
  }

  protected void setIntValue(int intValue) {
    this.hasResult = true;
    this.intValue = intValue;
  }

  protected long getLongValue() {
    return longValue;
  }

  protected void setLongValue(long longValue) {
    this.hasResult = true;
    this.longValue = longValue;
  }

  protected float getFloatValue() {
    return floatValue;
  }

  protected void setFloatValue(float floatValue) {
    this.hasResult = true;
    this.floatValue = floatValue;
  }

  protected double getDoubleValue() {
    return doubleValue;
  }

  protected void setDoubleValue(double doubleValue) {
    this.hasResult = true;
    this.doubleValue = doubleValue;
  }

  protected Binary getBinaryValue() {
    return binaryValue;
  }

  protected void setBinaryValue(Binary binaryValue) {
    this.hasResult = true;
    this.binaryValue = binaryValue;
  }

  protected boolean hasResult() {
    return hasResult;
  }
}
