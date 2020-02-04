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

import org.apache.iotdb.db.exception.query.PlannerException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public abstract class AggregateResult {

  protected TSDataType dataType;

  private boolean booleanRet;
  private int intRet;
  private long longRet;
  private float floatRet;
  private double doubleRet;
  private Binary binaryRet;

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
      throws PlannerException;

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
   * <p> This method is calculate the aggregation using the common timestamps of cross series
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
        return booleanRet;
      case DOUBLE:
        return doubleRet;
      case TEXT:
        return binaryRet;
      case FLOAT:
        return floatRet;
      case INT32:
        return intRet;
      case INT64:
        return longRet;
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
        booleanRet = (Boolean) v;
        break;
      case DOUBLE:
        doubleRet = (Double) v;
        break;
      case TEXT:
        binaryRet = (Binary) v;
        break;
      case FLOAT:
        floatRet = (Float) v;
        break;
      case INT32:
        intRet = (Integer) v;
        break;
      case INT64:
        longRet = (Long) v;
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  protected boolean getBooleanRet() {
    return booleanRet;
  }

  protected void setBooleanRet(boolean booleanRet) {
    this.hasResult = true;
    this.booleanRet = booleanRet;
  }

  protected int getIntRet() {
    return intRet;
  }

  protected void setIntRet(int intRet) {
    this.hasResult = true;
    this.intRet = intRet;
  }

  protected long getLongRet() {
    return longRet;
  }

  protected void setLongRet(long longRet) {
    this.hasResult = true;
    this.longRet = longRet;
  }

  protected float getFloatRet() {
    return floatRet;
  }

  protected void setFloatRet(float floatRet) {
    this.hasResult = true;
    this.floatRet = floatRet;
  }

  protected double getDoubleRet() {
    return doubleRet;
  }

  protected void setDoubleRet(double doubleRet) {
    this.hasResult = true;
    this.doubleRet = doubleRet;
  }

  protected Binary getBinaryRet() {
    return binaryRet;
  }

  protected void setBinaryRet(Binary binaryRet) {
    this.hasResult = true;
    this.binaryRet = binaryRet;
  }

  protected boolean hasResult() {
    return hasResult;
  }
}
