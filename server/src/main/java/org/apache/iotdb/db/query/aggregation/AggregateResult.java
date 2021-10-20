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
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class AggregateResult {

  public static final int TIME_LENGTH_FOR_FIRST_VALUE = 100;
  private final AggregationType aggregationType;
  protected TSDataType resultDataType;

  private boolean booleanValue;
  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  private Binary binaryValue;

  protected boolean hasCandidateResult;

  /**
   * construct.
   *
   * @param resultDataType result data type.
   */
  public AggregateResult(TSDataType resultDataType, AggregationType aggregationType) {
    this.aggregationType = aggregationType;
    this.resultDataType = resultDataType;
    this.hasCandidateResult = false;
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
   * @param batchIterator the data in Page
   */
  public abstract void updateResultFromPageData(IBatchDataIterator batchIterator)
      throws IOException, QueryProcessException;

  /**
   * Aggregate results cannot be calculated using Statistics directly, using the data in each page
   *
   * @param batchIterator the data in Page
   * @param minBound calculate points whose time >= bound
   * @param maxBound calculate points whose time < bound
   */
  public abstract void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) throws IOException;

  /**
   * This method calculates the aggregation using common timestamps of the cross series filter.
   *
   * @throws IOException TsFile data read error
   */
  public abstract void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException;

  /** This method calculates the aggregation using values that have been calculated */
  public abstract void updateResultUsingValues(long[] timestamps, int length, Object[] values);

  /**
   * Judge if aggregation results have been calculated. In other words, if the aggregated result
   * does not need to compute the remaining data, it returns true.
   *
   * @return If the aggregation result has been calculated return true, else return false.
   */
  public abstract boolean hasFinalResult();

  /** Merge another aggregateResult into this */
  public abstract void merge(AggregateResult another);

  public static AggregateResult deserializeFrom(ByteBuffer buffer) {
    AggregationType aggregationType = AggregationType.deserialize(buffer);
    TSDataType dataType = TSDataType.deserialize(buffer.get());
    boolean ascending = ReadWriteIOUtils.readBool(buffer);
    AggregateResult aggregateResult =
        AggregateResultFactory.getAggrResultByType(aggregationType, dataType, ascending);
    boolean hasResult = ReadWriteIOUtils.readBool(buffer);
    if (hasResult) {
      switch (dataType) {
        case BOOLEAN:
          aggregateResult.setBooleanValue(ReadWriteIOUtils.readBool(buffer));
          break;
        case INT32:
          aggregateResult.setIntValue(buffer.getInt());
          break;
        case INT64:
          aggregateResult.setLongValue(buffer.getLong());
          break;
        case FLOAT:
          aggregateResult.setFloatValue(buffer.getFloat());
          break;
        case DOUBLE:
          aggregateResult.setDoubleValue(buffer.getDouble());
          break;
        case TEXT:
          aggregateResult.setBinaryValue(ReadWriteIOUtils.readBinary(buffer));
          break;
        default:
          throw new IllegalArgumentException("Invalid Aggregation Type: " + dataType.name());
      }
      aggregateResult.deserializeSpecificFields(buffer);
    }
    return aggregateResult;
  }

  protected abstract void deserializeSpecificFields(ByteBuffer buffer);

  public void serializeTo(OutputStream outputStream) throws IOException {
    aggregationType.serializeTo(outputStream);
    ReadWriteIOUtils.write(resultDataType, outputStream);
    ReadWriteIOUtils.write(isAscending(), outputStream);
    ReadWriteIOUtils.write(hasCandidateResult(), outputStream);
    if (hasCandidateResult()) {
      switch (resultDataType) {
        case BOOLEAN:
          ReadWriteIOUtils.write(booleanValue, outputStream);
          break;
        case INT32:
          ReadWriteIOUtils.write(intValue, outputStream);
          break;
        case INT64:
          ReadWriteIOUtils.write(longValue, outputStream);
          break;
        case FLOAT:
          ReadWriteIOUtils.write(floatValue, outputStream);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write(doubleValue, outputStream);
          break;
        case TEXT:
          ReadWriteIOUtils.write(binaryValue, outputStream);
          break;
        default:
          throw new IllegalArgumentException("Invalid Aggregation Type: " + resultDataType.name());
      }
      serializeSpecificFields(outputStream);
    }
  }

  protected abstract void serializeSpecificFields(OutputStream outputStream) throws IOException;

  public void reset() {
    hasCandidateResult = false;
    booleanValue = false;
    doubleValue = 0;
    floatValue = 0;
    intValue = 0;
    longValue = 0;
    binaryValue = null;
  }

  protected Object getValue() {
    switch (resultDataType) {
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
        throw new UnSupportedDataTypeException(String.valueOf(resultDataType));
    }
  }

  /**
   * set an object.
   *
   * @param v object value
   */
  protected void setValue(Object v) {
    hasCandidateResult = true;
    switch (resultDataType) {
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
        throw new UnSupportedDataTypeException(String.valueOf(resultDataType));
    }
  }

  public TSDataType getResultDataType() {
    return resultDataType;
  }

  protected boolean getBooleanValue() {
    return booleanValue;
  }

  public void setBooleanValue(boolean booleanValue) {
    this.hasCandidateResult = true;
    this.booleanValue = booleanValue;
  }

  protected int getIntValue() {
    return intValue;
  }

  public void setIntValue(int intValue) {
    this.hasCandidateResult = true;
    this.intValue = intValue;
  }

  protected long getLongValue() {
    return longValue;
  }

  public void setLongValue(long longValue) {
    this.hasCandidateResult = true;
    this.longValue = longValue;
  }

  protected float getFloatValue() {
    return floatValue;
  }

  public void setFloatValue(float floatValue) {
    this.hasCandidateResult = true;
    this.floatValue = floatValue;
  }

  protected double getDoubleValue() {
    return doubleValue;
  }

  public void setDoubleValue(double doubleValue) {
    this.hasCandidateResult = true;
    this.doubleValue = doubleValue;
  }

  protected Binary getBinaryValue() {
    return binaryValue;
  }

  public void setBinaryValue(Binary binaryValue) {
    this.hasCandidateResult = true;
    this.binaryValue = binaryValue;
  }

  protected boolean hasCandidateResult() {
    return hasCandidateResult;
  }

  @Override
  public String toString() {
    return String.valueOf(getResult());
  }

  public AggregationType getAggregationType() {
    return aggregationType;
  }

  public boolean isAscending() {
    return true;
  }
}
