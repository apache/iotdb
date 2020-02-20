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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class AggreResultData {

  private long timestamp;
  private TSDataType dataType;

  private boolean booleanRet;
  private int intRet;
  private long longRet;
  private float floatRet;
  private double doubleRet;
  private Binary binaryRet;

  private boolean isSetValue;
  private boolean isSetTime;

  public AggreResultData(TSDataType dataType) {
    this.dataType = dataType;
    this.isSetTime = false;
    this.isSetValue = false;
  }

  public void reset() {
    isSetValue = false;
    isSetTime = false;
  }

  public void putTimeAndValue(long timestamp, Object v) {
    setTimestamp(timestamp);
    setAnObject((Comparable<?>) v);
  }

  public Object getValue() {
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
  public void setAnObject(Comparable<?> v) {
    isSetValue = true;
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

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    isSetTime = true;
    this.timestamp = timestamp;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isBooleanRet() {
    return booleanRet;
  }

  public void setBooleanRet(boolean booleanRet) {
    this.isSetValue = true;
    this.booleanRet = booleanRet;
  }

  public int getIntRet() {
    return intRet;
  }

  public void setIntRet(int intRet) {
    this.isSetValue = true;
    this.intRet = intRet;
  }

  public long getLongRet() {
    return longRet;
  }

  public void setLongRet(long longRet) {
    this.isSetValue = true;
    this.longRet = longRet;
  }

  public float getFloatRet() {
    return floatRet;
  }

  public void setFloatRet(float floatRet) {
    this.isSetValue = true;
    this.floatRet = floatRet;
  }

  public double getDoubleRet() {
    return doubleRet;
  }

  public void setDoubleRet(double doubleRet) {
    this.isSetValue = true;
    this.doubleRet = doubleRet;
  }

  public Binary getBinaryRet() {
    return binaryRet;
  }

  public void setBinaryRet(Binary binaryRet) {
    this.isSetValue = true;
    this.binaryRet = binaryRet;
  }

  public boolean isSetValue() {
    return isSetValue;
  }

  public boolean isSetTime() {
    return isSetTime;
  }

  public AggreResultData deepCopy() {
    AggreResultData aggreResultData = new AggreResultData(this.dataType);
    if (isSetValue) {
      aggreResultData.setAnObject((Comparable<?>) this.getValue());
    }
    if (isSetTime) {
      aggreResultData.setTimestamp(this.getTimestamp());
    }
    return aggreResultData;
  }
}
