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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class TimeValuePairUtils {

  private TimeValuePairUtils(){}
  /**
   * get given data's current (time,value) pair.
   *
   * @param data -batch data
   * @return -given data's (time,value) pair
   */
  public static TimeValuePair getCurrentTimeValuePair(BatchData data) {
    switch (data.getDataType()) {
      case INT32:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsInt(data.getInt()));
      case INT64:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsLong(data.getLong()));
      case FLOAT:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsFloat(data.getFloat()));
      case DOUBLE:
        return new TimeValuePair(data.currentTime(),
            new TsPrimitiveType.TsDouble(data.getDouble()));
      case TEXT:
        return new TimeValuePair(data.currentTime(),
            new TsPrimitiveType.TsBinary(data.getBinary()));
      case BOOLEAN:
        return new TimeValuePair(data.currentTime(),
            new TsPrimitiveType.TsBoolean(data.getBoolean()));
      default:
        throw new UnSupportedDataTypeException(String.valueOf(data.getDataType()));
    }
  }

  /**
   * get given data's current (time,value) pair.
   *
   * @param data -AggreResultData
   * @return -given data's (time,value) pair
   */
  public static TimeValuePair getCurrentTimeValuePair(AggreResultData data) {
    switch (data.getDataType()) {
      case INT32:
        return new TimeValuePair(data.getTimestamp(), new TsPrimitiveType.TsInt(data.getIntRet()));
      case INT64:
        return new TimeValuePair(data.getTimestamp(), new TsPrimitiveType.TsLong(data.getLongRet()));
      case FLOAT:
        return new TimeValuePair(data.getTimestamp(), new TsPrimitiveType.TsFloat(data.getFloatRet()));
      case DOUBLE:
        return new TimeValuePair(data.getTimestamp(), new TsPrimitiveType.TsDouble(data.getDoubleRet()));
      case TEXT:
        return new TimeValuePair(data.getTimestamp(), new TsPrimitiveType.TsBinary(data.getBinaryRet()));
      case BOOLEAN:
        return new TimeValuePair(data.getTimestamp(), new TsPrimitiveType.TsBoolean(data.isBooleanRet()));
      default:
        throw new UnSupportedDataTypeException(String.valueOf(data.getDataType()));
    }
  }
}
