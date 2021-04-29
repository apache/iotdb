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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;

public class TimeValuePairUtils {

  private TimeValuePairUtils() {}

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
        return new TimeValuePair(
            data.currentTime(), new TsPrimitiveType.TsDouble(data.getDouble()));
      case TEXT:
        return new TimeValuePair(
            data.currentTime(), new TsPrimitiveType.TsBinary(data.getBinary()));
      case BOOLEAN:
        return new TimeValuePair(
            data.currentTime(), new TsPrimitiveType.TsBoolean(data.getBoolean()));
      default:
        throw new UnSupportedDataTypeException(String.valueOf(data.getDataType()));
    }
  }

  public static void setTimeValuePair(TimeValuePair from, TimeValuePair to) {
    to.setTimestamp(from.getTimestamp());
    switch (from.getValue().getDataType()) {
      case INT32:
        to.getValue().setInt(from.getValue().getInt());
        break;
      case INT64:
        to.getValue().setLong(from.getValue().getLong());
        break;
      case FLOAT:
        to.getValue().setFloat(from.getValue().getFloat());
        break;
      case DOUBLE:
        to.getValue().setDouble(from.getValue().getDouble());
        break;
      case TEXT:
        to.getValue().setBinary(from.getValue().getBinary());
        break;
      case BOOLEAN:
        to.getValue().setBoolean(from.getValue().getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(from.getValue().getDataType()));
    }
  }

  public static TimeValuePair getEmptyTimeValuePair(TSDataType dataType) {
    switch (dataType) {
      case FLOAT:
        return new TimeValuePair(0, new TsFloat(0.0f));
      case INT32:
        return new TimeValuePair(0, new TsInt(0));
      case INT64:
        return new TimeValuePair(0, new TsLong(0));
      case BOOLEAN:
        return new TimeValuePair(0, new TsBoolean(false));
      case DOUBLE:
        return new TimeValuePair(0, new TsDouble(0.0));
      case TEXT:
        return new TimeValuePair(0, new TsBinary(new Binary("")));
      default:
        throw new UnsupportedOperationException("Unrecognized datatype: " + dataType);
    }
  }
}
