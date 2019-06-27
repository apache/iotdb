/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.utils.PrimitiveArrayListV2;
import org.apache.iotdb.db.utils.PrimitiveDataListPool;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.db.utils.datastructure.LongTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class WritableMemChunkV2 {

  private TSDataType dataType;
  private TVList list;
  private long timeOffset = 0;

  public WritableMemChunkV2(TSDataType dataType) {
    this.dataType = dataType;
    this.list = getTVList(dataType);
  }

  public TVList getTVList(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return null;
      case INT32:
        return null;
      case INT64:
        return new LongTVList();
      case FLOAT:
        return null;
      case DOUBLE:
        return null;
      case TEXT:
        return null;
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
  }

  public void write(long insertTime, String insertValue) {
    switch (dataType) {
      case BOOLEAN:
        putBoolean(insertTime, Boolean.valueOf(insertValue));
        break;
      case INT32:
        putInt(insertTime, Integer.valueOf(insertValue));
        break;
      case INT64:
        putLong(insertTime, Long.valueOf(insertValue));
        break;
      case FLOAT:
        putFloat(insertTime, Float.valueOf(insertValue));
        break;
      case DOUBLE:
        putDouble(insertTime, Double.valueOf(insertValue));
        break;
      case TEXT:
        putBinary(insertTime, Binary.valueOf(insertValue));
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  public void write(long insertTime, Object value) {
    switch (dataType) {
      case BOOLEAN:
        putBoolean(insertTime, (Boolean)value);
        break;
      case INT32:
        putInt(insertTime, (Integer)value);
        break;
      case INT64:
        putLong(insertTime, (Long)value);
        break;
      case FLOAT:
        putFloat(insertTime, (Float)value);
        break;
      case DOUBLE:
        putDouble(insertTime, (Double)value);
        break;
      case TEXT:
        putBinary(insertTime, (Binary)value);
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  public void putLong(long t, long v) {
    list.putLong(t, v);
  }

  public void putInt(long t, int v) {
    list.putInt(t, v);
  }

  public void putFloat(long t, float v) {
    list.putFloat(t, v);
  }

  public void putDouble(long t, double v) {
    list.putDouble(t, v);
  }

  public void putBinary(long t, Binary v) {
    list.putBinary(t, v);
  }

  public void putBoolean(long t, boolean v) {
    list.putBoolean(t, v);
  }

  public TSDataType getType() {
    return dataType;
  }

  public void setTimeOffset(long offset) {
    timeOffset = offset;
  }

  public TVList getSortedList() {
    list.sort();
    return list;
  }
}
