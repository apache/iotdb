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
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsLong;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableMemChunkV2 implements IWritableMemChunk {

  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunkV2.class);
  private TSDataType dataType;
  private TVList list;

  public WritableMemChunkV2(TSDataType dataType) {
    this.dataType = dataType;
    this.list = TVList.newList(dataType);
  }

  @Override
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

  @Override
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


  @Override
  public void putLong(long t, long v) {
    list.putLong(t, v);
  }

  @Override
  public void putInt(long t, int v) {
    list.putInt(t, v);
  }

  @Override
  public void putFloat(long t, float v) {
    list.putFloat(t, v);
  }

  @Override
  public void putDouble(long t, double v) {
    list.putDouble(t, v);
  }

  @Override
  public void putBinary(long t, Binary v) {
    list.putBinary(t, v);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    list.putBoolean(t, v);
  }

  @Override
  public TVList getSortedTVList() {
    list.sort();
    return list;
  }

  @Override
  public long count() {
    return list.size();
  }

  @Override
  public TSDataType getType() {
    return dataType;
  }

  @Override
  public void setTimeOffset(long offset) {
    list.setTimeOffset(offset);
  }

  @Override
  public List<TimeValuePair> getSortedTimeValuePairList() {
   List<TimeValuePair> result = new ArrayList<>();
   TVList cloneList = list.clone();
   cloneList.sort();
   for (int i = 0; i < cloneList.size(); i++) {
     long time = cloneList.getTime(i);
     if (time < cloneList.getTimeOffset() ||
         (i+1 < cloneList.size() && (time == cloneList.getTime(i+1)))) {
       continue;
     }

     switch (dataType) {
       case BOOLEAN:
         result.add(new TimeValuePair(time, new TsBoolean(cloneList.getBoolean(i))));
         break;
       case INT32:
         result.add(new TimeValuePair(time, new TsInt(cloneList.getInt(i))));
         break;
       case INT64:
         result.add(new TimeValuePair(time, new TsLong(cloneList.getLong(i))));
         break;
       case FLOAT:
         result.add(new TimeValuePair(time, new TsFloat(cloneList.getFloat(i))));
         break;
       case DOUBLE:
         result.add(new TimeValuePair(time, new TsDouble(cloneList.getDouble(i))));
         break;
       case TEXT:
         result.add(new TimeValuePair(time, new TsBinary(cloneList.getBinary(i))));
         break;
       default:
         LOGGER.error("don't support data type: {}", dataType);
         break;
     }
   }
   return result;
  }

  @Override
  public void releasePrimitiveArrayList(){}

}
