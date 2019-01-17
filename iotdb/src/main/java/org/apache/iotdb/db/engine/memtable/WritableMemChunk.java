/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.iotdb.db.utils.PrimitiveArrayList;
import org.apache.iotdb.db.utils.PrimitiveArrayListFactory;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class WritableMemChunk implements IWritableMemChunk {

  private TSDataType dataType;
  private PrimitiveArrayList list;

  public WritableMemChunk(TSDataType dataType) {
    this.dataType = dataType;
    this.list = PrimitiveArrayListFactory.getByDataType(dataType);
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
  public void putLong(long t, long v) {
    list.putTimestamp(t, v);
  }

  @Override
  public void putInt(long t, int v) {
    list.putTimestamp(t, v);
  }

  @Override
  public void putFloat(long t, float v) {
    list.putTimestamp(t, v);
  }

  @Override
  public void putDouble(long t, double v) {
    list.putTimestamp(t, v);
  }

  @Override
  public void putBinary(long t, Binary v) {
    list.putTimestamp(t, v);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    list.putTimestamp(t, v);
  }

  @Override
  // TODO: Consider using arrays to sort and remove duplicates
  public List<TimeValuePair> getSortedTimeValuePairList() {
    int length = list.size();
    TreeMap<Long, TsPrimitiveType> treeMap = new TreeMap<>();
    for (int i = 0; i < length; i++) {
      treeMap.put(list.getTimestamp(i), TsPrimitiveType.getByType(dataType, list.getValue(i)));
    }
    List<TimeValuePair> ret = new ArrayList<>();
    treeMap.forEach((k, v) -> {
      ret.add(new TimeValuePairInMemTable(k, v));
    });
    return ret;
  }

  @Override
  public void reset() {
    this.list = PrimitiveArrayListFactory.getByDataType(dataType);
  }

  @Override
  public int count() {
    return list.size();
  }

}
