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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage all primitive data list in memory, including get and release operation.
 */
public class PrimitiveDataListPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimitiveDataListPool.class);

  private static final Map<Class, ConcurrentLinkedQueue<PrimitiveArrayListV2>> primitiveDataListsMap = new ConcurrentHashMap<>();

  static {
    primitiveDataListsMap.put(boolean.class, new ConcurrentLinkedQueue<>());
    primitiveDataListsMap.put(int.class, new ConcurrentLinkedQueue<>());
    primitiveDataListsMap.put(long.class, new ConcurrentLinkedQueue<>());
    primitiveDataListsMap.put(float.class, new ConcurrentLinkedQueue<>());
    primitiveDataListsMap.put(double.class, new ConcurrentLinkedQueue<>());
    primitiveDataListsMap.put(Binary.class, new ConcurrentLinkedQueue<>());
  }

  private PrimitiveDataListPool() {
  }

  public PrimitiveArrayListV2 getPrimitiveDataListByDataType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return getPrimitiveDataList(boolean.class);
      case INT32:
        return getPrimitiveDataList(int.class);
      case INT64:
        return getPrimitiveDataList(long.class);
      case FLOAT:
        return getPrimitiveDataList(float.class);
      case DOUBLE:
        return getPrimitiveDataList(double.class);
      case TEXT:
        return getPrimitiveDataList(Binary.class);
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
  }

  private PrimitiveArrayListV2 getPrimitiveDataList(Class clazz) {
    ConcurrentLinkedQueue<PrimitiveArrayListV2> primitiveArrayList = primitiveDataListsMap.get(clazz);
    PrimitiveArrayListV2 dataList = primitiveArrayList.poll();
    return dataList == null ? new PrimitiveArrayListV2(clazz) : dataList;
  }

  public void release(PrimitiveArrayListV2 primitiveArrayList) {
    primitiveArrayList.reset();
    primitiveDataListsMap.get(primitiveArrayList.getClazz()).offer(primitiveArrayList);
  }

  public static PrimitiveDataListPool getInstance() {
    return PrimitiveDataListPool.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final PrimitiveDataListPool INSTANCE = new PrimitiveDataListPool();
  }

  public int getPrimitiveDataListSizeByDataType(TSDataType dataType){
    switch (dataType) {
      case BOOLEAN:
        return primitiveDataListsMap.get(boolean.class).size();
      case INT32:
        return primitiveDataListsMap.get(int.class).size();
      case INT64:
        return primitiveDataListsMap.get(long.class).size();
      case FLOAT:
        return primitiveDataListsMap.get(float.class).size();
      case DOUBLE:
        return primitiveDataListsMap.get(double.class).size();
      case TEXT:
        return primitiveDataListsMap.get(Binary.class).size();
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
  }
}
