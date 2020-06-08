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
package org.apache.iotdb.db.rescon;

import java.util.ArrayDeque;
import java.util.EnumMap;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.nvm.NVMSpaceManagerException;
import org.apache.iotdb.db.nvm.space.NVMBinaryDataSpace;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMPrimitiveArrayPool {

  /**
   * data type -> Array<PrimitiveArray>
   */
  private static final EnumMap<TSDataType, ArrayDeque<NVMDataSpace>> primitiveArraysMap = new EnumMap<>(TSDataType.class);

  public static final int ARRAY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getPrimitiveArraySize();

  static {
    primitiveArraysMap.put(TSDataType.BOOLEAN, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.INT32, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.INT64, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.FLOAT, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.DOUBLE, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.TEXT, new ArrayDeque());
  }

  public static NVMPrimitiveArrayPool getInstance() {
    return INSTANCE;
  }

  private static final NVMPrimitiveArrayPool INSTANCE = new NVMPrimitiveArrayPool();

  private NVMPrimitiveArrayPool() {}

  public synchronized NVMDataSpace getPrimitiveDataListByType(TSDataType dataType, boolean isTime) {
    ArrayDeque<NVMDataSpace> dataListQueue = primitiveArraysMap
        .computeIfAbsent(dataType, k -> new ArrayDeque<>());
    NVMDataSpace nvmSpace = dataListQueue.poll();

    if (nvmSpace == null) {
      try {
        long size = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
        nvmSpace = NVMSpaceManager.getInstance().allocateDataSpace(
            size * (dataType == TSDataType.TEXT ? NVMBinaryDataSpace.NUM_OF_TEXT_IN_SPACE
                : ARRAY_SIZE), dataType, isTime);
      } catch (NVMSpaceManagerException e) {
        e.printStackTrace();
        System.exit(0);
      }
    }
    return nvmSpace;
  }

  public synchronized void release(NVMDataSpace nvmSpace, TSDataType dataType) {
    primitiveArraysMap.get(dataType).add(nvmSpace);
  }

//  /**
//   * @param size needed capacity
//   * @return an array of primitive data arrays
//   */
//  public synchronized NVMDataSpace[] getDataListsByType(TSDataType dataType, int size) {
//    int arrayNumber = (int) Math.ceil((float) size / (float) ARRAY_SIZE);
//    NVMDataSpace[] nvmSpaces = new NVMDataSpace[arrayNumber];
//    for (int i = 0; i < arrayNumber; i++) {
//      nvmSpaces[i] = getPrimitiveDataListByType(dataType);
//    }
//    return nvmSpaces;
//  }
}
