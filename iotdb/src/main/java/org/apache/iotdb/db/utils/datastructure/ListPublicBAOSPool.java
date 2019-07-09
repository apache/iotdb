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
package org.apache.iotdb.db.utils.datastructure;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Just copy from {@linkplain org.apache.iotdb.db.rescon.PrimitiveArrayPool PrimitiveArrayPool}.
 *
 * Provide and recycle {@code byte[]} in {@linkplain ListPublicBAOS}.
 *
 * @author Pengze Lv, kangrong
 */
public class ListPublicBAOSPool {

  private static final ArrayDeque<byte[]> byteArrayQueue = new ArrayDeque<>();

  public static final int ARRAY_SIZE = 512;

  public static ListPublicBAOSPool getInstance() {
    return INSTANCE;
  }

  private static final ListPublicBAOSPool INSTANCE = new ListPublicBAOSPool();

  private ListPublicBAOSPool() {}

  public synchronized Object getPrimitiveByteList() {
    Object dataArray = byteArrayQueue.poll();

    if (dataArray == null) {
      dataArray = new byte[ARRAY_SIZE];
    }

    return dataArray;
  }


  public synchronized void release(byte[] dataArray) {
    byteArrayQueue.add(dataArray);
  }

  /**
   * @param size needed capacity
   * @return a list of primitive byte arrays
   */
  public synchronized List<byte[]> getByteLists(int size) {
    int arrayNumber = (int) Math.ceil((float) size / (float)ARRAY_SIZE);

    List<byte[]> bytes = new ArrayList<>();

    for (int i = 0; i < arrayNumber; i++) {
      bytes.add((byte[]) getPrimitiveByteList());
    }
    return bytes;
  }

}
