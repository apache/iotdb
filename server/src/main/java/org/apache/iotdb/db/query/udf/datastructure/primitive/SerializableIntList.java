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

package org.apache.iotdb.db.query.udf.datastructure.primitive;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.datastructure.SerializableList;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.MB;

public class SerializableIntList implements SerializableList {

  public static SerializableIntList newSerializableIntList(long queryId) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    return new SerializableIntList(recorder);
  }

  public static int calculateCapacity(float memoryLimitInMB) throws QueryProcessException {
    float memoryLimitInB = memoryLimitInMB * MB / 2;
    int size =
        ARRAY_CAPACITY_THRESHOLD
            * (int) (memoryLimitInB / (ReadWriteIOUtils.INT_LEN * ARRAY_CAPACITY_THRESHOLD));
    if (size <= 0) {
      throw new QueryProcessException("Memory is not enough for current query.");
    }
    return size;
  }

  private static final int ARRAY_CAPACITY_THRESHOLD = TSFileConfig.ARRAY_CAPACITY_THRESHOLD;

  private final SerializationRecorder serializationRecorder;

  private int capacity = 16;

  private List<int[]> list;
  private int writeCurListIndex;
  private int writeCurArrayIndex;
  private int count;

  public SerializableIntList(SerializationRecorder serializationRecorder) {
    this.serializationRecorder = serializationRecorder;
    init();
  }

  public void put(int value) {
    if (writeCurArrayIndex == capacity) {
      if (ARRAY_CAPACITY_THRESHOLD <= capacity) {
        list.add(new int[capacity]);
        ++writeCurListIndex;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;
        int[] newValueData = new int[newCapacity];
        System.arraycopy(list.get(0), 0, newValueData, 0, capacity);
        list.set(0, newValueData);
        capacity = newCapacity;
      }
    }

    list.get(writeCurListIndex)[writeCurArrayIndex] = value;
    ++writeCurArrayIndex;
    ++count;
  }

  public int get(int index) {
    return list.get(index / capacity)[index % capacity];
  }

  public int size() {
    return count;
  }

  @Override
  public void release() {
    list = null;
  }

  @Override
  public void init() {
    list = new ArrayList<>();
    list.add(new int[capacity]);
    writeCurListIndex = 0;
    writeCurArrayIndex = 0;
    count = 0;
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    serializationRecorder.setSerializedElementSize(count);
    int serializedByteLength = 0;
    for (int i = 0; i < count; ++i) {
      serializedByteLength += ReadWriteIOUtils.write(get(i), outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    for (int i = 0; i < serializedElementSize; ++i) {
      put(ReadWriteIOUtils.readInt(byteBuffer));
    }
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }
}
