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

package org.apache.iotdb.db.query.udf.datastructure.tv;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.db.conf.IoTDBConstant.MB;

public class SerializableBinaryTVList extends SerializableTVList {

  protected static final int MIN_OBJECT_HEADER_SIZE = 8;
  protected static final int MIN_ARRAY_HEADER_SIZE = MIN_OBJECT_HEADER_SIZE + 4;

  protected static int calculateCapacity(float memoryLimitInMB, int byteArrayLength) {
    float memoryLimitInB = memoryLimitInMB * MB / 2;
    return TSFileConfig.ARRAY_CAPACITY_THRESHOLD
        * (int)
            (memoryLimitInB
                / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
                    * calculateSingleBinaryTVPairMemory(byteArrayLength)));
  }

  protected static int calculateSingleBinaryTVPairMemory(int byteArrayLength) {
    return ReadWriteIOUtils.LONG_LEN // time
        + MIN_OBJECT_HEADER_SIZE // value: header length of Binary
        + MIN_ARRAY_HEADER_SIZE // value: header length of values in Binary
        + byteArrayLength; // value: length of values array in Binary
  }

  protected SerializableBinaryTVList(SerializationRecorder serializationRecorder) {
    super(TSDataType.TEXT, serializationRecorder);
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int size = length();
    serializationRecorder.setSerializedElementSize(size);
    int serializedByteLength = 0;
    for (int i = 0; i < size; ++i) {
      serializedByteLength += ReadWriteIOUtils.write(getTimeByIndex(i), outputStream);
      serializedByteLength += ReadWriteIOUtils.write(getBinaryByIndex(i), outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    for (int i = 0; i < serializedElementSize; ++i) {
      putBinary(ReadWriteIOUtils.readLong(byteBuffer), ReadWriteIOUtils.readBinary(byteBuffer));
    }
  }

  @Override
  public void release() {
    timeRet = null;
    binaryRet = null;
  }
}
