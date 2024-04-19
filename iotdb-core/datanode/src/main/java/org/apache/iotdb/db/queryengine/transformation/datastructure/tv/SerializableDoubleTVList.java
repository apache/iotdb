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

package org.apache.iotdb.db.queryengine.transformation.datastructure.tv;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class SerializableDoubleTVList extends SerializableTVList {

  protected static int calculateCapacity(float memoryLimitInMB) {
    float memoryLimitInB = memoryLimitInMB * MB / 2;
    // One time-value pair with 1 extra bit(1/8 Byte) in bitMap
    return TSFileConfig.ARRAY_CAPACITY_THRESHOLD
        * (int)
            (memoryLimitInB
                / ((ReadWriteIOUtils.LONG_LEN
                        + ReadWriteIOUtils.DOUBLE_LEN
                        + ReadWriteIOUtils.BIT_LEN)
                    * TSFileConfig.ARRAY_CAPACITY_THRESHOLD));
  }

  protected SerializableDoubleTVList(SerializationRecorder serializationRecorder) {
    super(TSDataType.DOUBLE, serializationRecorder);
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int size = length();
    serializationRecorder.setSerializedElementSize(size);
    int serializedByteLength = 0;
    for (int i = 0; i < size; ++i) {
      serializedByteLength += ReadWriteIOUtils.write(getTimeByIndex(i), outputStream);
      serializedByteLength += ReadWriteIOUtils.write(getDoubleByIndex(i), outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    for (int i = 0; i < serializedElementSize; ++i) {
      putDouble(ReadWriteIOUtils.readLong(byteBuffer), ReadWriteIOUtils.readDouble(byteBuffer));
    }
  }

  @Override
  public void release() {
    timeRet = null;
    doubleRet = null;
  }
}
