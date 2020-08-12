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

package org.apache.iotdb.db.query.udf.datastructure;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class SerializableBinaryTVList extends BatchData implements SerializableTVList {

  private final SerializationRecorder serializationRecorder;

  public SerializableBinaryTVList(SerializationRecorder serializationRecorder) {
    super(TSDataType.TEXT);
    this.serializationRecorder = serializationRecorder;
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int size = length();
    serializationRecorder.setSerializedElementSize(size);
    int serializedByteLength = 0;
    int lastOffset = size * (ReadWriteIOUtils.LONG_LEN + ReadWriteIOUtils.LONG_LEN);
    for (int i = 0; i < size; ++i) {
      serializedByteLength += ReadWriteIOUtils.write(getTimeByIndex(i), outputStream);
      serializedByteLength += ReadWriteIOUtils.write(lastOffset, outputStream);
      lastOffset += getBinaryByIndex(i).getLength();
    }
    for (int i = 0; i < size; ++i) {
      serializedByteLength += ReadWriteIOUtils.write(getBinaryByIndex(i), outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    for (int i = 0; i < serializedElementSize; ++i) {
      long timestamp = ReadWriteIOUtils.readLong(byteBuffer);
      int offset = ReadWriteIOUtils.readInt(byteBuffer);
      int oldPosition = byteBuffer.position();
      byteBuffer.position(offset);
      Binary value = ReadWriteIOUtils.readBinary(byteBuffer);
      byteBuffer.position(oldPosition);
      putBinary(timestamp, value);
    }
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }
}
