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

package org.apache.iotdb.db.mpp.common.object;

import org.apache.iotdb.db.mpp.common.object.io.SegmentedByteInputStream;
import org.apache.iotdb.db.mpp.common.object.io.SegmentedByteOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class ObjectSerDeserUtil {

  private static final byte OBJECT_START_SYMBOL = 0;
  private static final byte NO_MORE_OBJECT_SYMBOL = 1;

  private ObjectSerDeserUtil() {}

  public static List<ByteBuffer> serializeBatchObject(List<ObjectEntry> objectList) {
    SegmentedByteOutputStream segmentedByteOutputStream =
        new SegmentedByteOutputStream((int) (DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES * 0.8));
    DataOutputStream dataOutputStream = new DataOutputStream(segmentedByteOutputStream);
    try {
      for (ObjectEntry object : objectList) {
        dataOutputStream.write(OBJECT_START_SYMBOL);
        object.serializeObject(dataOutputStream);
      }
      dataOutputStream.write(NO_MORE_OBJECT_SYMBOL);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return segmentedByteOutputStream.getBufferList();
  }

  @SuppressWarnings("unchecked")
  public static <T extends ObjectEntry> List<T> deserializeBatchObject(
      List<ByteBuffer> bufferList) {
    List<T> objectList = new ArrayList<>();
    SegmentedByteInputStream segmentedByteInputStream = new SegmentedByteInputStream(bufferList);
    DataInputStream dataInputStream = new DataInputStream(segmentedByteInputStream);
    try {
      byte objectRecordSymbol = dataInputStream.readByte();
      while (objectRecordSymbol == OBJECT_START_SYMBOL) {
        ObjectEntry objectEntry =
            ObjectEntryFactory.getObjectEntry(ObjectType.deserialize(segmentedByteInputStream));
        objectEntry.deserializeObject(dataInputStream);
        objectList.add((T) objectEntry);
        objectRecordSymbol = dataInputStream.readByte();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return objectList;
  }
}
