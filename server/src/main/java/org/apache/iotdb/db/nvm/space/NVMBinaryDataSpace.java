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
package org.apache.iotdb.db.nvm.space;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class NVMBinaryDataSpace extends NVMDataSpace {

  public static final int NUM_OF_TEXT_IN_SPACE = 1;

  private int endPos;

  private int cacheSize;
  private Binary[] cachedBinaries;
  private int[] cachedOffset;

  NVMBinaryDataSpace(long offset, long size, ByteBuffer byteBuffer, int index, boolean recover) {
    super(offset, size, byteBuffer, index, TSDataType.TEXT, false);

    reset();
    cachedBinaries = new Binary[NUM_OF_TEXT_IN_SPACE];
    cachedOffset = new int[NUM_OF_TEXT_IN_SPACE];
    if (recover) {
      recoverCache();
    }
  }

  private void recoverCache() {
    cacheSize = byteBuffer.getInt();
    for (int i = 0; i < cacheSize; i++) {
      int len = byteBuffer.getInt();
      byte[] bytes = new byte[len];
      byteBuffer.get(bytes);

      cachedBinaries[i] = new Binary(bytes);
      cachedOffset[i] = endPos;
      endPos += len + NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32);
    }
  }

  @Override
  public int getUnitNum() {
    return cacheSize;
  }

  @Override
  public Object getData(int index) {
    return cachedBinaries[index];
  }

  @Override
  public void setData(int index, Object object) {
    Binary binary = (Binary) object;
    cachedOffset[index] = endPos;
    cachedBinaries[index] = binary;
    endPos = binary.getLength() + 2 * NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32);
    byteBuffer.position(NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32));
    byteBuffer.putInt(binary.getLength());
    byteBuffer.put(binary.getValues());
  }

  public void appendData(Binary binary) {
    cachedOffset[cacheSize] = endPos;
    cachedBinaries[cacheSize] = binary;
    cacheSize++;
    endPos += binary.getLength() + NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32);
    byteBuffer.putInt(0, cacheSize);
    byteBuffer.putInt(binary.getLength());
    byteBuffer.put(binary.getValues());
  }

  @Override
  public Object toArray() {
    return cachedBinaries;
  }

  public void reset() {
    cacheSize = 0;
    endPos = NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32);
  }
}
