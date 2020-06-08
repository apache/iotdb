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
import java.nio.MappedByteBuffer;

public class NVMSpace {

  protected long offset;
  protected long size;
  protected ByteBuffer byteBuffer;

  NVMSpace(long offset, long size, ByteBuffer byteBuffer) {
    this.offset = offset;
    this.size = size;
    this.byteBuffer = byteBuffer;
  }

  public long getOffset() {
    return offset;
  }

  public long getSize() {
    return size;
  }

  public void put(int index, byte v) {
    byteBuffer.put(index, v);
  }

  public byte get(int index) {
    return byteBuffer.get(index);
  }

  public void putShort(int index, short v) {
    byteBuffer.putShort(index * Short.BYTES, v);
  }

  public short getShort(int index) {
    return byteBuffer.getShort(index * Short.BYTES);
  }

  public void putInt(int index, int v) {
    byteBuffer.putInt(index * Integer.BYTES, v);
  }

  public int getInt(int index) {
    return byteBuffer.getInt(index * Integer.BYTES);
  }

  public void putLong(int index, long v) {
    byteBuffer.putLong(index * Long.BYTES, v);
  }

  public long getLong(int index) {
    return byteBuffer.getLong(index * Long.BYTES);
  }

  public void put(byte[] v) {
    byteBuffer.put(v);
  }

  public void get(byte[] src) {
    byteBuffer.get(src);
  }

  public void force() {
    ((MappedByteBuffer) byteBuffer).force();
  }
}
