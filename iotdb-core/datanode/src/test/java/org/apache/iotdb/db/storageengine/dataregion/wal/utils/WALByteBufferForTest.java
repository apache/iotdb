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
package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import java.nio.ByteBuffer;

public class WALByteBufferForTest extends IWALByteBufferView {
  private final ByteBuffer buffer;

  @Override
  public void write(int b) {
    put((byte) b);
  }

  @Override
  public void write(byte[] b) {
    put(b);
  }

  public WALByteBufferForTest(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void put(byte b) {
    buffer.put(b);
  }

  @Override
  public void put(byte[] src) {
    buffer.put(src);
  }

  @Override
  public void putChar(char value) {
    buffer.putChar(value);
  }

  @Override
  public void putShort(short value) {
    buffer.putShort(value);
  }

  @Override
  public void putInt(int value) {
    buffer.putInt(value);
  }

  @Override
  public void putLong(long value) {
    buffer.putLong(value);
  }

  @Override
  public void putFloat(float value) {
    buffer.putFloat(value);
  }

  @Override
  public void putDouble(double value) {
    buffer.putDouble(value);
  }

  @Override
  public int position() {
    return buffer.position();
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }
}
