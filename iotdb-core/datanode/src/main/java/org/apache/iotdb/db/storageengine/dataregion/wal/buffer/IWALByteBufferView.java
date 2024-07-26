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

package org.apache.iotdb.db.storageengine.dataregion.wal.buffer;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This ByteBuffer view provides blocking writing interface for wal to serialize huge object with
 * limited memory usage. This interface should behave like {@link ByteBuffer} and don't guarantee
 * the concurrent safety.
 */
public abstract class IWALByteBufferView extends OutputStream {
  /** Like {@link ByteBuffer#put(byte)}. */
  public abstract void put(byte b);

  /** Like {@link ByteBuffer#put(byte[])}. */
  public abstract void put(byte[] src);

  /** Like {@link ByteBuffer#putChar(char)}. */
  public abstract void putChar(char value);

  /** Like {@link ByteBuffer#putShort(short)}. */
  public abstract void putShort(short value);

  /** Like {@link ByteBuffer#putInt(int)}. */
  public abstract void putInt(int value);

  /** Like {@link ByteBuffer#putLong(long)}. */
  public abstract void putLong(long value);

  /** Like {@link ByteBuffer#putFloat(float)}. */
  public abstract void putFloat(float value);

  /** Like {@link ByteBuffer#putDouble(double)}. */
  public abstract void putDouble(double value);

  /** Like {@link ByteBuffer#position()}. */
  public abstract int position();
}
