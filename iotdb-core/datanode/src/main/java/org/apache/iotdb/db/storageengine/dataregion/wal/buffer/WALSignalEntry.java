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

import java.nio.ByteBuffer;

/** This entry class provides a signal to help wal buffer dealing with some special cases. */
public class WALSignalEntry extends WALEntry {
  public WALSignalEntry(WALEntryType signalType) {
    this(signalType, false);
  }

  public WALSignalEntry(WALEntryType signalType, boolean wait) {
    super(signalType, Long.MIN_VALUE, null, wait);
    switch (signalType) {
      case CLOSE_SIGNAL:
      case ROLL_WAL_LOG_WRITER_SIGNAL:
      case WAL_FILE_INFO_END_MARKER:
        break;
      default:
        throw new RuntimeException("Cannot use wal info type as wal signal type");
    }
  }

  @Override
  public int serializedSize() {
    return Byte.BYTES;
  }

  @Override
  public void serialize(IWALByteBufferView buffer) {
    buffer.put(type.getCode());
  }

  public void serialize(ByteBuffer buffer) {
    buffer.put(type.getCode());
  }

  @Override
  public boolean isSignal() {
    return true;
  }

  @Override
  public long getMemorySize() {
    return Byte.BYTES;
  }
}
