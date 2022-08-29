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
package org.apache.iotdb.db.wal.recover;

import org.apache.iotdb.db.wal.io.WALMetaData;
import org.apache.iotdb.db.wal.io.WALWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.apache.iotdb.db.wal.io.WALWriter.MAGIC_STRING;
import static org.apache.iotdb.db.wal.io.WALWriter.MAGIC_STRING_BYTES;

/** Check whether the wal file is broken and recover it. */
public class WALRecoverWriter {
  private final File logFile;

  public WALRecoverWriter(File logFile) {
    this.logFile = logFile;
  }

  public void recover(WALMetaData metaData) throws IOException {
    // locate broken data
    int truncateSize;
    if (logFile.length() < MAGIC_STRING_BYTES) { // file without magic string
      truncateSize = 0;
    } else {
      if (readTailMagic().equals(MAGIC_STRING)) { // complete file
        return;
      } else { // file with broken magic string
        truncateSize = metaData.getBuffersSize().stream().mapToInt(Integer::intValue).sum();
      }
    }
    // truncate broken data
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.APPEND)) {
      channel.truncate(truncateSize);
    }
    // flush metadata
    try (WALWriter walWriter = new WALWriter(logFile)) {
      walWriter.updateMetaData(metaData);
    }
  }

  private String readTailMagic() throws IOException {
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ)) {
      ByteBuffer magicStringBytes = ByteBuffer.allocate(MAGIC_STRING_BYTES);
      channel.read(magicStringBytes, channel.size() - MAGIC_STRING_BYTES);
      magicStringBytes.flip();
      return new String(magicStringBytes.array());
    }
  }
}
