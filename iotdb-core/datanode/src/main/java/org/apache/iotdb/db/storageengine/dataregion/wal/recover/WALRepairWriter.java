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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;

import static org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter.MAGIC_STRING_V1;
import static org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter.MAGIC_STRING_V1_BYTES;
import static org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter.MAGIC_STRING_V2;
import static org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter.MAGIC_STRING_V2_BYTES;

/** Check whether the wal file is broken and repair it. */
public class WALRepairWriter {
  private final File logFile;

  public WALRepairWriter(File logFile) {
    this.logFile = logFile;
  }

  public void repair(WALMetaData metaData) throws IOException {
    // locate broken data
    long truncateSize;
    WALFileVersion version = WALFileVersion.getVersion(logFile);
    if (version == WALFileVersion.UNKNOWN) {
      truncateSize = 0;
    } else if (version == WALFileVersion.V2) {
      if (readTailMagic(MAGIC_STRING_V2_BYTES).equals(MAGIC_STRING_V2)) { // complete file
        return;
      } else { // file with broken magic string
        truncateSize = metaData.getTruncateOffSet();
      }
    } else {
      if (readTailMagic(MAGIC_STRING_V1_BYTES).contains(MAGIC_STRING_V1)) {
        return;
      } else {
        truncateSize = metaData.getTruncateOffSet();
      }
    }
    // truncate broken data
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.APPEND)) {
      channel.truncate(truncateSize);
    }
    // flush metadata
    try (WALWriter walWriter = new WALWriter(logFile, version)) {
      walWriter.updateMetaData(metaData);
    }
  }

  private String readTailMagic(int size) throws IOException {
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ)) {
      ByteBuffer magicStringBytes = ByteBuffer.allocate(size);
      channel.read(magicStringBytes, channel.size() - size);
      magicStringBytes.flip();
      return new String(magicStringBytes.array(), StandardCharsets.UTF_8);
    }
  }
}
