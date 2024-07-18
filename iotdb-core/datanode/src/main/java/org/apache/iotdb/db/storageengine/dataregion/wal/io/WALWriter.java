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

package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALSignalEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** WALWriter writes the binary {@link WALEntry} into .wal file. */
public class WALWriter extends LogWriter {

  public static final String MAGIC_STRING_V1 = "WAL";
  public static final String MAGIC_STRING_V2 = "V2-WAL";
  public static final int MAGIC_STRING_V1_BYTES =
      MAGIC_STRING_V1.getBytes(StandardCharsets.UTF_8).length;
  public static final int MAGIC_STRING_V2_BYTES =
      MAGIC_STRING_V2.getBytes(StandardCharsets.UTF_8).length;
  private WALFileStatus walFileStatus = WALFileStatus.CONTAINS_NONE_SEARCH_INDEX;
  // wal files' metadata
  protected final WALMetaData metaData = new WALMetaData();
  // By default is V2
  private WALFileVersion version = WALFileVersion.V2;

  public WALWriter(File logFile) throws IOException {
    this(logFile, WALFileVersion.V2);
  }

  public WALWriter(File logFile, WALFileVersion version) throws IOException {
    super(logFile, version);
  }

  /**
   * Writes buffer and update its' metadata.
   *
   * @throws IOException when failing to write
   */
  public double write(ByteBuffer buffer, WALMetaData metaData) throws IOException {
    // update metadata
    updateMetaData(metaData);
    // flush buffer
    return write(buffer);
  }

  public void updateMetaData(WALMetaData metaData) {
    this.metaData.addAll(metaData);
  }

  private void endFile() throws IOException {
    WALSignalEntry endMarker = new WALSignalEntry(WALEntryType.WAL_FILE_INFO_END_MARKER);
    int metaDataSize = metaData.serializedSize();
    ByteBuffer buffer =
        ByteBuffer.allocate(
            endMarker.serializedSize()
                + metaDataSize
                + Integer.BYTES
                + (version != WALFileVersion.V2 ? MAGIC_STRING_V1_BYTES : MAGIC_STRING_V2_BYTES));
    // mark info part ends
    endMarker.serialize(buffer);
    // flush meta data
    metaData.serialize(buffer);
    buffer.putInt(metaDataSize);
    // add magic string
    buffer.put(
        (version != WALFileVersion.V2 ? MAGIC_STRING_V1 : MAGIC_STRING_V2)
            .getBytes(StandardCharsets.UTF_8));
    writeMetadata(buffer);
  }

  private void writeMetadata(ByteBuffer buffer) throws IOException {
    size += buffer.position();
    buffer.flip();
    logChannel.write(buffer);
  }

  @Override
  public void close() throws IOException {
    endFile();
    super.close();
  }

  public void updateFileStatus(WALFileStatus walFileStatus) {
    if (walFileStatus == WALFileStatus.CONTAINS_SEARCH_INDEX) {
      this.walFileStatus = WALFileStatus.CONTAINS_SEARCH_INDEX;
    }
  }

  public WALFileStatus getWalFileStatus() {
    return walFileStatus;
  }

  public void setVersion(WALFileVersion version) {
    this.version = version;
  }
}
