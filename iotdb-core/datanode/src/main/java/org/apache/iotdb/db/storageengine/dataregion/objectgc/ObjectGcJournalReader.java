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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/** Reads {@link ObjectGcJournal} from the checkpoint, stopping on a torn or corrupt tail. */
public class ObjectGcJournalReader implements AutoCloseable {

  private final ObjectGcJournal journal;
  private long seq;
  private long offset;
  private FileChannel channel;
  private final CRC32 crc32 = new CRC32();
  private final ByteBuffer lengthBuf = ByteBuffer.allocate(ObjectGcJournal.LENGTH_BYTES);
  private final ByteBuffer crcBuf = ByteBuffer.allocate(ObjectGcJournal.CRC_BYTES);

  public ObjectGcJournalReader(ObjectGcJournal journal) throws IOException {
    this.journal = journal;
    long[] ckpt = journal.readCheckpoint();
    this.seq = ckpt[0];
    this.offset = ckpt[1];
    openCurrent();
  }

  public ObjectGcRecord next() throws IOException {
    while (true) {
      if (channel == null) {
        return null;
      }
      ObjectGcRecord record = readOne();
      if (record != null) {
        return record;
      }
      File next = journal.logFile(seq + 1);
      if (!next.exists()) {
        return null;
      }
      closeChannel();
      seq++;
      offset = ObjectGcJournal.MAGIC_BYTES;
      openCurrent();
    }
  }

  @Override
  public void close() throws IOException {
    closeChannel();
  }

  private ObjectGcRecord readOne() throws IOException {
    long size = channel.size();
    if (offset + ObjectGcJournal.LENGTH_BYTES > size) {
      return null;
    }
    lengthBuf.clear();
    int n = channel.read(lengthBuf, offset);
    if (n < ObjectGcJournal.LENGTH_BYTES) {
      return null;
    }
    lengthBuf.flip();
    int length = lengthBuf.getInt();
    if (length <= 0
        || offset + ObjectGcJournal.LENGTH_BYTES + length + ObjectGcJournal.CRC_BYTES > size) {
      return null;
    }
    ByteBuffer payload = ByteBuffer.allocate(length);
    n = channel.read(payload, offset + ObjectGcJournal.LENGTH_BYTES);
    if (n < length) {
      return null;
    }
    crcBuf.clear();
    n = channel.read(crcBuf, offset + ObjectGcJournal.LENGTH_BYTES + length);
    if (n < ObjectGcJournal.CRC_BYTES) {
      return null;
    }
    payload.flip();
    crcBuf.flip();
    crc32.reset();
    crc32.update(payload.duplicate());
    long endOffset = offset + ObjectGcJournal.LENGTH_BYTES + length + ObjectGcJournal.CRC_BYTES;
    if (crc32.getValue() != crcBuf.getLong()) {
      return null;
    }
    ObjectGcRecord record = ObjectGcRecord.deserialize(payload);
    record.setLocation(seq, endOffset);
    offset = endOffset;
    return record;
  }

  private void openCurrent() throws IOException {
    File file = journal.logFile(seq);
    if (!file.exists()) {
      channel = null;
      return;
    }
    channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    if (offset < ObjectGcJournal.MAGIC_BYTES) {
      offset = ObjectGcJournal.MAGIC_BYTES;
    }
    if (channel.size() >= ObjectGcJournal.MAGIC_BYTES) {
      ByteBuffer magic = ByteBuffer.allocate(ObjectGcJournal.MAGIC_BYTES);
      channel.read(magic, 0);
      magic.flip();
      String magicStr = StandardCharsets.US_ASCII.decode(magic).toString();
      if (!ObjectGcJournal.MAGIC.equals(magicStr)) {
        closeChannel();
      }
    }
  }

  private void closeChannel() throws IOException {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }
}
