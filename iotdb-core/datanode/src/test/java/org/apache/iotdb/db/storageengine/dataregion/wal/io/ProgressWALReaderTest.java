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

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProgressWALReaderTest {

  @Test
  public void testReadWriterProgressMetadataFromV3Wal() throws Exception {
    Path dir = Files.createTempDirectory("progress-wal-reader");
    File walFile = dir.resolve("test.wal").toFile();

    try {
      try (WALWriter writer = new WALWriter(walFile, WALFileVersion.V3)) {
        writer.write(
            entryBuffer((byte) 1, (byte) 2, (byte) 3),
            singleEntryMeta(3, 10L, 1L, 1000L, 10L, 10000L, 1, 2L, 10L));
        writer.write(
            entryBuffer((byte) 4, (byte) 5),
            singleEntryMeta(2, 11L, 1L, 1000L, 11L, 10010L, 1, 2L, 11L));
        writer.write(
            entryBuffer((byte) 6, (byte) 7, (byte) 8, (byte) 9),
            singleEntryMeta(4, 12L, 2L, 2000L, 1L, 20000L, 4, 1L, 1L));
      }

      try (ProgressWALReader reader = new ProgressWALReader(walFile)) {
        assertTrue(reader.hasNext());
        assertArrayEquals(new byte[] {1, 2, 3}, reader.next().array());
        assertEquals(0, reader.getCurrentEntryIndex());
        assertEquals(10000L, reader.getCurrentEntryPhysicalTime());
        assertEquals(1, reader.getCurrentEntryNodeId());
        assertEquals(2L, reader.getCurrentEntryWriterEpoch());
        assertEquals(10L, reader.getCurrentEntryLocalSeq());

        assertTrue(reader.hasNext());
        assertArrayEquals(new byte[] {4, 5}, reader.next().array());
        assertEquals(1, reader.getCurrentEntryIndex());
        assertEquals(10010L, reader.getCurrentEntryPhysicalTime());
        assertEquals(1, reader.getCurrentEntryNodeId());
        assertEquals(2L, reader.getCurrentEntryWriterEpoch());
        assertEquals(11L, reader.getCurrentEntryLocalSeq());

        assertTrue(reader.hasNext());
        assertArrayEquals(new byte[] {6, 7, 8, 9}, reader.next().array());
        assertEquals(2, reader.getCurrentEntryIndex());
        assertEquals(20000L, reader.getCurrentEntryPhysicalTime());
        assertEquals(4, reader.getCurrentEntryNodeId());
        assertEquals(1L, reader.getCurrentEntryWriterEpoch());
        assertEquals(1L, reader.getCurrentEntryLocalSeq());
      }
    } finally {
      Files.deleteIfExists(walFile.toPath());
      Files.deleteIfExists(dir);
    }
  }

  private static ByteBuffer entryBuffer(byte... bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    return buffer;
  }

  private static WALMetaData singleEntryMeta(
      int size,
      long searchIndex,
      long memTableId,
      long epoch,
      long syncIndex,
      long physicalTime,
      int nodeId,
      long writerEpoch,
      long localSeq) {
    return singleEntryMeta(
        size, searchIndex, memTableId, physicalTime, nodeId, writerEpoch, localSeq);
  }

  private static WALMetaData singleEntryMeta(
      int size,
      long searchIndex,
      long memTableId,
      long physicalTime,
      int nodeId,
      long writerEpoch,
      long localSeq) {
    WALMetaData metaData = new WALMetaData();
    metaData.add(size, searchIndex, memTableId, physicalTime, nodeId, writerEpoch, localSeq);
    return metaData;
  }
}
