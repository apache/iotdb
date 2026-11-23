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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.junit.Test;

import java.io.File;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProgressWALReaderTest {

  @Test
  public void testPausedReadersReleaseDirectMemoryAndResumeWithinSegment() throws Exception {
    Path dir = Files.createTempDirectory("progress-wal-reader-buffers");
    File walFile = dir.resolve("test.wal").toFile();
    CompressionType originalCompression =
        IoTDBDescriptor.getInstance().getConfig().getWALCompressionAlgorithm();
    long originalMinCompressionSize = WALTestUtils.getMinCompressionSize();
    List<ProgressWALReader> readers = new ArrayList<>();
    try {
      WALTestUtils.setMinCompressionSize(0);
      for (CompressionType compression :
          new CompressionType[] {CompressionType.UNCOMPRESSED, CompressionType.LZ4}) {
        IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(compression);
        final int entrySize = 1024 * 1024;
        byte[] first = new byte[entrySize];
        byte[] second = new byte[entrySize];
        Arrays.fill(first, (byte) 11);
        Arrays.fill(second, (byte) 22);
        ByteBuffer segment = ByteBuffer.allocate(entrySize * 2);
        segment.put(first).put(second);
        WALMetaData metadata = new WALMetaData();
        metadata.add(entrySize, 10, 1, 1000, 1, 10);
        metadata.add(entrySize, 11, 1, 2000, 1, 11);
        try (WALWriter writer = new WALWriter(walFile, WALFileVersion.V3)) {
          writer.setCompressedByteBuffer(ByteBuffer.allocate(segment.capacity() * 2));
          writer.write(segment, metadata);
        }
        BufferPoolMXBean directPool =
            ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(pool -> "direct".equals(pool.getName()))
                .findFirst()
                .get();
        long initialDirectMemory = directPool.getMemoryUsed();
        for (int i = 0; i < 32; i++) {
          ProgressWALReader reader = new ProgressWALReader(walFile);
          readers.add(reader);
          assertArrayEquals(first, reader.next().array());
          reader.releaseReadBuffers();
          reader.releaseReadBuffers();
        }
        // All readers remain open halfway through the same segment. Their retained direct memory
        // must not grow with the number of subscriptions (64 MiB without releasing the buffers).
        assertTrue(directPool.getMemoryUsed() - initialDirectMemory < entrySize);
        for (ProgressWALReader reader : readers) {
          assertArrayEquals(second, reader.next().array());
          assertEquals(1, reader.getCurrentEntryIndex());
          assertEquals(11, reader.getCurrentEntryLocalSeq());
          assertFalse(reader.hasNext());
          reader.releaseReadBuffers();
          reader.close();
        }
        readers.clear();
        Files.delete(walFile.toPath());
      }
    } finally {
      for (ProgressWALReader reader : readers) {
        reader.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(originalCompression);
      WALTestUtils.setMinCompressionSize(originalMinCompressionSize);
      Files.deleteIfExists(walFile.toPath());
      Files.deleteIfExists(dir);
    }
  }

  @Test
  public void testReadWriterProgressMetadataFromV3Wal() throws Exception {
    Path dir = Files.createTempDirectory("progress-wal-reader");
    File walFile = dir.resolve("test.wal").toFile();

    try {
      try (WALWriter writer = new WALWriter(walFile, WALFileVersion.V3)) {
        writer.write(
            entryBuffer((byte) 1, (byte) 2, (byte) 3), singleEntryMeta(3, 10L, 1L, 10000L, 1, 10L));
        writer.write(entryBuffer((byte) 4, (byte) 5), singleEntryMeta(2, 11L, 1L, 10010L, 1, 11L));
        writer.write(
            entryBuffer((byte) 6, (byte) 7, (byte) 8, (byte) 9),
            singleEntryMeta(4, 12L, 2L, 20000L, 4, 1L));
      }

      try (ProgressWALReader reader = new ProgressWALReader(walFile)) {
        assertTrue(reader.hasNext());
        assertArrayEquals(new byte[] {1, 2, 3}, reader.next().array());
        assertEquals(0, reader.getCurrentEntryIndex());
        assertEquals(10000L, reader.getCurrentEntryPhysicalTime());
        assertEquals(1, reader.getCurrentEntryNodeId());
        assertEquals(10L, reader.getCurrentEntryLocalSeq());
        assertEquals(10L, reader.getCurrentEntrySearchIndex());

        assertTrue(reader.hasNext());
        assertArrayEquals(new byte[] {4, 5}, reader.next().array());
        assertEquals(1, reader.getCurrentEntryIndex());
        assertEquals(10010L, reader.getCurrentEntryPhysicalTime());
        assertEquals(1, reader.getCurrentEntryNodeId());
        assertEquals(11L, reader.getCurrentEntryLocalSeq());
        assertEquals(11L, reader.getCurrentEntrySearchIndex());

        assertTrue(reader.hasNext());
        assertArrayEquals(new byte[] {6, 7, 8, 9}, reader.next().array());
        assertEquals(2, reader.getCurrentEntryIndex());
        assertEquals(20000L, reader.getCurrentEntryPhysicalTime());
        assertEquals(4, reader.getCurrentEntryNodeId());
        assertEquals(1L, reader.getCurrentEntryLocalSeq());
        assertEquals(12L, reader.getCurrentEntrySearchIndex());
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
      int size, long searchIndex, long memTableId, long physicalTime, int nodeId, long localSeq) {
    WALMetaData metaData = new WALMetaData();
    metaData.add(size, searchIndex, memTableId, physicalTime, nodeId, localSeq);
    return metaData;
  }
}
