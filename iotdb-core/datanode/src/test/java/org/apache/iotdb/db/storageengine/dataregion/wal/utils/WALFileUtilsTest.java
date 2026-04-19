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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class WALFileUtilsTest {
  @Test
  public void binarySearchFileBySearchIndex01() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 0);
    Assert.assertEquals(-1, i);
  }

  @Test
  public void binarySearchFileBySearchIndex02() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 2);
    Assert.assertEquals(0, i);
  }

  @Test
  public void binarySearchFileBySearchIndex03() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 5);
    Assert.assertEquals(0, i);
  }

  @Test
  public void binarySearchFileBySearchIndex04() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 6);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex05() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 10);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex06() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 12);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex07() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, Long.MAX_VALUE);
    Assert.assertEquals(4, i);
  }

  @Test
  public void binarySearchFileBySearchIndex08() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 100, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 200, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 300, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 400, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 10);
    Assert.assertEquals(0, i);
  }

  @Test
  public void binarySearchFileBySearchIndex09() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 100, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 200, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 300, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 400, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 250);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex10() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(5, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(6, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(7, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(8, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(9, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(10, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(11, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 5);
    Assert.assertEquals(3, i);
  }

  @Test
  public void binarySearchFileBySearchIndex11() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(5, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(6, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(7, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(8, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(9, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(10, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(11, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 6);
    Assert.assertEquals(7, i);
  }

  @Test
  public void binarySearchFileBySearchIndex12() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(5, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(6, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(7, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(8, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(9, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(10, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(11, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 12);
    Assert.assertEquals(7, i);
  }

  @Test
  public void binarySearchFileBySearchIndex13() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_NONE_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 5);
    Assert.assertEquals(0, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 6);
    Assert.assertEquals(2, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 13);
    Assert.assertEquals(4, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 100);
    Assert.assertEquals(4, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 0);
    Assert.assertEquals(-1, i);
  }

  @Test
  public void testLocateByWriterProgress() throws Exception {
    final Path dir = Files.createTempDirectory("wal-writer-progress-utils");
    final File wal0 =
        dir.resolve(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File wal1 =
        dir.resolve(WALFileUtils.getLogFileName(1, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();
    final File wal2 =
        dir.resolve(WALFileUtils.getLogFileName(2, 13, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .toFile();

    try {
      try (final WALWriter writer = new WALWriter(wal0, WALFileVersion.V3)) {
        writer.write(entryBuffer(10L), singleEntryMeta(19, 10L, 1L, 10000L, 1, 2L, 110L));
        writer.write(entryBuffer(11L), singleEntryMeta(19, 11L, 1L, 10010L, 1, 2L, 111L));
      }
      try (final WALWriter writer = new WALWriter(wal1, WALFileVersion.V3)) {
        writer.write(entryBuffer(13L), singleEntryMeta(19, 13L, 1L, 10020L, 1, 2L, 113L));
      }
      // Leave wal2 as the active file placeholder; helper methods only scan sealed files.
      try (final WALWriter writer = new WALWriter(wal2, WALFileVersion.V3)) {
        writer.write(entryBuffer(20L), singleEntryMeta(19, 20L, 1L, 20000L, 4, 1L, 120L));
      }

      Assert.assertArrayEquals(
          new long[] {11L, 1L},
          WALFileUtils.locateByWriterProgress(dir.toFile(), 1, 2L, 10010L, 111L));
      Assert.assertArrayEquals(
          new long[] {10L, 0L},
          WALFileUtils.locateByWriterProgress(dir.toFile(), 1, 2L, 9999L, 109L));
      Assert.assertEquals(
          13L, WALFileUtils.findSearchIndexAfterWriterProgress(dir.toFile(), 1, 2L, 10010L, 111L));
      Assert.assertEquals(
          -1L, WALFileUtils.findSearchIndexAfterWriterProgress(dir.toFile(), 4, 1L, 20000L, 120L));
    } finally {
      Files.deleteIfExists(wal0.toPath());
      Files.deleteIfExists(wal1.toPath());
      Files.deleteIfExists(wal2.toPath());
      Files.deleteIfExists(dir);
    }
  }

  private static ByteBuffer entryBuffer(final long bodySearchIndex) {
    final ByteBuffer buffer =
        ByteBuffer.allocate(WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES + Long.BYTES);
    buffer.put(WALEntryType.INSERT_ROW_NODE.getCode());
    buffer.putLong(1L);
    buffer.putShort(PlanNodeType.INSERT_ROW.getNodeType());
    buffer.putLong(bodySearchIndex);
    return buffer;
  }

  private static WALMetaData singleEntryMeta(
      final int size,
      final long searchIndex,
      final long memTableId,
      final long physicalTime,
      final int nodeId,
      final long writerEpoch,
      final long localSeq) {
    final WALMetaData metaData = new WALMetaData();
    metaData.add(size, searchIndex, memTableId, physicalTime, nodeId, writerEpoch, localSeq);
    return metaData;
  }
}
