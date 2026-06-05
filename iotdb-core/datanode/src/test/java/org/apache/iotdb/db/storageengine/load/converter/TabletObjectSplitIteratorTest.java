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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;

import com.timecho.iotdb.calc.storageengine.dataregion.Base32ObjectPath;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class TabletObjectSplitIteratorTest {

  @Test
  public void testOnlyIdColumnReturnsBaseOnly() throws Exception {
    final Tablet tablet =
        new Tablet(
            "t_obj",
            Arrays.asList("id"),
            Arrays.asList(TSDataType.STRING),
            Arrays.asList(ColumnCategory.TAG),
            2);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "device_1");
    tablet.setRowSize(1);

    try (TabletObjectSplitIterator iterator = new TabletObjectSplitIterator(tablet, null, null)) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertSame(tablet, iterator.next());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testIdAndObjectColumnSplit() throws Exception {
    final Path tempDir = Files.createTempDirectory("tablet_object_split_iterator_test_");
    try {
      final byte[] fileContent = "abcdefghij".getBytes(StandardCharsets.UTF_8);

      final Tablet tablet =
          new Tablet(
              "t_obj",
              Arrays.asList("id", "file"),
              Arrays.asList(TSDataType.STRING, TSDataType.OBJECT),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
              4);

      tablet.addTimestamp(0, 1L);
      tablet.addValue(0, 0, "device_1");
      setObjectBinaryAndCreateFile(tempDir, tablet, 0, 1, 1L, "device_1", fileContent);

      tablet.addTimestamp(1, 2L);
      tablet.addValue(1, 0, "device_1");
      // Keep row-1 object null, iterator should skip it.
      tablet.setRowSize(2);

      try (TabletObjectSplitIterator iterator =
          new TabletObjectSplitIterator(tablet, null, tempDir.toFile())) {
        long expectedOffset = 0;
        int chunkCount = 0;
        final byte[] actual = new byte[fileContent.length];
        int copied = 0;
        while (iterator.hasNext()) {
          Assert.assertTrue(iterator.hasNext());
          final Tablet chunk = iterator.next();
          Assert.assertEquals(1, chunk.getRowSize());
          Assert.assertEquals(1L, chunk.getTimestamps()[0]);
          Assert.assertEquals("device_1", ((Binary[]) chunk.getValues()[0])[0].toString());

          final ObjectChunk chunkMeta = decodeChunkBinary(((Binary[]) chunk.getValues()[1])[0]);
          Assert.assertEquals(expectedOffset, chunkMeta.offset);
          expectedOffset += chunkMeta.content.length;
          System.arraycopy(chunkMeta.content, 0, actual, copied, chunkMeta.content.length);
          copied += chunkMeta.content.length;
          chunkCount++;
          if (!iterator.hasNext()) {
            Assert.assertTrue(chunkMeta.isEOF);
          }
        }
        Assert.assertTrue(chunkCount >= 1);
        Assert.assertEquals(fileContent.length, copied);
        Assert.assertArrayEquals(fileContent, actual);
        Assert.assertFalse(iterator.hasNext());
      }
    } finally {
      deleteDirectory(tempDir);
    }
  }

  @Test
  public void testOnlyObjectColumnWithoutIdSplit() throws Exception {
    final Path tempDir = Files.createTempDirectory("tablet_object_only_split_test_");
    try {
      final byte[] fileContent = "hello".getBytes(StandardCharsets.UTF_8);

      final Tablet tablet =
          new Tablet(
              "t_obj",
              Arrays.asList("file"),
              Arrays.asList(TSDataType.OBJECT),
              Arrays.asList(ColumnCategory.FIELD),
              2);
      tablet.addTimestamp(0, 10L);
      setObjectBinaryAndCreateFile(tempDir, tablet, 0, 0, 10L, "device_only", fileContent);
      tablet.setRowSize(1);

      try (TabletObjectSplitIterator iterator =
          new TabletObjectSplitIterator(tablet, null, tempDir.toFile(), true)) {
        long expectedOffset = 0;
        int chunkCount = 0;
        final byte[] actual = new byte[fileContent.length];
        int copied = 0;
        while (iterator.hasNext()) {
          final Tablet chunk = iterator.next();
          final ObjectChunk chunkMeta = decodeChunkBinary(((Binary[]) chunk.getValues()[0])[0]);
          Assert.assertEquals(expectedOffset, chunkMeta.offset);
          expectedOffset += chunkMeta.content.length;
          System.arraycopy(chunkMeta.content, 0, actual, copied, chunkMeta.content.length);
          copied += chunkMeta.content.length;
          chunkCount++;
          if (!iterator.hasNext()) {
            Assert.assertTrue(chunkMeta.isEOF);
          }
        }
        Assert.assertTrue(chunkCount >= 1);
        Assert.assertEquals(fileContent.length, copied);
        Assert.assertArrayEquals(fileContent, actual);
        Assert.assertFalse(iterator.hasNext());
      }
    } finally {
      deleteDirectory(tempDir);
    }
  }

  @Test
  public void testObjectValueContentBinarySplitWithoutObjectDir() throws Exception {
    final byte[] objectPayload = "object-payload-1".getBytes(StandardCharsets.UTF_8);

    final Tablet tablet =
        new Tablet(
            "t_obj",
            Arrays.asList("id", "file"),
            Arrays.asList(TSDataType.STRING, TSDataType.OBJECT),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            2);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "device_1");
    tablet.addValue(0, 1, true, 0, objectPayload);
    tablet.setRowSize(1);

    try (TabletObjectSplitIterator iterator = new TabletObjectSplitIterator(tablet, null, null)) {
      Assert.assertTrue(iterator.hasNext());
      final Tablet chunk = iterator.next();
      Assert.assertEquals(1, chunk.getRowSize());
      Assert.assertEquals(1L, chunk.getTimestamps()[0]);
      Assert.assertEquals("device_1", ((Binary[]) chunk.getValues()[0])[0].toString());

      final ObjectChunk objectChunk = decodeChunkBinary(((Binary[]) chunk.getValues()[1])[0]);
      Assert.assertTrue(objectChunk.isEOF);
      Assert.assertEquals(0, objectChunk.offset);
      Assert.assertArrayEquals(objectPayload, objectChunk.content);
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testIdAndNormalWithoutObjectReturnsBaseOnly() throws Exception {
    final Tablet tablet =
        new Tablet(
            "t_obj",
            Arrays.asList("id", "value"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            2);
    tablet.addTimestamp(0, 100L);
    tablet.addValue(0, 0, "device_2");
    tablet.addValue(0, 1, 7);
    tablet.setRowSize(1);

    try (TabletObjectSplitIterator iterator = new TabletObjectSplitIterator(tablet, null, null)) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertSame(tablet, iterator.next());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testIdNormalAndObjectReturnBaseThenSplit() throws Exception {
    final Path tempDir = Files.createTempDirectory("tablet_object_split_iterator_base_test_");
    try {
      final byte[] fileContent = "payload".getBytes(StandardCharsets.UTF_8);

      final Tablet tablet =
          new Tablet(
              "t_obj",
              Arrays.asList("id", "value", "file"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.OBJECT),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
              2);

      tablet.addTimestamp(0, 100L);
      tablet.addValue(0, 0, "device_2");
      tablet.addValue(0, 1, 7);
      setObjectBinaryAndCreateFile(tempDir, tablet, 0, 2, 100L, "device_2", fileContent);
      final Binary originalObjectBinary = ((Binary[]) tablet.getValues()[2])[0];
      tablet.setRowSize(1);

      try (TabletObjectSplitIterator iterator =
          new TabletObjectSplitIterator(tablet, null, tempDir.toFile(), true)) {
        Assert.assertTrue(iterator.hasNext());
        final Tablet first = iterator.next();
        Assert.assertNotSame(tablet, first);
        Assert.assertSame(originalObjectBinary, ((Binary[]) tablet.getValues()[2])[0]);

        Assert.assertTrue(iterator.hasNext());
        long expectedOffset = 0;
        int copied = 0;
        final byte[] actual = new byte[fileContent.length];
        while (iterator.hasNext()) {
          final Tablet objectChunk = iterator.next();
          Assert.assertEquals(1, objectChunk.getRowSize());
          Assert.assertEquals(100L, objectChunk.getTimestamps()[0]);
          final ObjectChunk chunkMeta =
              decodeChunkBinary(((Binary[]) objectChunk.getValues()[1])[0]);
          Assert.assertEquals(expectedOffset, chunkMeta.offset);
          expectedOffset += chunkMeta.content.length;
          System.arraycopy(chunkMeta.content, 0, actual, copied, chunkMeta.content.length);
          copied += chunkMeta.content.length;
          if (!iterator.hasNext()) {
            Assert.assertTrue(chunkMeta.isEOF);
          }
        }
        Assert.assertEquals(fileContent.length, copied);
        Assert.assertArrayEquals(fileContent, actual);

        Assert.assertFalse(iterator.hasNext());
      }
    } finally {
      deleteDirectory(tempDir);
    }
  }

  @Test
  public void testDisableCopyWillMutateOriginalTablet() throws Exception {
    final Path tempDir = Files.createTempDirectory("tablet_object_split_iterator_mutate_test_");
    try {
      final byte[] fileContent = "payload".getBytes(StandardCharsets.UTF_8);

      final Tablet tablet =
          new Tablet(
              "t_obj",
              Arrays.asList("id", "value", "file"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.OBJECT),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
              2);

      tablet.addTimestamp(0, 100L);
      tablet.addValue(0, 0, "device_2");
      tablet.addValue(0, 1, 7);
      setObjectBinaryAndCreateFile(tempDir, tablet, 0, 2, 100L, "device_2", fileContent);
      tablet.setRowSize(1);

      try (TabletObjectSplitIterator iterator =
          new TabletObjectSplitIterator(tablet, null, tempDir.toFile(), false)) {
        Assert.assertTrue(iterator.hasNext());
        final Tablet first = iterator.next();
        Assert.assertSame(tablet, first);
        Assert.assertEquals(Binary.EMPTY_VALUE, ((Binary[]) tablet.getValues()[2])[0]);
      }
    } finally {
      deleteDirectory(tempDir);
    }
  }

  private static void setObjectBinaryAndCreateFile(
      final Path searchRoot,
      final Tablet tablet,
      final int row,
      final int objectColIdx,
      final long timestamp,
      final String deviceSegment,
      final byte[] content)
      throws Exception {
    final IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"t_obj", deviceSegment});
    final IObjectPath objectPath = new Base32ObjectPath(0, timestamp, deviceID, "file");
    final Path objectFilePath = searchRoot.resolve(objectPath.toString());
    Files.createDirectories(objectFilePath.getParent());
    Files.write(objectFilePath, content);

    final Binary objectBinary = ObjectTypeUtils.generateObjectBinary(content.length, objectPath);
    ((Binary[]) tablet.getValues()[objectColIdx])[row] = objectBinary;
    if (tablet.getBitMaps() == null) {
      tablet.initBitMaps();
    }
    final BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps[objectColIdx] == null) {
      bitMaps[objectColIdx] = new BitMap(tablet.getMaxRowNumber());
      bitMaps[objectColIdx].markAll();
    }
    bitMaps[objectColIdx].unmark(row);
  }

  private static ObjectChunk decodeChunkBinary(final Binary binary) {
    final ByteBuffer buffer = ByteBuffer.wrap(binary.getValues());
    final boolean isEOF = buffer.get() == 1;
    final long offset = buffer.getLong();
    final byte[] content = new byte[buffer.remaining()];
    buffer.get(content);
    return new ObjectChunk(isEOF, offset, content);
  }

  private static void deleteDirectory(final Path root) throws Exception {
    if (root == null || !Files.exists(root)) {
      return;
    }
    Files.walk(root)
        .sorted((a, b) -> b.compareTo(a))
        .forEach(
            path -> {
              try {
                Files.deleteIfExists(path);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static final class ObjectChunk {
    private final boolean isEOF;
    private final long offset;
    private final byte[] content;

    private ObjectChunk(final boolean isEOF, final long offset, final byte[] content) {
      this.isEOF = isEOF;
      this.offset = offset;
      this.content = content;
    }
  }
}
