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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch.ObjectFileChunk;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class LoadTsFileObjectFileBatchTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSerializeDeserializeAndWriteChunks() throws Exception {
    final TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(100L);
    final LoadTsFileObjectFileBatch batch =
        new LoadTsFileObjectFileBatch(
            Arrays.asList(
                new ObjectFileChunk("0/dir/file.bin", 0, 5, false, new byte[] {1, 2, 3}),
                new ObjectFileChunk("0/dir/file.bin", 3, 5, true, new byte[] {4, 5})),
            timePartitionSlot);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (final DataOutputStream stream = new DataOutputStream(byteArrayOutputStream)) {
      batch.serialize(stream);
    }

    final LoadTsFileObjectFileBatch deserialized =
        (LoadTsFileObjectFileBatch)
            TsFileData.deserialize(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    Assert.assertEquals(
        TimePartitionUtils.getTimePartitionSlot(timePartitionSlot.getStartTime()),
        deserialized.getLastTimePartitionSlot());
    Assert.assertEquals(2, deserialized.getObjectFileChunks().size());
    assertChunk(deserialized.getObjectFileChunks().get(0), "0/dir/file.bin", 0, 5, false, 1, 2, 3);
    assertChunk(deserialized.getObjectFileChunks().get(1), "0/dir/file.bin", 3, 5, true, 4, 5);

    final File stagedDir = temporaryFolder.newFolder("staged");
    for (final ObjectFileChunk chunk : deserialized.getObjectFileChunks()) {
      chunk.writeChunk(stagedDir, "1");
    }

    final Path writtenFile = stagedDir.toPath().resolve("1").resolve("dir").resolve("file.bin");
    Assert.assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, Files.readAllBytes(writtenFile));
  }

  @Test
  public void testWriteChunkValidatesRelativePathAndOffsets() throws Exception {
    final File stagedDir = temporaryFolder.newFolder("staged-validate");

    try {
      new ObjectFileChunk("file.bin", 0, 1, true, new byte[] {1}).writeChunk(stagedDir, "1");
      Assert.fail("Expected invalid object relative path to be rejected.");
    } catch (final IOException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid object relative path"));
    }

    final ObjectFileChunk firstChunk =
        new ObjectFileChunk("0/dir/file.bin", 0, 3, false, new byte[] {1, 2});
    firstChunk.writeChunk(stagedDir, "1");

    try {
      new ObjectFileChunk("0/dir/file.bin", 1, 3, true, new byte[] {3}).writeChunk(stagedDir, "1");
      Assert.fail("Expected offset mismatch to be rejected.");
    } catch (final IOException e) {
      Assert.assertTrue(e.getMessage().contains("offset mismatch"));
    }

    try {
      new ObjectFileChunk("0/dir/another.bin", 0, 5, true, new byte[] {1, 2})
          .writeChunk(stagedDir, "1");
      Assert.fail("Expected final chunk size mismatch to be rejected.");
    } catch (final IOException e) {
      Assert.assertTrue(e.getMessage().contains("size mismatch"));
    }
  }

  @Test
  public void testIteratorSplitsFilesAndSkipsEmptyEntries() throws Exception {
    final File baseDir = temporaryFolder.newFolder("iterator");
    final Path nestedDir = baseDir.toPath().resolve("0");
    Files.createDirectories(nestedDir);
    Files.write(nestedDir.resolve("a.bin"), new byte[] {0, 1, 2, 3, 4});
    Files.write(nestedDir.resolve("b.bin"), new byte[] {9, 8});
    Files.write(nestedDir.resolve("empty.bin"), new byte[0]);

    final Set<Pair<File, String>> fileSet = new LinkedHashSet<>();
    fileSet.add(new Pair<>(baseDir, "0/missing.bin"));
    fileSet.add(new Pair<>(baseDir, "0/empty.bin"));
    fileSet.add(new Pair<>(baseDir, "0/a.bin"));
    fileSet.add(new Pair<>(baseDir, "0/b.bin"));

    try (final LoadTsFileObjectFileBatchIterator iterator =
        new LoadTsFileObjectFileBatchIterator(fileSet, 4, new TTimePartitionSlot(7L))) {
      Assert.assertTrue(iterator.hasNext());
      final List<ObjectFileChunk> firstBatch = iterator.next().getObjectFileChunks();
      Assert.assertEquals(1, firstBatch.size());
      assertChunk(firstBatch.get(0), "0/a.bin", 0, 5, false, 0, 1, 2, 3);

      Assert.assertTrue(iterator.hasNext());
      final List<ObjectFileChunk> secondBatch = iterator.next().getObjectFileChunks();
      Assert.assertEquals(2, secondBatch.size());
      assertChunk(secondBatch.get(0), "0/a.bin", 4, 5, true, 4);
      assertChunk(secondBatch.get(1), "0/b.bin", 0, 2, true, 9, 8);

      Assert.assertFalse(iterator.hasNext());
      try {
        iterator.next();
        Assert.fail("Expected iterator exhaustion to throw.");
      } catch (final NoSuchElementException ignored) {
      }
    }
  }

  private static void assertChunk(
      final ObjectFileChunk chunk,
      final String relativePath,
      final long offset,
      final long totalLength,
      final boolean includeLastByte,
      final int... bytes) {
    Assert.assertEquals(relativePath, chunk.getObjectRelativePath());
    Assert.assertEquals(offset, chunk.getFileOffset());
    Assert.assertEquals(totalLength, chunk.getTotalFileLength());
    Assert.assertEquals(includeLastByte, chunk.isIncludeLastByte());

    final byte[] expected = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      expected[i] = (byte) bytes[i];
    }
    Assert.assertArrayEquals(expected, chunk.getBytes());
  }
}
