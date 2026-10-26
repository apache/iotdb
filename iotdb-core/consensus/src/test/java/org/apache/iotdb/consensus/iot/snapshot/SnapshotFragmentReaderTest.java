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

package org.apache.iotdb.consensus.iot.snapshot;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Regression test for the snapshot-transmission read path. The sending side reuses a single,
 * caller-supplied {@link ByteBuffer} across every file of a snapshot (instead of allocating a fresh
 * 10MB buffer per file), so this verifies that (1) a single buffer reused across many readers still
 * reconstructs every file byte-for-byte, and (2) the buffer is genuinely shared rather than
 * re-allocated per file.
 */
public class SnapshotFragmentReaderTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void reusedBufferReadsEveryFileCorrectly() throws IOException {
    final Random random = new Random(42);
    // Deliberately use a tiny buffer so files span multiple fragments, and use file sizes that are
    // not multiples of the buffer size to exercise the partial-final-fragment path.
    final int bufferSize = 16;
    final int[] fileSizes = {0, 1, bufferSize, bufferSize + 1, 5 * bufferSize + 7};

    byte[][] contents = new byte[fileSizes.length][];
    Path[] paths = new Path[fileSizes.length];
    for (int i = 0; i < fileSizes.length; i++) {
      contents[i] = new byte[fileSizes[i]];
      random.nextBytes(contents[i]);
      paths[i] = temporaryFolder.newFile("file-" + i).toPath();
      Files.write(paths[i], contents[i]);
    }

    final ByteBuffer sharedBuffer = ByteBuffer.allocate(bufferSize);
    ByteBuffer firstReusedChunk = null;

    for (int i = 0; i < paths.length; i++) {
      SnapshotFragmentReader reader = new SnapshotFragmentReader("snap", paths[i], sharedBuffer);
      try {
        ByteArrayOutputStream reconstructed = new ByteArrayOutputStream();
        while (reader.hasNext()) {
          SnapshotFragment fragment = reader.next();
          // Every fragment must be backed by the one shared buffer, proving it is reused and not
          // re-allocated per file.
          Assert.assertSame(sharedBuffer, fragment.getFileChunk());
          if (firstReusedChunk == null) {
            firstReusedChunk = fragment.getFileChunk();
          } else {
            Assert.assertSame(firstReusedChunk, fragment.getFileChunk());
          }

          // Drain the fragment immediately, mirroring how the sender serializes each fragment
          // synchronously before the next read overwrites the shared buffer.
          ByteBuffer chunk = fragment.getFileChunk();
          byte[] bytes = new byte[chunk.remaining()];
          chunk.get(bytes);
          reconstructed.write(bytes);
        }
        Assert.assertArrayEquals(
            "File " + i + " was not reconstructed correctly",
            contents[i],
            reconstructed.toByteArray());
        Assert.assertEquals(contents[i].length, reader.getTotalReadSize());
      } finally {
        reader.close();
      }
    }
  }
}
