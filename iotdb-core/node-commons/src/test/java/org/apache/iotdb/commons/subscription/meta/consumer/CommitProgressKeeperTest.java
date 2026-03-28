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

package org.apache.iotdb.commons.subscription.meta.consumer;

import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommitProgressKeeperTest {

  @Test
  public void testUpdateAndReplaceAllUseDefensiveCopies() throws Exception {
    final CommitProgressKeeper keeper = new CommitProgressKeeper();
    final String key = CommitProgressKeeper.generateKey("cg", "topic", "1_1", 3);
    final RegionProgress regionProgress = createRegionProgress("1_1", 7, 2L, 100L, 10L);

    final ByteBuffer source = serialize(regionProgress);
    keeper.updateRegionProgress(key, source);
    source.position(source.limit());

    final ByteBuffer firstRead = keeper.getRegionProgress(key);
    assertTrue(firstRead.isReadOnly());
    firstRead.get();
    assertEquals(regionProgress, RegionProgress.deserialize(keeper.getRegionProgress(key)));

    final Map<String, ByteBuffer> replacement = new LinkedHashMap<>();
    final RegionProgress replacementProgress = createRegionProgress("1_1", 8, 3L, 120L, 12L);
    final ByteBuffer replacementBuffer = serialize(replacementProgress);
    replacement.put(key, replacementBuffer);

    keeper.replaceAll(replacement);
    replacementBuffer.position(replacementBuffer.limit());

    assertEquals(replacementProgress, RegionProgress.deserialize(keeper.getRegionProgress(key)));
  }

  @Test
  public void testSnapshotRoundTripPreservesRegionProgress() throws Exception {
    final CommitProgressKeeper keeper = new CommitProgressKeeper();
    final String firstKey = CommitProgressKeeper.generateKey("cg", "topicA", "1_1", 3);
    final String secondKey = CommitProgressKeeper.generateKey("cg", "topicB", "1_2", 5);
    final RegionProgress firstProgress =
        createRegionProgress(
            "1_1",
            new WriterId("1_1", 7, 2L),
            new WriterProgress(100L, 10L),
            new WriterId("1_1", 8, 2L),
            new WriterProgress(110L, 11L));
    final RegionProgress secondProgress = createRegionProgress("1_2", 9, 4L, 200L, 20L);

    keeper.updateRegionProgress(firstKey, serialize(firstProgress));
    keeper.updateRegionProgress(secondKey, serialize(secondProgress));

    final Path snapshot = Files.createTempFile("commit-progress-keeper", ".snapshot");
    try {
      try (FileOutputStream fos = new FileOutputStream(snapshot.toFile())) {
        keeper.processTakeSnapshot(fos);
      }

      final CommitProgressKeeper restored = new CommitProgressKeeper();
      try (FileInputStream fis = new FileInputStream(snapshot.toFile())) {
        restored.processLoadSnapshot(fis);
      }

      assertEquals(firstProgress, RegionProgress.deserialize(restored.getRegionProgress(firstKey)));
      assertEquals(
          secondProgress, RegionProgress.deserialize(restored.getRegionProgress(secondKey)));
      assertEquals(2, restored.getAllRegionProgress().size());
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  private static RegionProgress createRegionProgress(
      final String regionId,
      final int nodeId,
      final long writerEpoch,
      final long physicalTime,
      final long localSeq) {
    return createRegionProgress(
        regionId,
        new WriterId(regionId, nodeId, writerEpoch),
        new WriterProgress(physicalTime, localSeq));
  }

  private static RegionProgress createRegionProgress(
      final String regionId,
      final WriterId firstWriterId,
      final WriterProgress firstWriterProgress) {
    return createRegionProgress(regionId, firstWriterId, firstWriterProgress, null, null);
  }

  private static RegionProgress createRegionProgress(
      final String regionId,
      final WriterId firstWriterId,
      final WriterProgress firstWriterProgress,
      final WriterId secondWriterId,
      final WriterProgress secondWriterProgress) {
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(firstWriterId, firstWriterProgress);
    if (secondWriterId != null && secondWriterProgress != null) {
      writerPositions.put(secondWriterId, secondWriterProgress);
    }
    return new RegionProgress(writerPositions);
  }

  private static ByteBuffer serialize(final RegionProgress regionProgress) throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      regionProgress.serialize(dos);
      dos.flush();
      return ByteBuffer.wrap(baos.toByteArray()).asReadOnlyBuffer();
    }
  }
}
