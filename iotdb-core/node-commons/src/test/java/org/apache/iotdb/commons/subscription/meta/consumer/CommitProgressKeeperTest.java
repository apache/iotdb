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
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CommitProgressKeeperTest {

  @Test
  public void testStateAccessUsesOneInstanceMonitor() throws Exception {
    assertSynchronized("removeTopicProgress", String.class, String.class);
    assertSynchronized("updateRegionProgress", String.class, ByteBuffer.class);
    assertSynchronized("getRegionProgress", String.class);
    assertSynchronized("getAllRegionProgress");
    assertSynchronized("replaceAll", Map.class);
    assertSynchronized("isEmpty");
    assertSynchronized("processTakeSnapshot", FileOutputStream.class);
    assertSynchronized("processLoadSnapshot", FileInputStream.class);
    assertSynchronized("serializeToStream", DataOutputStream.class);
  }

  @Test
  public void testVersionedKeysAvoidLegacySeparatorCollision() {
    final String firstKey = CommitProgressKeeper.generateKey("a##b", "c", "1_1", 3);
    final String secondKey = CommitProgressKeeper.generateKey("a", "b##c", "1_1", 3);

    assertNotEquals(firstKey, secondKey);
    assertEquals(
        CommitProgressKeeper.generateLegacyKey("a##b", "c", "1_1", 3),
        CommitProgressKeeper.generateLegacyKey("a", "b##c", "1_1", 3));
    assertTrue(
        firstKey.startsWith(CommitProgressKeeper.generateRegionKeyPrefix("a##b", "c", "1_1")));
  }

  @Test
  public void testValidatesProgressKeyGrammarAndLegacyEligibility() {
    final String prefix = CommitProgressKeeper.generateRegionKeyPrefix("cg", "topic", "1_1");

    assertTrue(
        CommitProgressKeeper.isValidDataNodeProgressKey(
            CommitProgressKeeper.generateKey("cg", "topic", "1_1", 3), prefix));
    assertFalse(CommitProgressKeeper.isValidDataNodeProgressKey(prefix + "3##4", prefix));
    assertFalse(CommitProgressKeeper.isValidDataNodeProgressKey(prefix + "03", prefix));
    assertTrue(CommitProgressKeeper.isLegacyKeyUnambiguous("cg", "topic", "1_1"));
    assertFalse(CommitProgressKeeper.isLegacyKeyUnambiguous("cg##suffix", "topic", "1_1"));
  }

  @Test
  public void testRemoveTopicProgressRemovesVersionedAndLegacyKeys() {
    final CommitProgressKeeper keeper = new CommitProgressKeeper();
    final ByteBuffer progress = ByteBuffer.wrap(new byte[] {1});
    final String versionedKey = CommitProgressKeeper.generateKey("cg", "topic", "1_1", 1);
    final String legacyKey = CommitProgressKeeper.generateLegacyKey("cg", "topic", "1_2", 2);
    final String delimiterKey = CommitProgressKeeper.generateKey("a##b", "c", "1_3", 3);
    final String ambiguousLegacyKey = CommitProgressKeeper.generateLegacyKey("a##b", "c", "1_3", 3);
    final String otherTopicKey = CommitProgressKeeper.generateKey("cg", "other", "1_1", 1);
    final String masqueradingLegacyKey =
        CommitProgressKeeper.generateLegacyKey(
            CommitProgressKeeper.generateRegionKey("cg", "topic", ""), "legacyTopic", "1_4", 4);
    keeper.updateRegionProgress(versionedKey, progress);
    keeper.updateRegionProgress(legacyKey, progress);
    keeper.updateRegionProgress(delimiterKey, progress);
    keeper.updateRegionProgress(ambiguousLegacyKey, progress);
    keeper.updateRegionProgress(otherTopicKey, progress);
    keeper.updateRegionProgress(masqueradingLegacyKey, progress);

    keeper.removeTopicProgress("cg", "topic");
    keeper.removeTopicProgress("a##b", "c");

    assertNull(keeper.getRegionProgress(versionedKey));
    assertNull(keeper.getRegionProgress(legacyKey));
    assertNull(keeper.getRegionProgress(delimiterKey));
    assertNotNull(keeper.getRegionProgress(ambiguousLegacyKey));
    assertNotNull(keeper.getRegionProgress(otherTopicKey));
    assertNotNull(keeper.getRegionProgress(masqueradingLegacyKey));
  }

  @Test
  public void testUpdateAndReplaceAllUseDefensiveCopies() throws Exception {
    final CommitProgressKeeper keeper = new CommitProgressKeeper();
    final String key = CommitProgressKeeper.generateKey("cg", "topic", "1_1", 3);
    final RegionProgress regionProgress = createRegionProgress("1_1", 7, 100L, 10L);

    final ByteBuffer source = serialize(regionProgress);
    keeper.updateRegionProgress(key, source);
    source.position(source.limit());

    final ByteBuffer firstRead = keeper.getRegionProgress(key);
    assertTrue(firstRead.isReadOnly());
    firstRead.get();
    assertEquals(regionProgress, RegionProgress.deserialize(keeper.getRegionProgress(key)));

    final Map<String, ByteBuffer> replacement = new LinkedHashMap<>();
    final RegionProgress replacementProgress = createRegionProgress("1_1", 8, 120L, 12L);
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
            new WriterId("1_1", 7),
            new WriterProgress(100L, 10L),
            new WriterId("1_1", 8),
            new WriterProgress(110L, 11L));
    final RegionProgress secondProgress = createRegionProgress("1_2", 9, 200L, 20L);

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

  @Test
  public void testSnapshotLoadHandlesShortReads() throws Exception {
    final CommitProgressKeeper keeper = new CommitProgressKeeper();
    final String key = CommitProgressKeeper.generateKey("cg", "topic", "1_1", 3);
    final RegionProgress regionProgress = createRegionProgress("1_1", 7, 100L, 10L);
    keeper.updateRegionProgress(key, serialize(regionProgress));

    final Path snapshot = Files.createTempFile("commit-progress-keeper-short-read", ".snapshot");
    try {
      try (FileOutputStream fos = new FileOutputStream(snapshot.toFile())) {
        keeper.processTakeSnapshot(fos);
      }

      final CommitProgressKeeper restored = new CommitProgressKeeper();
      try (FileInputStream fis = new OneByteAtATimeFileInputStream(snapshot)) {
        restored.processLoadSnapshot(fis);
      }

      assertEquals(regionProgress, RegionProgress.deserialize(restored.getRegionProgress(key)));
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  @Test
  public void testSnapshotLoadRejectsTruncatedSizeHeader() throws Exception {
    final Path snapshot =
        Files.createTempFile("commit-progress-keeper-truncated-size", ".snapshot");
    try {
      Files.write(snapshot, new byte[0]);
      try (FileInputStream fis = new FileInputStream(snapshot.toFile())) {
        new CommitProgressKeeper().processLoadSnapshot(fis);
      }

      Files.write(snapshot, new byte[] {0, 0, 0});
      try (FileInputStream fis = new FileInputStream(snapshot.toFile())) {
        try {
          new CommitProgressKeeper().processLoadSnapshot(fis);
          org.junit.Assert.fail("Expected IOException for a truncated size header");
        } catch (final IOException expected) {
          // expected
        }
      }
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  @Test
  public void testSnapshotLoadRejectsNegativeLengths() throws Exception {
    final Path snapshot = Files.createTempFile("commit-progress-keeper-negative", ".snapshot");
    try {
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(snapshot.toFile()))) {
        dos.writeInt(-1);
      }
      assertSnapshotLoadFails(snapshot);

      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(snapshot.toFile()))) {
        dos.writeInt(1);
        dos.writeInt(-1);
      }
      assertSnapshotLoadFails(snapshot);

      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(snapshot.toFile()))) {
        dos.writeInt(1);
        dos.writeInt(1);
        dos.writeByte('k');
        dos.writeInt(-1);
      }
      assertSnapshotLoadFails(snapshot);
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  @Test
  public void testSnapshotLoadRejectsLengthsExceedingRemainingBytes() throws Exception {
    final Path snapshot = Files.createTempFile("commit-progress-keeper-oversized", ".snapshot");
    try {
      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(snapshot.toFile()))) {
        dos.writeInt(Integer.MAX_VALUE);
      }
      assertSnapshotLoadFails(snapshot);

      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(snapshot.toFile()))) {
        dos.writeInt(1);
        dos.writeInt(Integer.MAX_VALUE);
      }
      assertSnapshotLoadFails(snapshot);

      try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(snapshot.toFile()))) {
        dos.writeInt(1);
        dos.writeInt(0);
        dos.writeInt(Integer.MAX_VALUE);
      }
      assertSnapshotLoadFails(snapshot);
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  @Test
  public void testRegionProgressMapDeserializationRejectsNegativeLengths() {
    assertDeserializeRegionProgressFails(ByteBuffer.allocate(Integer.BYTES).putInt(-1).flip());
    assertDeserializeRegionProgressFails(
        ByteBuffer.allocate(2 * Integer.BYTES).putInt(1).putInt(-1).flip());
    assertDeserializeRegionProgressFails(
        ByteBuffer.allocate(3 * Integer.BYTES + 1)
            .putInt(1)
            .putInt(1)
            .put((byte) 'k')
            .putInt(-1)
            .flip());
  }

  @Test
  public void testRegionProgressMapDeserializationRejectsLengthsExceedingRemainingBytes() {
    assertDeserializeRegionProgressFails(
        ByteBuffer.allocate(Integer.BYTES).putInt(Integer.MAX_VALUE).flip());
    assertDeserializeRegionProgressFails(
        ByteBuffer.allocate(2 * Integer.BYTES).putInt(1).putInt(Integer.MAX_VALUE).flip());
    assertDeserializeRegionProgressFails(
        ByteBuffer.allocate(3 * Integer.BYTES)
            .putInt(1)
            .putInt(0)
            .putInt(Integer.MAX_VALUE)
            .flip());
  }

  @Test
  public void testRegionProgressMapSerializationRoundTrip() throws Exception {
    final String firstKey = CommitProgressKeeper.generateKey("cg", "topicA", "1_1", 3);
    final String secondKey = CommitProgressKeeper.generateKey("cg", "topicB", "1_2", 5);
    final RegionProgress firstProgress = createRegionProgress("1_1", 7, 100L, 10L);
    final RegionProgress secondProgress = createRegionProgress("1_2", 8, 200L, 20L);
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(firstKey, serialize(firstProgress));
    progressMap.put(secondKey, serialize(secondProgress));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      CommitProgressKeeper.serializeRegionProgressMapToStream(progressMap, dos);
    }

    final Map<String, ByteBuffer> restored =
        CommitProgressKeeper.deserializeRegionProgressFromBuffer(
            ByteBuffer.wrap(baos.toByteArray()));
    assertEquals(firstProgress, RegionProgress.deserialize(restored.get(firstKey)));
    assertEquals(secondProgress, RegionProgress.deserialize(restored.get(secondKey)));
  }

  private static void assertSnapshotLoadFails(final Path snapshot) throws Exception {
    try (FileInputStream fis = new FileInputStream(snapshot.toFile())) {
      try {
        new CommitProgressKeeper().processLoadSnapshot(fis);
        org.junit.Assert.fail("Expected IOException for invalid snapshot lengths");
      } catch (final IOException expected) {
        // expected
      }
    }
  }

  private static void assertDeserializeRegionProgressFails(final ByteBuffer buffer) {
    try {
      CommitProgressKeeper.deserializeRegionProgressFromBuffer(buffer);
      org.junit.Assert.fail("Expected IllegalArgumentException for invalid buffer lengths");
    } catch (final IllegalArgumentException expected) {
      // expected
    }
  }

  private static RegionProgress createRegionProgress(
      final String regionId, final int nodeId, final long physicalTime, final long localSeq) {
    return createRegionProgress(
        regionId, new WriterId(regionId, nodeId), new WriterProgress(physicalTime, localSeq));
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

  private static class OneByteAtATimeFileInputStream extends FileInputStream {

    private OneByteAtATimeFileInputStream(final Path path) throws IOException {
      super(path.toFile());
    }

    @Override
    public int read(final byte[] bytes) throws IOException {
      return read(bytes, 0, bytes.length);
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException {
      return super.read(bytes, offset, Math.min(length, 1));
    }
  }

  private static void assertSynchronized(final String methodName, final Class<?>... parameterTypes)
      throws Exception {
    final Method method = CommitProgressKeeper.class.getMethod(methodName, parameterTypes);
    assertTrue(methodName, Modifier.isSynchronized(method.getModifiers()));
  }
}
