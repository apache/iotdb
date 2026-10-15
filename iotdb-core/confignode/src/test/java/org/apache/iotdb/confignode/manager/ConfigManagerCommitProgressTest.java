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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.subscription.meta.consumer.CommitProgressKeeper;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressResp;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigManagerCommitProgressTest {

  @Test
  public void testAbsentProgressDoesNotSetCommittedRegionProgress() {
    final TGetCommitProgressResp resp =
        ConfigManager.buildCommitProgressResponse(Collections.emptyMap(), "cg", "topic", 1);

    assertFalse(resp.isSetCommittedRegionProgress());
  }

  @Test
  public void testVersionedEmptyProgressSetsCommittedRegionProgress() throws Exception {
    final int regionId = 1;
    final String regionIdString = new DataRegionId(regionId).toString();
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateKey("cg", "topic", regionIdString, 1),
        serialize(new RegionProgress(Collections.emptyMap())));

    assertExplicitEmptyProgress(
        ConfigManager.buildCommitProgressResponse(progressMap, "cg", "topic", regionId));
  }

  @Test
  public void testLegacyEmptyProgressSetsCommittedRegionProgress() throws Exception {
    final int regionId = 2;
    final String regionIdString = new DataRegionId(regionId).toString();
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey("cg", "topic", regionIdString, 1),
        serialize(new RegionProgress(Collections.emptyMap())));

    assertExplicitEmptyProgress(
        ConfigManager.buildCommitProgressResponse(progressMap, "cg", "topic", regionId));
  }

  @Test
  public void testCommitProgressResponseCanBeSerializedByThrift() throws Exception {
    final int regionId = 2;
    final String regionIdString = new DataRegionId(regionId).toString();
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateKey("cg", "topic", regionIdString, 1),
        serialize(
            createRegionProgress(new WriterId(regionIdString, 1), new WriterProgress(100L, 1L))));
    final TGetCommitProgressResp resp =
        ConfigManager.buildCommitProgressResponse(progressMap, "cg", "topic", regionId);

    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final TTransport transport = new TIOStreamTransport(outputStream);
      resp.write(new TBinaryProtocol(transport));
      transport.flush();
      assertTrue(outputStream.size() > 0);
    }
  }

  @Test
  public void testMasqueradingLegacyEmptyProgressDoesNotSetCommittedRegionProgress()
      throws Exception {
    final int regionId = 3;
    final String regionIdString = new DataRegionId(regionId).toString();
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey(
            CommitProgressKeeper.generateRegionKey("cg", "topic", regionIdString),
            "legacyTopic",
            "legacyRegion",
            1),
        serialize(new RegionProgress(Collections.emptyMap())));

    final TGetCommitProgressResp resp =
        ConfigManager.buildCommitProgressResponse(progressMap, "cg", "topic", regionId);

    assertFalse(resp.isSetCommittedRegionProgress());
  }

  @Test
  public void testVersionedProgressMergesLegacyProgressDuringRollingUpgrade() throws Exception {
    final int regionId = 1;
    final String regionIdString = new DataRegionId(regionId).toString();
    final WriterId versionedWriter = new WriterId(regionIdString, 1);
    final WriterId legacyWriter = new WriterId(regionIdString, 2);
    final RegionProgress versionedProgress =
        createRegionProgress(versionedWriter, new WriterProgress(100L, 1L));
    final RegionProgress legacyProgress =
        createRegionProgress(legacyWriter, new WriterProgress(200L, 2L));
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey("cg", "topic", regionIdString, 1),
        serialize(legacyProgress));
    progressMap.put(
        CommitProgressKeeper.generateKey("cg", "topic", regionIdString, 2),
        serialize(versionedProgress));
    final Map<WriterId, WriterProgress> expectedWriterPositions = new LinkedHashMap<>();
    expectedWriterPositions.put(versionedWriter, new WriterProgress(100L, 1L));
    expectedWriterPositions.put(legacyWriter, new WriterProgress(200L, 2L));

    assertEquals(
        new RegionProgress(expectedWriterPositions),
        ConfigManager.mergeCommitProgress(progressMap, "cg", "topic", regionId));
  }

  @Test
  public void testLegacyProgressIsUsedWhenVersionedProgressIsAbsent() throws Exception {
    final int regionId = 2;
    final String regionIdString = new DataRegionId(regionId).toString();
    final WriterId writerId = new WriterId(regionIdString, 1);
    final RegionProgress legacyProgress =
        createRegionProgress(writerId, new WriterProgress(100L, 1L));
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey("cg", "topic", regionIdString, 1),
        serialize(legacyProgress));

    assertEquals(
        legacyProgress, ConfigManager.mergeCommitProgress(progressMap, "cg", "topic", regionId));
  }

  @Test
  public void testLegacyKeyCannotMasqueradeAsVersionedProgress() throws Exception {
    final int regionId = 3;
    final String regionIdString = new DataRegionId(regionId).toString();
    final WriterId legacyWriter = new WriterId(regionIdString, 1);
    final WriterId masqueradingWriter = new WriterId(regionIdString, 2);
    final RegionProgress legacyProgress =
        createRegionProgress(legacyWriter, new WriterProgress(100L, 1L));
    final RegionProgress masqueradingProgress =
        createRegionProgress(masqueradingWriter, new WriterProgress(200L, 2L));
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey("cg", "topic", regionIdString, 1),
        serialize(legacyProgress));
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey(
            CommitProgressKeeper.generateRegionKey("cg", "topic", regionIdString),
            "legacyTopic",
            "legacyRegion",
            2),
        serialize(masqueradingProgress));

    assertEquals(
        legacyProgress, ConfigManager.mergeCommitProgress(progressMap, "cg", "topic", regionId));
  }

  @Test
  public void testVersionedProgressSeparatesLegacyKeyCollisions() throws Exception {
    final int regionId = 4;
    final String regionIdString = new DataRegionId(regionId).toString();
    final WriterId firstWriter = new WriterId(regionIdString, 1);
    final WriterId secondWriter = new WriterId(regionIdString, 2);
    final WriterId legacyWriter = new WriterId(regionIdString, 3);
    final RegionProgress firstProgress =
        createRegionProgress(firstWriter, new WriterProgress(100L, 1L));
    final RegionProgress secondProgress =
        createRegionProgress(secondWriter, new WriterProgress(200L, 2L));
    final RegionProgress legacyProgress =
        createRegionProgress(legacyWriter, new WriterProgress(300L, 3L));
    final Map<String, ByteBuffer> progressMap = new LinkedHashMap<>();
    progressMap.put(
        CommitProgressKeeper.generateKey("a##b", "c", regionIdString, 1), serialize(firstProgress));
    progressMap.put(
        CommitProgressKeeper.generateKey("a", "b##c", regionIdString, 2),
        serialize(secondProgress));
    progressMap.put(
        CommitProgressKeeper.generateLegacyKey("a##b", "c", regionIdString, 3),
        serialize(legacyProgress));

    assertEquals(
        firstProgress, ConfigManager.mergeCommitProgress(progressMap, "a##b", "c", regionId));
    assertEquals(
        secondProgress, ConfigManager.mergeCommitProgress(progressMap, "a", "b##c", regionId));
  }

  private static RegionProgress createRegionProgress(
      final WriterId writerId, final WriterProgress writerProgress) {
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(writerId, writerProgress);
    return new RegionProgress(writerPositions);
  }

  private static void assertExplicitEmptyProgress(final TGetCommitProgressResp resp) {
    assertTrue(resp.isSetCommittedRegionProgress());
    assertTrue(
        RegionProgress.deserialize(ByteBuffer.wrap(resp.getCommittedRegionProgress()))
            .getWriterPositions()
            .isEmpty());
  }

  private static ByteBuffer serialize(final RegionProgress regionProgress) throws Exception {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos)) {
      regionProgress.serialize(dos);
      dos.flush();
      return ByteBuffer.wrap(baos.toByteArray()).asReadOnlyBuffer();
    }
  }
}
