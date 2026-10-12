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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

public class LoadTsFileConsensusNodeTest {

  @Test
  public void testPrepareSerializeRoundTrip() {
    final LoadTsFileConsensusNode prepare =
        LoadTsFileConsensusNode.prepare(
            new PlanNodeId("n1"), "load-1", 4, 4096L, true, Collections.emptyMap());
    final ByteBuffer buffer = prepare.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.PREPARE, node.getOp());
    Assert.assertEquals("load-1", node.getLoadId());
    Assert.assertEquals(4, node.getPieceCount());
    Assert.assertEquals(4096L, node.getTotalBytes());
    Assert.assertTrue(node.isGeneratedByPipe());
  }

  @Test
  public void testCommitSerializeRoundTrip() {
    final LoadTsFileConsensusNode commit =
        LoadTsFileConsensusNode.commit(
            new PlanNodeId("n2"), "load-2", true, Collections.emptyMap());
    final ByteBuffer buffer = commit.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.COMMIT, node.getOp());
    Assert.assertEquals("load-2", node.getLoadId());
    Assert.assertTrue(node.isGeneratedByPipe());
  }

  @Test
  public void testAbortSerializeRoundTrip() {
    final LoadTsFileConsensusNode abort =
        LoadTsFileConsensusNode.abort(new PlanNodeId("n3"), "load-3", true);
    final ByteBuffer buffer = abort.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.ABORT, node.getOp());
    Assert.assertEquals("load-3", node.getLoadId());
    Assert.assertTrue(node.isGeneratedByPipe());
  }

  @Test
  public void testPieceRefSerializeRoundTrip() {
    final LoadTsFileConsensusNode.PieceRef ref =
        new LoadTsFileConsensusNode.PieceRef("task-1/partition.tsfile", 4096L, 3L);
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.pieceRefs(
            new PlanNodeId("n4"), "load-4", "file-4", 0L, Collections.singletonList(ref), 0L, 3L);
    final ByteBuffer buffer = piece.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.PIECE, node.getOp());
    Assert.assertEquals("file-4", node.getTsFileId());
    Assert.assertEquals(1, node.getPieceRefs().size());
    final LoadTsFileConsensusNode.PieceRef actual = node.getPieceRefs().get(0);
    Assert.assertEquals("task-1/partition.tsfile", actual.getRelativePath());
    Assert.assertEquals(4096L, actual.getOffset());
    Assert.assertEquals(3L, actual.getSize());
  }

  @Test
  public void testPieceMarkerSerializeRoundTrip() {
    final LoadTsFileConsensusNode marker =
        LoadTsFileConsensusNode.pieceMarker(
            new PlanNodeId("n5"), "load-5", "file-5", 7L, 123456L, 1024L);
    final ByteBuffer buffer = marker.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.PIECE, node.getOp());
    Assert.assertEquals("load-5", node.getLoadId());
    Assert.assertEquals("file-5", node.getTsFileId());
    Assert.assertEquals(7L, node.getPieceIndex());
    Assert.assertEquals(123456L, node.getChecksum());
    Assert.assertEquals(1024L, node.getDataSize());
    Assert.assertFalse(node.hasChunkData());
    Assert.assertTrue(node.getPieceRefs().isEmpty());
  }

  /** A COMMIT carries none of the PIECE fields, such as the piece index or the checksum. */
  @Test
  public void testACommandOnlyCarriesTheFieldsOfItsPhase() {
    final LoadTsFileConsensusNode.PieceRef ref =
        new LoadTsFileConsensusNode.PieceRef("task-1/partition.tsfile", 0L, 1L);
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.pieceRefs(
            new PlanNodeId("n6"), "load-6", "file-6", 0L, Collections.singletonList(ref), 7L, 1L);
    final LoadTsFileConsensusNode commit =
        LoadTsFileConsensusNode.commit(
            new PlanNodeId("n7"), "load-6", false, Collections.emptyMap());

    Assert.assertTrue(commit.serializedSize() < piece.serializedSize());
    Assert.assertEquals(-1L, commit.getPieceIndex());
    Assert.assertNull(commit.getTsFileId());
    Assert.assertEquals(0L, commit.getChecksum());
    Assert.assertEquals(-1, commit.getPieceCount());
    Assert.assertTrue(commit.getPieceRefs().isEmpty());
    Assert.assertTrue(commit.getTsFileDataList().isEmpty());
  }

  /**
   * The buffer the dispatcher hands to a replica is read back as a consensus entry ({@code
   * WALEntry.deserializeForConsensus}), so it has to be parseable as one and to carry the search
   * index of the piece.
   */
  @Test
  public void testForwardedNodeIsReadableAsAConsensusEntry() {
    final LoadTsFileConsensusNode prepare =
        LoadTsFileConsensusNode.prepare(
            new PlanNodeId("n8"), "load-8", 1, 8L, false, Collections.emptyMap());
    prepare.setSearchIndex(7L);

    final PlanNode parsed = WALEntry.deserializeForConsensus(prepare.serialize());

    Assert.assertTrue(parsed instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode actual = (LoadTsFileConsensusNode) parsed;
    Assert.assertEquals(LoadTsFileConsensusOp.PREPARE, actual.getOp());
    Assert.assertEquals("load-8", actual.getLoadId());
    Assert.assertEquals(1, actual.getPieceCount());
    Assert.assertEquals(8L, actual.getTotalBytes());
    Assert.assertEquals(7L, actual.getSearchIndex());
    Assert.assertEquals(new PlanNodeId("n8"), actual.getPlanNodeId());
  }
}
