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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileDataType;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

  /**
   * The calculated size has to be the bytes the serializer writes, for every phase and for a PIECE
   * that carries records and references.
   */
  @Test
  public void testTheCalculatedSizeIsWhatTheSerializerWrites() throws Exception {
    final Map<TTimePartitionSlot, byte[]> progressIndexes = new HashMap<>();
    progressIndexes.put(new TTimePartitionSlot(10L), new byte[] {1, 2, 3});
    progressIndexes.put(new TTimePartitionSlot(20L), new byte[] {4, 5});

    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.piece(
            new PlanNodeId("size-piece"),
            "load-size",
            "file-size.tsfile",
            3L,
            new ArrayList<>(Arrays.asList(new FixedSizeData(11), new FixedSizeData(0))));
    final LoadTsFileConsensusNode pieceWithRefs =
        LoadTsFileConsensusNode.pieceRefs(
            new PlanNodeId("size-refs"),
            "load-size",
            "file-size.tsfile",
            4L,
            Arrays.asList(
                new LoadTsFileConsensusNode.PieceRef("task/partition.tsfile", 0L, 4096L),
                new LoadTsFileConsensusNode.PieceRef("task/other.tsfile", 4096L, 512L)),
            99L,
            5L);
    final Map<TTimePartitionSlot, Long> expectedDataEnd = new HashMap<>();
    expectedDataEnd.put(new TTimePartitionSlot(10L), 4096L);
    expectedDataEnd.put(new TTimePartitionSlot(20L), 2048L);
    final LoadTsFileConsensusNode prepare =
        LoadTsFileConsensusNode.prepare(
            new PlanNodeId("size-prepare"),
            "load-size",
            7,
            12345L,
            true,
            progressIndexes,
            expectedDataEnd);
    final LoadTsFileConsensusNode commit =
        LoadTsFileConsensusNode.commit(
            new PlanNodeId("size-commit"), "load-size", true, progressIndexes, expectedDataEnd);
    final LoadTsFileConsensusNode abort =
        LoadTsFileConsensusNode.abort(
            new PlanNodeId("size-abort"), "load-size", false, expectedDataEnd);

    for (final LoadTsFileConsensusNode node :
        Arrays.asList(piece, pieceWithRefs, prepare, commit, abort)) {
      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      node.serializeBody(new DataOutputStream(body));
      Assert.assertEquals(Short.BYTES + Long.BYTES + body.size(), node.serializedSize());
    }
  }

  @Test
  public void testExpectedDataEndSerializeRoundTrip() {
    final Map<TTimePartitionSlot, Long> expectedDataEnd = new HashMap<>();
    expectedDataEnd.put(new TTimePartitionSlot(11L), 4096L);

    final LoadTsFileConsensusNode abort =
        LoadTsFileConsensusNode.abort(new PlanNodeId("n4"), "load-4", false, expectedDataEnd);
    final LoadTsFileConsensusNode node =
        (LoadTsFileConsensusNode) PlanNodeType.deserialize(abort.serializeToByteBuffer());

    Assert.assertEquals(
        Long.valueOf(4096L),
        node.getTimePartition2ExpectedDataEnd().get(new TTimePartitionSlot(11L)));
  }

  /** A record that writes a fixed number of bytes, so that a PIECE can be sized without chunks. */
  private static final class FixedSizeData implements TsFileData {

    private final int size;

    private FixedSizeData(final int size) {
      this.size = size;
    }

    @Override
    public long getDataSize() {
      return size;
    }

    @Override
    public TsFileDataType getType() {
      return TsFileDataType.CHUNK;
    }

    @Override
    public void serialize(final DataOutputStream stream) throws IOException {
      serialize(stream, true);
    }

    @Override
    public void serialize(final DataOutputStream stream, final boolean includeContent)
        throws IOException {
      stream.write(new byte[size]);
    }

    @Override
    public int serializedSize(final boolean includeContent) {
      return size;
    }
  }
}
