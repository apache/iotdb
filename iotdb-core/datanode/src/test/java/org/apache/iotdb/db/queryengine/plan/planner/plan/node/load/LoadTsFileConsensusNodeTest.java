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

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class LoadTsFileConsensusNodeTest {

  @Test
  public void testBeginSerializeRoundTrip() {
    final LoadTsFileConsensusNode begin =
        LoadTsFileConsensusNode.begin(new PlanNodeId("n1"), "load-1", "file-1", true, "db", 3);
    final ByteBuffer buffer = begin.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.BEGIN, node.getOp());
    Assert.assertEquals("load-1", node.getLoadId());
    Assert.assertEquals("file-1", node.getTsFileId());
    Assert.assertTrue(node.isTableModel());
    Assert.assertEquals("db", node.getDatabase());
    Assert.assertEquals(3, node.getExpectedPieceCount());
  }

  @Test
  public void testPieceSerializeRoundTrip() {
    final LoadTsFileConsensusNode commit =
        LoadTsFileConsensusNode.commit(
            new PlanNodeId("n2"), "load-2", "file-2", true, false, Collections.emptyMap());
    final ByteBuffer buffer = commit.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.COMMIT, node.getOp());
    Assert.assertTrue(node.isGeneratedByPipe());
  }

  @Test
  public void testPiecePreservesCallerChecksum() {
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.piece(
            new PlanNodeId("checksum"),
            "load",
            "file",
            0L,
            0L,
            Collections.emptyList(),
            987654321L);

    Assert.assertEquals(987654321L, piece.getChecksum());
  }

  @Test
  public void testPieceRefSerializeRoundTrip() {
    final LoadTsFileConsensusNode.PieceRef ref =
        new LoadTsFileConsensusNode.PieceRef(
            "task-1/partition.tsfile", 4096L, 3L, new byte[] {1, 2, 3});
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.pieceRefs(
            new PlanNodeId("n3"), "load-3", "file-3", 0L, Collections.singletonList(ref), 0L, 3L);
    final ByteBuffer buffer = piece.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.PIECE, node.getOp());
    Assert.assertEquals(1, node.getPieceRefs().size());
    final LoadTsFileConsensusNode.PieceRef actual = node.getPieceRefs().get(0);
    Assert.assertEquals("task-1/partition.tsfile", actual.getRelativePath());
    Assert.assertEquals(4096L, actual.getOffset());
    Assert.assertEquals(3L, actual.getSize());
    Assert.assertTrue(Arrays.equals(new byte[] {1, 2, 3}, actual.getContent()));
  }

  @Test
  public void testPieceMarkerSerializeRoundTrip() {
    final LoadTsFileConsensusNode marker =
        LoadTsFileConsensusNode.pieceMarker(
            new PlanNodeId("n4"), "load-4", "file-4", 7L, 123456L, 1024L);
    final ByteBuffer buffer = marker.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.PIECE, node.getOp());
    Assert.assertEquals("load-4", node.getLoadId());
    Assert.assertEquals("file-4", node.getTsFileId());
    Assert.assertEquals(7L, node.getPieceIndex());
    Assert.assertEquals(123456L, node.getChecksum());
    Assert.assertEquals(1024L, node.getDataSize());
    Assert.assertFalse(node.hasChunkData());
    Assert.assertTrue(node.getPieceRefs().isEmpty());
  }

  @Test
  public void testPullSerializeRoundTrip() {
    final LoadTsFileConsensusNode pull =
        LoadTsFileConsensusNode.pull(
            new PlanNodeId("n5"), "load-5", "file-5", 3L, 999L, "192.168.1.10:6667");
    final ByteBuffer buffer = pull.serializeToByteBuffer();
    final PlanNode deserialized = PlanNodeType.deserialize(buffer);
    Assert.assertTrue(deserialized instanceof LoadTsFileConsensusNode);
    final LoadTsFileConsensusNode node = (LoadTsFileConsensusNode) deserialized;
    Assert.assertEquals(LoadTsFileConsensusOp.PULL, node.getOp());
    Assert.assertEquals(3L, node.getPieceIndex());
    Assert.assertEquals(999L, node.getChecksum());
    Assert.assertEquals("192.168.1.10:6667", node.getPullSourceEndPoint());
  }
}
