/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

public class LoadTsFilePieceNodeAssemblerTest {

  @Test
  public void testAssembleSlices() {
    final LoadTsFilePieceNodeAssembler assembler = new LoadTsFilePieceNodeAssembler(3, 7);

    final LoadTsFilePieceNodeAssembler.Result first =
        assembler.append(ByteBuffer.wrap(new byte[] {0, 1, 2}), 0, 3, 7);
    Assert.assertTrue(first.isValid());
    Assert.assertFalse(first.isComplete());

    final ByteBuffer secondBody = ByteBuffer.wrap(new byte[] {9, 3, 4, 9});
    secondBody.position(1);
    secondBody.limit(3);
    final LoadTsFilePieceNodeAssembler.Result second = assembler.append(secondBody, 1, 3, 7);
    Assert.assertTrue(second.isValid());
    Assert.assertFalse(second.isComplete());

    final LoadTsFilePieceNodeAssembler.Result last =
        assembler.append(ByteBuffer.wrap(new byte[] {5, 6}), 2, 3, 7);
    Assert.assertTrue(last.isValid());
    Assert.assertTrue(last.isComplete());

    final byte[] assembled = new byte[last.getBody().remaining()];
    last.getBody().get(assembled);
    Assert.assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6}, assembled);
  }

  @Test
  public void testRejectOutOfOrderSlice() {
    final LoadTsFilePieceNodeAssembler.Result result =
        new LoadTsFilePieceNodeAssembler(2, 2).append(ByteBuffer.wrap(new byte[] {1}), 1, 2, 2);

    Assert.assertFalse(result.isValid());
    Assert.assertFalse(result.isComplete());
  }

  @Test
  public void testRejectMismatchedOriginBodySize() {
    final LoadTsFilePieceNodeAssembler assembler = new LoadTsFilePieceNodeAssembler(2, 2);
    Assert.assertTrue(assembler.append(ByteBuffer.wrap(new byte[] {0}), 0, 2, 2).isValid());

    final LoadTsFilePieceNodeAssembler.Result result =
        assembler.append(ByteBuffer.wrap(new byte[] {1}), 1, 2, 3);
    Assert.assertFalse(result.isValid());
    Assert.assertFalse(result.isComplete());
  }

  @Test
  public void testAssembledBodyCanDeserializeLoadTsFilePieceNode() {
    final LoadTsFilePieceNode pieceNode =
        new LoadTsFilePieceNode(new PlanNodeId("piece"), new File("test.tsfile"));
    final ByteBuffer body = pieceNode.serializeToByteBuffer();
    final int firstSliceSize = body.remaining() / 2;
    final LoadTsFilePieceNodeAssembler assembler =
        new LoadTsFilePieceNodeAssembler(2, body.remaining());

    final ByteBuffer firstSlice = body.duplicate();
    firstSlice.limit(firstSlice.position() + firstSliceSize);
    Assert.assertFalse(assembler.append(firstSlice.slice(), 0, 2, body.remaining()).isComplete());

    final ByteBuffer lastSlice = body.duplicate();
    lastSlice.position(lastSlice.position() + firstSliceSize);
    final LoadTsFilePieceNodeAssembler.Result result =
        assembler.append(lastSlice.slice(), 1, 2, body.remaining());

    Assert.assertTrue(result.isValid());
    Assert.assertTrue(result.isComplete());
    Assert.assertEquals(pieceNode, PlanNodeType.deserialize(result.getBody()));
  }
}
