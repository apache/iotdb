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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
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
    Assert.assertEquals(
        String.format(
            StorageEngineMessages
                .MESSAGE_UNEXPECTED_LOAD_TSFILE_SLICE_INDEX_ARG_EXPECTED_ARG_SLICECOUNT_ARG_76260F62,
            1,
            0,
            2),
        result.getErrorMessage());
  }

  @Test
  public void testRejectMismatchedOriginBodySize() {
    final LoadTsFilePieceNodeAssembler assembler = new LoadTsFilePieceNodeAssembler(2, 2);
    Assert.assertTrue(assembler.append(ByteBuffer.wrap(new byte[] {0}), 0, 2, 2).isValid());

    final LoadTsFilePieceNodeAssembler.Result result =
        assembler.append(ByteBuffer.wrap(new byte[] {1}), 1, 2, 3);
    Assert.assertFalse(result.isValid());
    Assert.assertFalse(result.isComplete());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages
                .MESSAGE_LOAD_TSFILE_SLICE_METADATA_CHANGED_SLICECOUNT_ARG_EXPECTED_ARG_ORIGINBODYSIZE_ARG_EXPECTED_ARG_B16B3122,
            2,
            2,
            3,
            2),
        result.getErrorMessage());
  }

  @Test
  public void testAssembleDirectAndReadOnlySlicesPreservesInputPositions() {
    final byte[] expected = new byte[20000];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) i;
    }
    for (boolean direct : new boolean[] {false, true}) {
      final LoadTsFilePieceNodeAssembler assembler =
          new LoadTsFilePieceNodeAssembler(2, expected.length);
      final ByteBuffer backing =
          direct ? ByteBuffer.allocateDirect(17002) : ByteBuffer.allocate(17002);
      backing.position(1);
      backing.put(expected, 0, 17000);
      backing.flip();
      backing.position(1);
      final ByteBuffer slice = backing.asReadOnlyBuffer();
      Assert.assertFalse(slice.hasArray());
      Assert.assertTrue(assembler.append(slice, 0, 2, expected.length).isValid());
      Assert.assertEquals(1, slice.position());
      Assert.assertEquals(17001, slice.limit());
      final LoadTsFilePieceNodeAssembler.Result result =
          assembler.append(ByteBuffer.wrap(expected, 17000, 3000), 1, 2, expected.length);
      Assert.assertTrue(result.isComplete());
      Assert.assertTrue(result.getBody().isReadOnly());
      final byte[] actual = new byte[result.getBody().remaining()];
      result.getBody().get(actual);
      Assert.assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testRejectInvalidBodyAndMetadataWithReason() {
    for (ByteBuffer body : new ByteBuffer[] {null, ByteBuffer.allocate(0)}) {
      final LoadTsFilePieceNodeAssembler.Result result =
          new LoadTsFilePieceNodeAssembler(2, 2).append(body, 0, 2, 2);
      Assert.assertFalse(result.isValid());
      Assert.assertEquals(
          StorageEngineMessages.MESSAGE_LOAD_TSFILE_SLICE_BODY_IS_NULL_OR_EMPTY_2A65366C,
          result.getErrorMessage());
    }
    for (int[] metadata : new int[][] {{1, 2}, {0, 2}, {2, 0}, {2, -1}}) {
      final LoadTsFilePieceNodeAssembler.Result result =
          new LoadTsFilePieceNodeAssembler(metadata[0], metadata[1])
              .append(ByteBuffer.wrap(new byte[] {1}), 0, metadata[0], metadata[1]);
      Assert.assertFalse(result.isValid());
      Assert.assertEquals(
          String.format(
              StorageEngineMessages
                  .MESSAGE_INVALID_LOAD_TSFILE_SLICE_METADATA_SLICECOUNT_ARG_ORIGINBODYSIZE_ARG_379BF1B8,
              metadata[0],
              metadata[1]),
          result.getErrorMessage());
    }
  }

  @Test
  public void testRejectBodySizeViolationsWithReason() {
    final LoadTsFilePieceNodeAssembler.Result overflow =
        new LoadTsFilePieceNodeAssembler(2, 2).append(ByteBuffer.allocate(3), 0, 2, 2);
    Assert.assertFalse(overflow.isValid());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages
                .MESSAGE_LOAD_TSFILE_SLICE_EXCEEDS_ORIGINBODYSIZE_ASSEMBLEDSIZE_ARG_SLICESIZE_ARG_ORIGINBODYSIZE_ARG_0198E0D0,
            0,
            3,
            2),
        overflow.getErrorMessage());

    final LoadTsFilePieceNodeAssembler.Result earlyCompletion =
        new LoadTsFilePieceNodeAssembler(2, 2).append(ByteBuffer.allocate(2), 0, 2, 2);
    Assert.assertFalse(earlyCompletion.isValid());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages
                .MESSAGE_LOAD_TSFILE_BODY_COMPLETED_BEFORE_THE_LAST_SLICE_RECEIVED_ARG_SLICECOUNT_ARG_ORIGINBODYSIZE_ARG_0425EA2C,
            1,
            2,
            2),
        earlyCompletion.getErrorMessage());

    final LoadTsFilePieceNodeAssembler assembler = new LoadTsFilePieceNodeAssembler(2, 3);
    Assert.assertTrue(assembler.append(ByteBuffer.allocate(1), 0, 2, 3).isValid());
    final LoadTsFilePieceNodeAssembler.Result incomplete =
        assembler.append(ByteBuffer.allocate(1), 1, 2, 3);
    Assert.assertFalse(incomplete.isValid());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages
                .MESSAGE_LOAD_TSFILE_BODY_SIZE_MISMATCH_ASSEMBLEDSIZE_ARG_ORIGINBODYSIZE_ARG_5733FC4B,
            2,
            3),
        incomplete.getErrorMessage());
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
