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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferSliceReqBuilder;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlockCategory;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class PipeAsyncThriftRequestMemoryBlockTest {

  @Test
  public void testReservationCoversThriftBufferGrowth() throws Exception {
    final TProtocolFactory[] protocolFactories = {
      new TBinaryProtocol.Factory(), new TCompactProtocol.Factory()
    };
    for (final TProtocolFactory protocolFactory : protocolFactories) {
      for (final int bodySize : new int[] {0, 83, 84, 85, 86, 211, 212, 213, 214, 4096, 1 << 20}) {
        final TPipeTransferReq request = new TPipeTransferReq();
        request.version = 1;
        request.type = 1;
        request.body = ByteBuffer.wrap(new byte[bodySize]);

        final byte[][] binaryWriteArray = new byte[1][];
        final TMemoryBuffer memoryBuffer =
            new TMemoryBuffer(128) {
              @Override
              public void write(final byte[] buffer, final int offset, final int length) {
                if (length == bodySize) {
                  binaryWriteArray[0] = buffer;
                }
                super.write(buffer, offset, length);
              }
            };
        final TProtocol protocol = protocolFactory.getProtocol(memoryBuffer);
        final IClientRPCService.pipeTransfer_args args = new IClientRPCService.pipeTransfer_args();
        args.setReq(request);
        protocol.writeMessageBegin(new TMessage("pipeTransfer", TMessageType.CALL, 0));
        args.write(protocol);
        protocol.writeMessageEnd();

        if (protocolFactory instanceof TCompactProtocol.Factory && bodySize > 0) {
          Assert.assertNotSame(request.body.array(), binaryWriteArray[0]);
          Assert.assertEquals(bodySize, binaryWriteArray[0].length);
        }
        Assert.assertTrue(
            PipeAsyncThriftRequestMemoryBlock.calculateMemoryUsage(bodySize, bodySize)
                >= 2L * bodySize + memoryBuffer.getArray().length + Integer.BYTES);
      }
    }
  }

  @Test
  public void testRequestMemoryBlockIsReleased() {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long usedMemoryBefore = memoryManager.getUsedMemorySizeInBytes();
    final PipeMemoryBlock memoryBlock =
        PipeAsyncThriftRequestMemoryBlock.allocateForSerializedBody(1024, "test");

    Assert.assertEquals(PipeMemoryBlockCategory.SINK, memoryBlock.getCategory());
    Assert.assertEquals(
        PipeAsyncThriftRequestMemoryBlock.calculateMemoryUsage(1024, 1024),
        memoryBlock.getMemoryUsageInBytes());
    memoryBlock.close();

    Assert.assertEquals(usedMemoryBefore, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testNonBlockingSerializedBodyReservationIsReleased() {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long usedMemoryBefore = memoryManager.getUsedMemorySizeInBytes();
    final PipeMemoryBlock memoryBlock =
        PipeAsyncThriftRequestMemoryBlock.tryAllocateForSerializedBody(1024, "try-allocate-test");

    Assert.assertNotNull(memoryBlock);
    Assert.assertEquals(PipeMemoryBlockCategory.SINK, memoryBlock.getCategory());
    Assert.assertEquals(
        PipeAsyncThriftRequestMemoryBlock.calculateMemoryUsage(1024, 1024),
        memoryBlock.getMemoryUsageInBytes());
    memoryBlock.close();

    Assert.assertEquals(usedMemoryBefore, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testRequestMemoryBlockResizeTracksBackingArrayCapacity() {
    final PipeMemoryBlock memoryBlock =
        PipeAsyncThriftRequestMemoryBlock.allocateForSerializedBody(
            1024, PipeAsyncThriftRequestMemoryBlockTest.class.getSimpleName());
    final TPipeTransferReq request = new TPipeTransferReq();
    request.body = ByteBuffer.wrap(new byte[512], 64, 128);

    PipeAsyncThriftRequestMemoryBlock.resizeForRequest(memoryBlock, request);

    Assert.assertEquals(
        PipeAsyncThriftRequestMemoryBlock.calculateMemoryUsage(512, 128),
        memoryBlock.getMemoryUsageInBytes());
    memoryBlock.close();
  }

  @Test
  public void testReadOnlyRequestMemoryBlockTracksBufferCapacity() {
    final PipeMemoryBlock memoryBlock =
        PipeAsyncThriftRequestMemoryBlock.allocateForSerializedBody(
            1024, PipeAsyncThriftRequestMemoryBlockTest.class.getSimpleName());
    final TPipeTransferReq request = new TPipeTransferReq();
    request.body = ByteBuffer.wrap(new byte[512], 64, 128).asReadOnlyBuffer();

    PipeAsyncThriftRequestMemoryBlock.resizeForRequest(memoryBlock, request);

    Assert.assertEquals(
        PipeAsyncThriftRequestMemoryBlock.calculateMemoryUsage(512, 128),
        memoryBlock.getMemoryUsageInBytes());
    memoryBlock.close();
  }

  @Test
  public void testCompressionPeakReservationUpdatesGlobalSinkUsage() {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long usedMemoryBefore = memoryManager.getUsedMemorySizeInBytes();
    final TPipeTransferReq request = new TPipeTransferReq();
    request.body = ByteBuffer.wrap(new byte[512], 64, 128);
    final PipeMemoryBlock memoryBlock =
        PipeAsyncThriftRequestMemoryBlock.allocateForRequest(request, "compression-test");

    try {
      final long compressionPeakMemoryUsage = 4096;
      PipeAsyncThriftRequestMemoryBlock.reserveForCompression(
          memoryBlock, request, compressionPeakMemoryUsage);

      Assert.assertEquals(PipeMemoryBlockCategory.SINK, memoryBlock.getCategory());
      Assert.assertTrue(memoryBlock.getMemoryUsageInBytes() >= 512 + compressionPeakMemoryUsage);
      Assert.assertTrue(memoryManager.getUsedMemorySizeInBytes() > usedMemoryBefore);
    } finally {
      memoryBlock.close();
    }

    Assert.assertEquals(usedMemoryBefore, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testSliceReservationCoversTransportCopies() throws Exception {
    final TPipeTransferReq originalRequest = new TPipeTransferReq();
    originalRequest.version = 1;
    originalRequest.type = 1;
    originalRequest.body = ByteBuffer.wrap(new byte[4096]);
    final PipeMemoryBlock memoryBlock =
        PipeAsyncThriftRequestMemoryBlock.allocateForRequest(
            originalRequest, PipeAsyncThriftRequestMemoryBlockTest.class.getSimpleName());

    try {
      final int sliceBodyLength = 1024;
      PipeAsyncThriftRequestMemoryBlock.reserveForSlice(
          memoryBlock, originalRequest, sliceBodyLength);
      Assert.assertTrue(
          memoryBlock.getMemoryUsageInBytes()
              >= 4096L + sliceBodyLength + 3L * (sliceBodyLength + 22));

      final TPipeTransferReq sliceRequest =
          PipeTransferSliceReqBuilder.buildSliceReq(originalRequest, 1, 0, 4, sliceBodyLength);
      PipeAsyncThriftRequestMemoryBlock.resizeForSlice(memoryBlock, originalRequest, sliceRequest);

      final TMemoryBuffer memoryBuffer = new TMemoryBuffer(128);
      final TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(memoryBuffer);
      final IClientRPCService.pipeTransfer_args args = new IClientRPCService.pipeTransfer_args();
      args.setReq(sliceRequest);
      args.write(protocol);

      Assert.assertTrue(
          memoryBlock.getMemoryUsageInBytes()
              >= originalRequest.body.array().length
                  + ((PipeTransferSliceReq) sliceRequest).getSliceBody().length
                  + sliceRequest.body.array().length
                  + memoryBuffer.getArray().length
                  + Integer.BYTES);
    } finally {
      memoryBlock.close();
    }
  }

  @Test
  public void testTsFilePieceBodyUsesExactCapacityAndCanReleaseSourcePayload() throws Exception {
    final byte[] payload = new byte[1024];
    final PipeTransferTsFilePieceReq request =
        PipeTransferTsFilePieceReq.toTPipeTransferReq(
            PipeTransferTsFilePieceReq.class.getSimpleName(), 0, payload);
    final int serializedBodySize =
        PipeTransferFilePieceReq.calculateSerializedSize(
            PipeTransferTsFilePieceReq.class.getSimpleName(), payload.length);

    Assert.assertEquals(serializedBodySize, request.body.remaining());
    Assert.assertEquals(serializedBodySize, request.body.capacity());
    request.releaseFilePiece();
    Assert.assertNull(request.getFilePiece());
  }
}
