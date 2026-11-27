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

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlockCategory;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.nio.ByteBuffer;

public final class PipeAsyncThriftRequestMemoryBlock {

  private static final int T_MEMORY_BUFFER_INITIAL_CAPACITY = 128;
  private static final int PIPE_TRANSFER_MESSAGE_PREFIX_SIZE = 43;
  private static final int STRUCT_STOP_COUNT = 2;
  private static final int FRAME_SIZE_PREFIX_SIZE = Integer.BYTES;
  private static final int SLICE_REQ_METADATA_SIZE = 22;
  private static final int BYTE_ARRAY_OUTPUT_STREAM_INITIAL_CAPACITY = 32;

  private PipeAsyncThriftRequestMemoryBlock() {}

  public static PipeMemoryBlock allocateForSerializedBody(
      final long serializedBodySize, final Object assigner) {
    return allocate(serializedBodySize, serializedBodySize, assigner);
  }

  public static PipeMemoryBlock tryAllocateForSerializedBody(
      final long serializedBodySize, final Object assigner) {
    return tryAllocate(calculateMemoryUsage(serializedBodySize, serializedBodySize), assigner);
  }

  public static PipeMemoryBlock tryAllocateForSerializedBodyWithAdditionalMemory(
      final long serializedBodySize, final long additionalMemoryUsage, final Object assigner) {
    return tryAllocate(
        calculateSerializedBodyMemoryUsage(serializedBodySize, additionalMemoryUsage), assigner);
  }

  public static boolean tryResizeForSerializedBodyWithAdditionalMemory(
      final PipeMemoryBlock memoryBlock,
      final long serializedBodySize,
      final long additionalMemoryUsage) {
    return PipeDataNodeResourceManager.memory()
        .tryResize(
            memoryBlock,
            calculateSerializedBodyMemoryUsage(serializedBodySize, additionalMemoryUsage));
  }

  public static PipeMemoryBlock allocateForRequest(
      final TPipeTransferReq request, final Object assigner) {
    final ByteBuffer body = request == null ? null : request.body;
    if (body == null) {
      return allocate(0, 0, assigner);
    }
    return allocate(getBodyCapacity(body), body.remaining(), assigner);
  }

  public static PipeMemoryBlock tryAllocateForRequest(
      final TPipeTransferReq request, final Object assigner) {
    final ByteBuffer body = request == null ? null : request.body;
    final long bodyLength = getBodyLength(body);
    final long bodyCapacity = getBodyCapacity(body);
    return tryAllocate(calculateMemoryUsage(bodyCapacity, bodyLength), assigner);
  }

  public static void resizeForRequest(
      final PipeMemoryBlock memoryBlock, final TPipeTransferReq request) {
    final ByteBuffer body = request == null ? null : request.body;
    final long bodyLength = body == null ? 0 : body.remaining();
    final long bodyCapacity = getBodyCapacity(body);
    PipeDataNodeResourceManager.memory()
        .forceResize(memoryBlock, calculateMemoryUsage(bodyCapacity, bodyLength));
  }

  public static boolean tryResizeForRequest(
      final PipeMemoryBlock memoryBlock, final TPipeTransferReq request) {
    final ByteBuffer body = request == null ? null : request.body;
    final long bodyLength = getBodyLength(body);
    final long bodyCapacity = getBodyCapacity(body);
    return PipeDataNodeResourceManager.memory()
        .tryResize(memoryBlock, calculateMemoryUsage(bodyCapacity, bodyLength));
  }

  public static void reserveForCompression(
      final PipeMemoryBlock memoryBlock,
      final TPipeTransferReq request,
      final long compressionPeakMemoryUsage) {
    PipeDataNodeResourceManager.memory()
        .forceResize(
            memoryBlock,
            calculateCompressionReservation(memoryBlock, request, compressionPeakMemoryUsage));
  }

  public static boolean tryReserveForCompression(
      final PipeMemoryBlock memoryBlock,
      final TPipeTransferReq request,
      final long compressionPeakMemoryUsage) {
    return PipeDataNodeResourceManager.memory()
        .tryResize(
            memoryBlock,
            calculateCompressionReservation(memoryBlock, request, compressionPeakMemoryUsage));
  }

  private static long calculateCompressionReservation(
      final PipeMemoryBlock memoryBlock,
      final TPipeTransferReq request,
      final long compressionPeakMemoryUsage) {
    final ByteBuffer body = request == null ? null : request.body;
    final long bodyLength = getBodyLength(body);
    final long bodyCapacity = getBodyCapacity(body);
    return Math.max(
        memoryBlock.getMemoryUsageInBytes(),
        Math.max(
            calculateMemoryUsage(bodyCapacity, bodyLength),
            Math.addExact(bodyCapacity, compressionPeakMemoryUsage)));
  }

  public static PipeMemoryBlock tryAllocateForSlice(
      final int sliceBodyLength, final Object assigner) {
    final long sliceRequestBodyLength =
        Math.addExact((long) SLICE_REQ_METADATA_SIZE, sliceBodyLength);
    final long sliceRequestBodyCapacityUpperBound =
        Math.max(
            BYTE_ARRAY_OUTPUT_STREAM_INITIAL_CAPACITY,
            Math.multiplyExact(2, sliceRequestBodyLength));
    final long slicePreparationPeak =
        Math.addExact(sliceBodyLength, Math.multiplyExact(3, sliceRequestBodyLength));
    final long sliceTransferPeak =
        Math.addExact(
            sliceBodyLength,
            calculateMemoryUsage(sliceRequestBodyCapacityUpperBound, sliceRequestBodyLength));
    return tryAllocate(Math.max(slicePreparationPeak, sliceTransferPeak), assigner);
  }

  public static void reserveForSlice(
      final PipeMemoryBlock memoryBlock,
      final TPipeTransferReq originalRequest,
      final int sliceBodyLength) {
    final ByteBuffer originalBody = originalRequest == null ? null : originalRequest.body;
    final long originalBodyCapacity = getBodyCapacity(originalBody);
    final long sliceRequestBodyLength =
        Math.addExact((long) SLICE_REQ_METADATA_SIZE, sliceBodyLength);
    final long sliceRequestBodyCapacityUpperBound =
        Math.max(
            BYTE_ARRAY_OUTPUT_STREAM_INITIAL_CAPACITY,
            Math.multiplyExact(2, sliceRequestBodyLength));
    final long slicePreparationPeak =
        Math.addExact(
            Math.addExact(originalBodyCapacity, sliceBodyLength),
            Math.multiplyExact(3, sliceRequestBodyLength));
    final long sliceTransferPeak =
        Math.addExact(
            Math.addExact(originalBodyCapacity, sliceBodyLength),
            calculateMemoryUsage(sliceRequestBodyCapacityUpperBound, sliceRequestBodyLength));
    PipeDataNodeResourceManager.memory()
        .forceResize(
            memoryBlock,
            Math.max(
                memoryBlock.getMemoryUsageInBytes(),
                Math.max(
                    calculateMemoryUsage(originalBodyCapacity, getBodyLength(originalBody)),
                    Math.max(slicePreparationPeak, sliceTransferPeak))));
  }

  public static void resizeForSlice(
      final PipeMemoryBlock memoryBlock,
      final TPipeTransferReq originalRequest,
      final TPipeTransferReq sliceRequest) {
    final ByteBuffer originalBody = originalRequest == null ? null : originalRequest.body;
    final ByteBuffer sliceBody = sliceRequest == null ? null : sliceRequest.body;
    final long slicePayloadLength =
        sliceRequest instanceof PipeTransferSliceReq
            ? ((PipeTransferSliceReq) sliceRequest).getSliceBody().length
            : 0;
    final long retainedBodyCapacity =
        Math.addExact(getBodyCapacity(originalBody), slicePayloadLength);
    PipeDataNodeResourceManager.memory()
        .forceResize(
            memoryBlock,
            Math.addExact(
                retainedBodyCapacity,
                calculateMemoryUsage(getBodyCapacity(sliceBody), getBodyLength(sliceBody))));
  }

  static long calculateMemoryUsage(final long bodyCapacity, final long bodyLength) {
    if (bodyCapacity < 0 || bodyLength < 0) {
      throw new IllegalArgumentException();
    }
    return Math.addExact(
        Math.addExact(bodyCapacity, bodyLength),
        Math.addExact(calculateThriftBufferPeak(bodyLength), FRAME_SIZE_PREFIX_SIZE));
  }

  private static long calculateSerializedBodyMemoryUsage(
      final long serializedBodySize, final long additionalMemoryUsage) {
    if (serializedBodySize < 0 || additionalMemoryUsage < 0) {
      throw new IllegalArgumentException();
    }
    return Math.addExact(
        calculateMemoryUsage(serializedBodySize, serializedBodySize), additionalMemoryUsage);
  }

  private static PipeMemoryBlock allocate(
      final long bodyCapacity, final long bodyLength, final Object assigner) {
    return PipeDataNodeResourceManager.memory()
        .forceAllocate(
            PipeAsyncThriftRequestMemoryBlock.class.getSimpleName(),
            calculateMemoryUsage(bodyCapacity, bodyLength),
            PipeMemoryBlockCategory.SINK,
            assigner,
            null);
  }

  private static PipeMemoryBlock tryAllocate(final long sizeInBytes, final Object assigner) {
    final PipeMemoryBlock memoryBlock =
        PipeDataNodeResourceManager.memory()
            .tryAllocate(
                PipeAsyncThriftRequestMemoryBlock.class.getSimpleName(),
                sizeInBytes,
                PipeMemoryBlockCategory.SINK,
                assigner);
    if (memoryBlock.getMemoryUsageInBytes() != sizeInBytes) {
      memoryBlock.close();
      return null;
    }
    return memoryBlock;
  }

  private static long calculateThriftBufferPeak(final long bodyLength) {
    long capacity = T_MEMORY_BUFFER_INITIAL_CAPACITY;
    long writtenBytes = PIPE_TRANSFER_MESSAGE_PREFIX_SIZE;
    long peakCapacity = capacity;

    writtenBytes = Math.addExact(writtenBytes, bodyLength);
    if (writtenBytes > capacity) {
      final long newCapacity = Math.max(capacity << 1, writtenBytes);
      peakCapacity = Math.max(peakCapacity, capacity + newCapacity);
      capacity = newCapacity;
    }

    for (int i = 0; i < STRUCT_STOP_COUNT; i++) {
      final long requiredCapacity = writtenBytes + 1;
      if (requiredCapacity > capacity) {
        final long newCapacity = Math.max(capacity << 1, requiredCapacity);
        peakCapacity = Math.max(peakCapacity, capacity + newCapacity);
        capacity = newCapacity;
      }
      writtenBytes++;
    }

    return peakCapacity;
  }

  private static long getBodyCapacity(final ByteBuffer body) {
    return body == null ? 0 : body.hasArray() ? body.array().length : body.capacity();
  }

  private static long getBodyLength(final ByteBuffer body) {
    return body == null ? 0 : body.remaining();
  }
}
