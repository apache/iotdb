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
 */
package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.BytesUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

/**
 * Calculates deterministic, absolute physical TsFile data-zone offsets for independently
 * distributed chunks, enabling concurrent out-of-order writes.
 */
public final class ChunkOffsetCalculator {

  private static final long FILE_HEADER_SIZE =
      BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING).length + Byte.BYTES;

  private final CountingOutputStream countingStream = new CountingOutputStream();

  private long nextOffset;
  private IDeviceID currentDevice;
  private long chunkGroupIndex = -1;
  private long chunkGroupHeaderOffset = -1;
  private int nextChunkIndex;

  public ChunkOffsetCalculator() {
    this.nextOffset = FILE_HEADER_SIZE;
  }

  // -------------------------------------------------------------------------
  // Offset Allocation & Layout Assignment
  // -------------------------------------------------------------------------

  /** Allocates physical file offset and group layout for a single chunk. */
  public ChunkLayout allocate(final IDeviceID device, final boolean aligned, final Chunk chunk) {
    Objects.requireNonNull(device, DataNodeQueryMessages.EXCEPTION_DEVICE_CANNOT_BE_NULL_F1EB20B6);
    Objects.requireNonNull(chunk, DataNodeQueryMessages.EXCEPTION_CHUNK_CANNOT_BE_NULL_280ECF98);

    // Start a new chunk group if target device switches
    if (!Objects.equals(currentDevice, device)) {
      currentDevice = device;
      chunkGroupIndex++;
      chunkGroupHeaderOffset = nextOffset;
      nextOffset += computeChunkGroupHeaderSize(device);
      nextChunkIndex = 0;
    }

    final long chunkOffset = nextOffset;
    final long chunkLength =
        computeChunkHeaderSize(chunk.getHeader()) + (long) chunk.getData().remaining();
    final boolean firstChunkOfGroup = (nextChunkIndex == 0);

    final ChunkLayout layout =
        new ChunkLayout(
            device,
            aligned,
            chunkGroupIndex,
            chunkGroupHeaderOffset,
            firstChunkOfGroup,
            chunk,
            chunkOffset,
            chunkLength,
            nextChunkIndex);

    nextOffset += chunkLength;
    nextChunkIndex++;
    return layout;
  }

  /** Allocates offset for one chunk and binds the resulting layout to {@link ChunkData}. */
  public ChunkData.ChunkLayout assign(final ChunkData chunkData, final Chunk chunk) {
    Objects.requireNonNull(
        chunkData, DataNodeQueryMessages.EXCEPTION_CHUNKDATA_CANNOT_BE_NULL_7D931C4D);
    final ChunkLayout allocated = allocate(chunkData.getDevice(), chunkData.isAligned(), chunk);
    final ChunkData.ChunkLayout layout =
        new ChunkData.ChunkLayout(
            allocated.chunkGroupIndex(),
            allocated.chunkGroupHeaderOffset(),
            allocated.offset(),
            allocated.length(),
            allocated.chunkIndexInGroup(),
            allocated.firstChunkOfGroup());
    chunkData.setChunkLayout(layout);
    return layout;
  }

  /** Sequentially allocates all chunks inside {@link ChunkData} and sets the aggregated layout. */
  public ChunkData.ChunkLayout assign(final ChunkData chunkData) {
    Objects.requireNonNull(
        chunkData, DataNodeQueryMessages.EXCEPTION_CHUNKDATA_CANNOT_BE_NULL_7D931C4D);
    final List<Chunk> chunks = chunkData.getChunks();
    if (chunks.isEmpty()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_CHUNKDATA_CONTAINS_NO_PHYSICAL_CHUNK_607BCFC8);
    }

    final IDeviceID device = chunkData.getDevice();
    final boolean aligned = chunkData.isAligned();

    // Allocate first chunk to establish baseline layout metrics
    final ChunkLayout firstLayout = allocate(device, aligned, chunks.get(0));
    long totalLength = firstLayout.length();

    // Allocate subsequent chunks continuously within the same group
    final int size = chunks.size();
    for (int i = 1; i < size; i++) {
      totalLength += allocate(device, aligned, chunks.get(i)).length();
    }

    final ChunkData.ChunkLayout aggregatedLayout =
        new ChunkData.ChunkLayout(
            firstLayout.chunkGroupIndex(),
            firstLayout.chunkGroupHeaderOffset(),
            firstLayout.offset(),
            totalLength,
            firstLayout.chunkIndexInGroup(),
            firstLayout.firstChunkOfGroup());

    chunkData.setChunkLayout(aggregatedLayout);
    return aggregatedLayout;
  }

  // -------------------------------------------------------------------------
  // Zero-Allocation Header Size Calculation
  // -------------------------------------------------------------------------

  private int computeChunkGroupHeaderSize(final IDeviceID device) {
    final IDeviceID normalizedDevice =
        device instanceof StringArrayDeviceID ? device : new StringArrayDeviceID(device.toString());
    try {
      countingStream.reset();
      return new ChunkGroupHeader(normalizedDevice).serializeTo(countingStream);
    } catch (final IOException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_FAILED_TO_COMPUTE_CHUNK_GROUP_HEADER_SIZE_E6B40B2C, e);
    }
  }

  private int computeChunkHeaderSize(final ChunkHeader chunkHeader) {
    try {
      countingStream.reset();
      return chunkHeader.serializeTo(countingStream);
    } catch (final IOException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_FAILED_TO_COMPUTE_CHUNK_HEADER_SIZE_2E88289A, e);
    }
  }

  // -------------------------------------------------------------------------
  // State Accessors & Lifecycle
  // -------------------------------------------------------------------------

  public long getNextOffset() {
    return nextOffset;
  }

  public long getChunkGroupIndex() {
    return chunkGroupIndex;
  }

  public long getChunkGroupHeaderOffset() {
    return chunkGroupHeaderOffset;
  }

  public void reset() {
    nextOffset = FILE_HEADER_SIZE;
    currentDevice = null;
    chunkGroupIndex = -1;
    chunkGroupHeaderOffset = -1;
    nextChunkIndex = 0;
    countingStream.reset();
  }

  // -------------------------------------------------------------------------
  // Performance Utilities
  // -------------------------------------------------------------------------

  /**
   * Lightweight non-buffering stream that counts serialized bytes without allocating byte arrays.
   */
  private static final class CountingOutputStream extends OutputStream {
    private int count;

    @Override
    public void write(final int b) {
      count++;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
      count += len;
    }

    public void reset() {
      count = 0;
    }

    public int getCount() {
      return count;
    }
  }
}
