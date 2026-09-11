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

import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.BytesUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Calculates absolute TsFile data-zone offsets for independently distributed Chunks. */
public final class ChunkOffsetCalculator {

  private static final long FILE_HEADER_SIZE =
      BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING).length + Byte.BYTES;

  private long nextOffset;
  private IDeviceID currentDevice;
  private long chunkGroupIndex = -1;
  private long chunkGroupHeaderOffset = -1;
  private int nextChunkIndex;

  public ChunkOffsetCalculator() {
    this.nextOffset = FILE_HEADER_SIZE;
  }

  public ChunkLayout allocate(final IDeviceID device, final boolean aligned, final Chunk chunk) {
    Objects.requireNonNull(device);
    Objects.requireNonNull(chunk);

    if (!Objects.equals(currentDevice, device)) {
      currentDevice = device;
      chunkGroupIndex++;
      chunkGroupHeaderOffset = nextOffset;
      nextOffset += getChunkGroupHeaderSize(device);
      nextChunkIndex = 0;
    }

    final long chunkOffset = nextOffset;
    final long chunkLength =
        chunk.getHeader().getSerializedSize() + (long) chunk.getData().remaining();
    final boolean firstChunkOfGroup = nextChunkIndex == 0;
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

  public ChunkData.ChunkLayout assign(final ChunkData chunkData, final Chunk chunk) {
    Objects.requireNonNull(chunkData);
    Objects.requireNonNull(chunk);

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

  public ChunkData.ChunkLayout assign(final ChunkData chunkData) {
    Objects.requireNonNull(chunkData);
    final List<Chunk> chunks = chunkData.getChunks();
    if (chunks.isEmpty()) {
      throw new IllegalArgumentException("ChunkData contains no physical chunk");
    }

    final ChunkLayout first = allocate(chunkData.getDevice(), chunkData.isAligned(), chunks.get(0));
    long totalLength = first.length();
    for (int i = 1; i < chunks.size(); i++) {
      totalLength += allocate(chunkData.getDevice(), chunkData.isAligned(), chunks.get(i)).length();
    }
    final ChunkData.ChunkLayout layout =
        new ChunkData.ChunkLayout(
            first.chunkGroupIndex(),
            first.chunkGroupHeaderOffset(),
            first.offset(),
            totalLength,
            first.chunkIndexInGroup(),
            first.firstChunkOfGroup());
    chunkData.setChunkLayout(layout);
    return layout;
  }

  private int getChunkGroupHeaderSize(final IDeviceID device) {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      return new ChunkGroupHeader(device).serializeTo(output);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

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
  }
}
