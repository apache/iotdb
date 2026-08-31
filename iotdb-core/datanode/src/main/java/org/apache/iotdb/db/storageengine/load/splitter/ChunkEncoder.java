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
package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Encodes an undecoded source chunk into its physical TsFile chunk bytes. */
public final class ChunkEncoder {

  private ChunkEncoder() {}

  public static EncodedChunkGroup encode(final ChunkData chunkData) throws IOException {
    final MemoryTsFileOutput output = new MemoryTsFileOutput();
    final TsFileIOWriter writer = new TsFileIOWriter(output);
    writer.startChunkGroup(chunkData.getDevice());
    final int chunkStart = (int) writer.getPos();
    try {
      chunkData.writeToFileWriter(writer);
    } catch (Exception e) {
      throw new IOException(e);
    }
    final int chunkEnd = (int) writer.getPos();
    final List<ChunkMetadata> metadata = writer.getChunkMetadataListOfCurrentDeviceInMemory();
    final List<EncodedChunk> chunks = new ArrayList<>(metadata.size());
    for (int i = 0; i < metadata.size(); i++) {
      final int start = (int) metadata.get(i).getOffsetOfChunkHeader();
      final int end =
          i + 1 < metadata.size() ? (int) metadata.get(i + 1).getOffsetOfChunkHeader() : chunkEnd;
      if (end > start && start >= chunkStart) {
        chunks.add(
            new EncodedChunk(
                chunkData.getDevice(),
                chunkData.getTimePartitionSlot(),
                output.copyRange(start, end - start),
                new ChunkMetadata(metadata.get(i))));
      }
    }
    return new EncodedChunkGroup(chunkData.getDevice(), chunkData.getTimePartitionSlot(), chunks);
  }
}
