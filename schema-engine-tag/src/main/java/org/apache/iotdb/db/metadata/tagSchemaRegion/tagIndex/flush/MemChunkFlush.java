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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.flush;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.Chunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexEntry;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.FlushRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.FlushResponse;
import org.apache.iotdb.db.metadata.tagSchemaRegion.utils.ConvertUtils;
import org.apache.iotdb.db.metadata.tagSchemaRegion.utils.RoaringBitMapUtils;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** flush for MemChunk */
@FlushProcessor(level = 2)
public class MemChunkFlush extends FlushLevelProcessor<MemChunk, Object, FlushRequest> {

  @Override
  public List<Object> getChildren(
      MemChunk memNode, FlushRequest request, FlushRequestContext context) {
    return null;
  }

  @Override
  public void flush(MemChunk memNode, FlushRequest request, FlushRequestContext context)
      throws IOException {
    FileOutput fileOutput = context.getFileOutput();
    List<RoaringBitmap> roaringBitmaps =
        RoaringBitMapUtils.sliceRoaringBitMap(
            memNode.getRoaringBitmap(), request.getChunkMaxSize());
    ChunkIndex chunkIndex = new ChunkIndex();
    int count = 0;
    for (RoaringBitmap roaringBitmap : roaringBitmaps) {
      Chunk chunk = ConvertUtils.getChunkFromRoaringBitMap(roaringBitmap);
      byte[] bytes = new byte[chunk.getChunkHeader().getSize()];
      roaringBitmap.serialize(ByteBuffer.wrap(bytes));
      fileOutput.write(bytes);
      ChunkIndexEntry chunkIndexEntry =
          ConvertUtils.getChunkIndexEntryFromRoaringBitMap(roaringBitmap);
      chunkIndexEntry.setOffset(fileOutput.write(chunk.getChunkHeader()));
      chunkIndex.getChunkIndexEntries().add(chunkIndexEntry);
      count++;
    }
    chunkIndex.setChunkIndexHeader(new ChunkIndexHeader(count));
    long offset = fileOutput.write(chunkIndex);
    FlushResponse response = context.getResponse();
    response.addChunkOffset(memNode, offset);
  }
}
