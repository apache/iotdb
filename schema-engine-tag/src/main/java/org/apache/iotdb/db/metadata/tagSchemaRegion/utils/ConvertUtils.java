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
package org.apache.iotdb.db.metadata.tagSchemaRegion.utils;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.Chunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexEntry;

import org.roaringbitmap.RoaringBitmap;

public class ConvertUtils {
  public static Chunk getChunkFromRoaringBitMap(RoaringBitmap roaringBitmap) {
    Chunk chunk = new Chunk();
    chunk.setRoaringBitmap(roaringBitmap);
    chunk.setChunkHeader(getChunkHeaderFromRoaringBitMap(roaringBitmap));
    return chunk;
  }

  public static ChunkHeader getChunkHeaderFromRoaringBitMap(RoaringBitmap roaringBitmap) {
    ChunkHeader chunkHeader = new ChunkHeader();
    chunkHeader.setSize(roaringBitmap.serializedSizeInBytes());
    return chunkHeader;
  }

  public static ChunkIndexEntry getChunkIndexEntryFromRoaringBitMap(RoaringBitmap roaringBitmap) {
    ChunkIndexEntry chunkIndexEntry = new ChunkIndexEntry();
    int[] results = roaringBitmap.stream().toArray();
    chunkIndexEntry.setCount(results.length);
    if (results.length != 0) {
      chunkIndexEntry.setIdMax(results[results.length - 1]);
      chunkIndexEntry.setIdMin(results[0]);
    }
    return chunkIndexEntry;
  }
}
