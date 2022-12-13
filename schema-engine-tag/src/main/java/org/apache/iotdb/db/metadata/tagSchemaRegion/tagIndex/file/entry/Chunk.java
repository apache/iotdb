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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.IOException;

public class Chunk implements IEntry {
  private ChunkHeader chunkHeader;

  private RoaringBitmap roaringBitmap;

  @Override
  public IEntry deserialize(DataInput input) throws IOException {
    chunkHeader = new ChunkHeader();
    chunkHeader.deserialize(input);
    roaringBitmap = new RoaringBitmap();
    roaringBitmap.deserialize(input);
    return this;
  }

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  public void setChunkHeader(ChunkHeader chunkHeader) {
    this.chunkHeader = chunkHeader;
  }

  public RoaringBitmap getRoaringBitmap() {
    return roaringBitmap;
  }

  public void setRoaringBitmap(RoaringBitmap roaringBitmap) {
    this.roaringBitmap = roaringBitmap;
  }

  @Override
  public String toString() {
    return "Chunk{" + "chunkHeader=" + chunkHeader + ", roaringBitmap=" + roaringBitmap + '}';
  }
}
