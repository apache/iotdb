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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.reader;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexEntry;
import org.apache.iotdb.lsm.sstable.fileIO.IFileInput;
import org.apache.iotdb.lsm.sstable.interator.IDiskIterator;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ChunkGroupReader implements IDiskIterator<Integer> {

  private final IFileInput tiFileInput;

  ChunkIndex chunkIndex;

  // The deviceID output by the next iteration
  private Integer nextID;

  private int index;

  private ChunkReader chunkReader;

  public ChunkGroupReader(IFileInput tiFileInput) {
    this.tiFileInput = tiFileInput;
  }

  public ChunkGroupReader(IFileInput tiFileInput, long offset) throws IOException {
    this.tiFileInput = tiFileInput;
    tiFileInput.position(offset);
  }

  public ChunkIndex readChunkIndex(long offset) throws IOException {
    tiFileInput.position(offset);
    ChunkIndex chunkIndex = new ChunkIndex();
    tiFileInput.read(chunkIndex);
    return chunkIndex;
  }

  public RoaringBitmap readAllDeviceID(long offset) throws IOException {
    if (chunkIndex == null) {
      chunkIndex = readChunkIndex(tiFileInput.position());
      if (chunkIndex.getChunkIndexEntries().size() == 0) {
        return new RoaringBitmap();
      }
    }
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (ChunkIndexEntry chunkIndexEntry : chunkIndex.getChunkIndexEntries()) {
      ChunkReader chunkReader = new ChunkReader(tiFileInput);
      roaringBitmap.or(chunkReader.readRoaringBitmap(chunkIndexEntry.getOffset()));
    }
    return roaringBitmap;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (nextID != null) {
      return true;
    }
    if (chunkIndex == null) {
      chunkIndex = readChunkIndex(tiFileInput.position());
      if (chunkIndex.getChunkIndexEntries().size() == 0) {
        return false;
      }
    }
    if (chunkReader != null) {
      if (chunkReader.hasNext()) {
        nextID = chunkReader.next();
        return true;
      } else {
        chunkReader = null;
        index++;
      }
    }
    while (index < chunkIndex.getChunkIndexEntries().size()) {
      chunkReader =
          new ChunkReader(tiFileInput, chunkIndex.getChunkIndexEntries().get(index).getOffset());
      if (chunkReader.hasNext()) {
        nextID = chunkReader.next();
        return true;
      }
      index++;
    }
    return false;
  }

  @Override
  public Integer next() throws IOException {
    if (nextID == null) {
      throw new NoSuchElementException();
    }
    int nowId = nextID;
    nextID = null;
    return nowId;
  }

  public void close() throws IOException {
    if (chunkReader != null) {
      chunkReader.close();
    }
    tiFileInput.close();
  }
}
