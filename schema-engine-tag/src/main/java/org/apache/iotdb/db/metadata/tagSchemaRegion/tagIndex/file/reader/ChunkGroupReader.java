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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkMetaEntry;
import org.apache.iotdb.lsm.sstable.fileIO.ISSTableInputStream;

import org.roaringbitmap.RoaringBitmap;

import java.io.EOFException;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * It is used to read chunk group-related objects from TiFile, and supports iterative acquisition of
 * deviceID
 */
public class ChunkGroupReader implements IChunkGroupReader {

  private final ISSTableInputStream tiFileInput;

  ChunkIndex chunkIndex;

  // The deviceID output by the next iteration
  private Integer nextID;

  private int index;

  private ChunkReader chunkReader;

  public ChunkGroupReader(ISSTableInputStream tiFileInput) {
    this.tiFileInput = tiFileInput;
  }

  public ChunkGroupReader(ISSTableInputStream tiFileInput, long offset) throws IOException {
    this.tiFileInput = tiFileInput;
    tiFileInput.position(offset);
  }

  /**
   * Read chunk index from the specified location in the file
   *
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     TiFile
   * @return a Chunk Index instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public ChunkIndex readChunkIndex(long offset) throws IOException {
    tiFileInput.position(offset);
    ChunkIndex chunkIndex = new ChunkIndex();
    tiFileInput.read(chunkIndex);
    return chunkIndex;
  }

  /**
   * Read all ids from the specified location in the file
   *
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     TiFile
   * @return a {@link org.roaringbitmap.RoaringBitmap RoaringBitmap} instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public RoaringBitmap readAllDeviceID(long offset) throws IOException {
    if (chunkIndex == null) {
      chunkIndex = readChunkIndex(offset);
      if (chunkIndex.getChunkMetaEntries().size() == 0) {
        return new RoaringBitmap();
      }
    }
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (ChunkMetaEntry chunkMetaEntry : chunkIndex.getChunkMetaEntries()) {
      ChunkReader chunkReader = new ChunkReader(tiFileInput);
      roaringBitmap.or(chunkReader.readRoaringBitmap(chunkMetaEntry.getOffset()));
    }
    return roaringBitmap;
  }

  /**
   * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true}
   * if {@link #next} would return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   * @exception EOFException Signals that an end of file or end of stream has been reached
   *     unexpectedly during input.
   * @exception IOException Signals that an I/O exception of some sort has occurred. This class is
   *     the general class of exceptions produced by failed or interrupted I/O operations.
   */
  @Override
  public boolean hasNext() throws IOException {
    if (nextID != null) {
      return true;
    }
    if (chunkIndex == null) {
      chunkIndex = readChunkIndex(tiFileInput.position());
      if (chunkIndex.getChunkMetaEntries().size() == 0) {
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
    while (index < chunkIndex.getChunkMetaEntries().size()) {
      chunkReader =
          new ChunkReader(tiFileInput, chunkIndex.getChunkMetaEntries().get(index).getOffset());
      if (chunkReader.hasNext()) {
        nextID = chunkReader.next();
        return true;
      }
      index++;
    }
    return false;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   * @throws NoSuchElementException if the iteration has no more elements
   */
  @Override
  public Integer next() {
    if (nextID == null) {
      throw new NoSuchElementException();
    }
    int nowId = nextID;
    nextID = null;
    return nowId;
  }

  /**
   * Closes this reader and releases any system resources associated with the reader.
   *
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    if (chunkReader != null) {
      chunkReader.close();
    }
    tiFileInput.close();
  }
}
