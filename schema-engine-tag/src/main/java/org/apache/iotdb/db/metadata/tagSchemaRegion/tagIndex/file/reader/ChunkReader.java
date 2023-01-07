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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.RoaringBitmapHeader;
import org.apache.iotdb.lsm.sstable.fileIO.ISSTableInputStream;
import org.apache.iotdb.lsm.sstable.interator.IDiskIterator;

import org.roaringbitmap.RoaringBitmap;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * It is used to read chunk-related objects from TiFile, and supports iterative acquisition of
 * deviceID
 */
public class ChunkReader implements IChunkReader {

  private final ISSTableInputStream tiFileInput;

  // The deviceID output by the next iteration
  private Integer nextID;

  private RoaringBitmap roaringBitmap;

  private ChunkHeader chunkHeader;

  private RoaringBitmapHeader roaringBitmapHeader;

  // Record the number of the currently read container during the iteration process
  private int index;

  // Iteratively read data from the RoaringBitmap container
  private IDiskIterator<Integer> containerIterator;

  public ChunkReader(ISSTableInputStream tiFileInput) throws IOException {
    this.tiFileInput = tiFileInput;
  }

  public ChunkReader(ISSTableInputStream tiFileInput, long chunkHeaderOffset) throws IOException {
    this.tiFileInput = tiFileInput;
    tiFileInput.position(chunkHeaderOffset);
  }

  /**
   * Read the {@link org.roaringbitmap.RoaringBitmap RoaringBitmap} from the given position
   *
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     TiFile
   * @return a RoaringBitmap instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public RoaringBitmap readRoaringBitmap(long offset) throws IOException {
    ChunkHeader chunkHeader = readChunkHeader(offset);
    tiFileInput.position(offset - chunkHeader.getSize());
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.deserialize(tiFileInput.wrapAsInputStream());
    return roaringBitmap;
  }

  /**
   * Read the {@link org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader
   * ChunkHeader} from the given position
   *
   * @param offset a non-negative integer counting the number of bytes from the beginning of the
   *     TiFile
   * @return a ChunkHeader instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public ChunkHeader readChunkHeader(long offset) throws IOException {
    ChunkHeader chunkHeader = new ChunkHeader();
    tiFileInput.read(chunkHeader, offset);
    return chunkHeader;
  }

  /**
   * Closes this reader and releases any system resources associated with the reader.
   *
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    tiFileInput.close();
    if (roaringBitmap != null) {
      roaringBitmap.clear();
    }
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
    // We need to first obtain the roaringBitmap size recorded in the chunkHeader, and get the start
    // offset of roaringBitmap in the disk file according to the size
    if (chunkHeader == null) {
      chunkHeader = new ChunkHeader();
      long chunkHeaderOffset = tiFileInput.position();
      chunkHeader = readChunkHeader(chunkHeaderOffset);
      tiFileInput.position(chunkHeaderOffset - chunkHeader.getSize());
    }
    // roaringBitmapHeader records the relevant information of the container
    if (roaringBitmapHeader == null) {
      roaringBitmapHeader = new RoaringBitmapHeader();
      tiFileInput.read(roaringBitmapHeader);
      if (!roaringBitmapHeader.hasRun() || roaringBitmapHeader.getSize() >= 4) {
        tiFileInput.skipBytes(roaringBitmapHeader.getSize() * 4);
      }
    }
    // Records the number of deviceIDs saved for each container
    int[] cardinalities = roaringBitmapHeader.getCardinalities();
    // Records the high 16-bit value of deviceID for each container
    char[] keys = roaringBitmapHeader.getKeys();
    // First determine whether the current container still has data
    if (containerIterator != null) {
      if (containerIterator.hasNext()) {
        nextID = generateId(keys[index], containerIterator.next());
        return true;
      } else {
        // If the current container has been iterated, increase the index to get data from the
        // following container
        index++;
        containerIterator = null;
      }
    }
    while (index < roaringBitmapHeader.getSize()) {
      // Get an iterator over the next container to read
      if (cardinalities[index] > 4096) {
        containerIterator = new BitmapContainerIterator(cardinalities[index]);
      } else {
        containerIterator = new ArrayContainerIterator(cardinalities[index]);
      }
      if (containerIterator.hasNext()) {
        nextID = generateId(keys[index], containerIterator.next());
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
   * Used to iteratively obtain records from {@link org.roaringbitmap.BitmapContainer
   * BitmapContainer}
   */
  private class BitmapContainerIterator implements IDiskIterator<Integer> {

    private int high;
    private List<Integer> ids;
    private Iterator<Integer> iterator;
    private Integer next;
    // The amount of data stored in this container
    private int containerLength;
    // How much data has been read
    private int count;

    public BitmapContainerIterator(int containerLength) {
      this.containerLength = containerLength;
      this.high = 0;
      this.count = 0;
    }

    /**
     * Returns {@code true} if the iteration has more elements. (In other words, returns {@code
     * true} if {@link #next} would return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     * @exception EOFException Signals that an end of file or end of stream has been reached
     *     unexpectedly during input.
     * @exception IOException Signals that an I/O exception of some sort has occurred. This class is
     *     the general class of exceptions produced by failed or interrupted I/O operations.
     */
    @Override
    public boolean hasNext() throws IOException {
      if (next != null) {
        return true;
      }
      if (count < containerLength) {
        if (ids != null) {
          if (iterator.hasNext()) {
            next = (high << 6 | iterator.next());
            count++;
            return true;
          } else {
            ids = null;
            iterator = null;
            high++;
          }
        }
        while (high < 1024) {
          long bitmap = Long.reverseBytes(tiFileInput.readLong());
          ids = parseBitmap(bitmap);
          iterator = ids.iterator();
          if (iterator.hasNext()) {
            next = (high << 6 | iterator.next());
            count++;
            return true;
          }
          high++;
        }
      }
      tiFileInput.skipBytes((1023 - high) * 8);
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
      if (next == null) {
        throw new NoSuchElementException();
      }
      int now = next;
      next = null;
      return now;
    }

    /** Parse the bitmap and get all records */
    private List<Integer> parseBitmap(long bitmap) {
      List<Integer> results = new ArrayList<>();
      long now;
      for (int i = 0; i < 64; i++) {
        now = (long) 1 << i;
        if ((now & bitmap) == now) {
          results.add(i);
        }
      }
      return results;
    }
  }

  /**
   * Used to iteratively obtain records from {@link org.roaringbitmap.ArrayContainer ArrayContainer}
   */
  private class ArrayContainerIterator implements IDiskIterator<Integer> {

    private Character next;
    // The amount of data stored in this container
    private int containerLength;

    private int index;

    public ArrayContainerIterator(int containerLength) {
      this.containerLength = containerLength;
    }

    /**
     * Returns {@code true} if the iteration has more elements. (In other words, returns {@code
     * true} if {@link #next} would return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     * @exception EOFException Signals that an end of file or end of stream has been reached
     *     unexpectedly during input.
     * @exception IOException Signals that an I/O exception of some sort has occurred. This class is
     *     the general class of exceptions produced by failed or interrupted I/O operations.
     */
    @Override
    public boolean hasNext() throws IOException {
      if (next != null) {
        return true;
      }
      if (index < containerLength) {
        next = Character.reverseBytes(tiFileInput.readChar());
        index++;
        return true;
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
      if (next == null) {
        throw new NoSuchElementException();
      }
      int now = next;
      next = null;
      return now;
    }
  }

  private int generateId(int high, int low) {
    return (high << 16) | low;
  }
}
