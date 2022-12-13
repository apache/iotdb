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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.RoaringBitmapHeader;
import org.apache.iotdb.lsm.sstable.reader.DiskIterator;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ChunkReader implements IChunkReader {

  private final RandomAccessFile tiFileIuput;

  private Integer nextID;

  private RoaringBitmap roaringBitmap;

  private final ChunkHeader chunkHeader;

  private RoaringBitmapHeader roaringBitmapHeader;

  private int index;

  private DiskIterator<Integer> containerIterator;

  public ChunkReader(RandomAccessFile tiFileIuput) throws IOException {
    this.tiFileIuput = tiFileIuput;
    chunkHeader = new ChunkHeader();
    chunkHeader.deserialize(tiFileIuput);
  }

  @Override
  public RoaringBitmap readRoaringBitmap() throws IOException {
    if (roaringBitmap == null) {
      roaringBitmap = new RoaringBitmap();
      roaringBitmap.deserialize(tiFileIuput);
      return roaringBitmap;
    }
    return roaringBitmap;
  }

  @TestOnly
  @Override
  public void close() throws IOException {
    tiFileIuput.close();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (nextID != null) {
      return true;
    }
    if (roaringBitmapHeader == null) {
      roaringBitmapHeader = new RoaringBitmapHeader();
      roaringBitmapHeader = (RoaringBitmapHeader) roaringBitmapHeader.deserialize(tiFileIuput);
      if (!roaringBitmapHeader.hasRun() || roaringBitmapHeader.getSize() >= 4) {
        tiFileIuput.skipBytes(roaringBitmapHeader.getSize() * 4);
      }
    }
    int[] cardinalities = roaringBitmapHeader.getCardinalities();
    char[] keys = roaringBitmapHeader.getKeys();
    if (containerIterator != null) {
      if (containerIterator.hasNext()) {
        nextID = generateId(keys[index], containerIterator.next());
        return true;
      } else {
        index++;
        containerIterator = null;
      }
    }
    while (index < roaringBitmapHeader.getSize()) {
      if (containerIterator == null) {
        if (cardinalities[index] > 4096) {
          containerIterator = new BitmapContainerIterator(cardinalities[index]);
        } else {
          containerIterator = new ArrayContainerIterator(cardinalities[index]);
        }
      }
      if (containerIterator.hasNext()) {
        nextID = generateId(keys[index], containerIterator.next());
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

  private class BitmapContainerIterator implements DiskIterator<Integer> {

    private int high;
    private List<Integer> ids;
    private Iterator<Integer> iterator;
    private Integer next;
    private int containerLength;
    private int count;

    public BitmapContainerIterator(int containerLength) {
      this.containerLength = containerLength;
      this.high = 0;
      this.count = 0;
    }

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
          long bitmap = Long.reverseBytes(tiFileIuput.readLong());
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
      tiFileIuput.skipBytes((1023 - high) * 8);
      return false;
    }

    @Override
    public Integer next() throws IOException {
      if (next == null) {
        throw new NoSuchElementException();
      }
      int now = next;
      next = null;
      return now;
    }

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

  private class ArrayContainerIterator implements DiskIterator<Integer> {

    private Character next;

    private int containerLength;

    private int index;

    public ArrayContainerIterator(int containerLength) {
      this.containerLength = containerLength;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (next != null) {
        return true;
      }
      if (index < containerLength) {
        next = Character.reverseBytes(tiFileIuput.readChar());
        index++;
        return true;
      }
      return false;
    }

    @Override
    public Integer next() throws IOException {
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
