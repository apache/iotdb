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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** To read chunk data in memory. */
public class MemChunkReader implements IChunkReader, IPointReader {

  private final MemPointIterator timeValuePairIterator;
  private final Filter globalTimeFilter;
  private final List<IPageReader> pageReaderList;

  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  public MemChunkReader(ReadOnlyMemChunk readableChunk, Filter globalTimeFilter) {
    this.timeValuePairIterator = readableChunk.getMemPointIterator();
    this.globalTimeFilter = globalTimeFilter;
    this.pageReaderList = new ArrayList<>();
    initAllPageReaders(readableChunk.getChunkMetaData(), readableChunk.getPageStatisticsList());
  }

  private void initAllPageReaders(
      IChunkMetadata metadata, List<Statistics<? extends Serializable>> pageStats) {
    Supplier<TsBlock> tsBlockSupplier = new TsBlockSupplier();
    for (int pageIndex = 0; pageIndex < pageStats.size(); pageIndex++) {
      MemPageReader pageReader =
          new MemPageReader(
              tsBlockSupplier,
              pageIndex,
              metadata.getDataType(),
              metadata.getMeasurementUid(),
              pageStats.get(pageIndex),
              globalTimeFilter);
      this.pageReaderList.add(pageReader);
    }
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = timeValuePairIterator.nextTimeValuePair();
      if (globalTimeFilter == null
          || globalTimeFilter.satisfy(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        hasCachedTimeValuePair = true;
        cachedTimeValuePair = timeValuePair;
        break;
      }
    }
    return hasCachedTimeValuePair;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    } else {
      return timeValuePairIterator.nextTimeValuePair();
    }
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    if (!hasCachedTimeValuePair) {
      cachedTimeValuePair = timeValuePairIterator.nextTimeValuePair();
      hasCachedTimeValuePair = true;
    }
    return cachedTimeValuePair;
  }

  @Override
  public boolean hasNextSatisfiedPage() throws IOException {
    return hasNextTimeValuePair();
  }

  @Override
  public BatchData nextPageData() throws IOException {
    return pageReaderList.remove(0).getAllSatisfiedPageData();
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return timeValuePairIterator.getUsedMemorySize();
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return this.pageReaderList;
  }

  /**
   * TsBlockSupplier enables to read pages in MemTable lazily. All MemPageReaders share one
   * TsBlockSupplier object.
   */
  class TsBlockSupplier implements Supplier<TsBlock> {
    private int tsBlockIndex;

    public TsBlockSupplier() {}

    public void setTsBlockIndex(int tsBlockIndex) {
      this.tsBlockIndex = tsBlockIndex;
    }

    @Override
    public TsBlock get() {
      return timeValuePairIterator.getBatch(tsBlockIndex);
    }
  }
}
