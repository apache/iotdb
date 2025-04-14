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

import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** To read aligned chunk data in memory. */
public class MemAlignedChunkReader implements IChunkReader {
  private final MemPointIterator timeValuePairIterator;
  private final Filter globalTimeFilter;
  private final List<IPageReader> pageReaderList;

  public MemAlignedChunkReader(AlignedReadOnlyMemChunk readableChunk, Filter globalTimeFilter) {
    timeValuePairIterator = readableChunk.getMemPointIterator();
    this.globalTimeFilter = globalTimeFilter;
    this.pageReaderList = new ArrayList<>();
    initAllPageReaders(
        readableChunk.getDataTypes(),
        readableChunk.getTimeStatisticsList(),
        readableChunk.getValuesStatisticsList());
  }

  private void initAllPageReaders(
      List<TSDataType> tsDataTypes,
      List<Statistics<? extends Serializable>> timeStatistics,
      List<Statistics<? extends Serializable>[]> valuesStatistics) {
    Supplier<TsBlock> tsBlockSupplier = new MemAlignedChunkReader.TsBlockSupplier();
    for (int pageIndex = 0; pageIndex < timeStatistics.size(); pageIndex++) {
      MemAlignedPageReader pageReader =
          new MemAlignedPageReader(
              tsBlockSupplier,
              pageIndex,
              tsDataTypes,
              timeStatistics.get(pageIndex),
              valuesStatistics.get(pageIndex),
              globalTimeFilter);
      this.pageReaderList.add(pageReader);
    }
  }

  @Override
  public boolean hasNextSatisfiedPage() throws IOException {
    throw new IOException("mem chunk reader does not support this method");
  }

  @Override
  public BatchData nextPageData() throws IOException {
    throw new IOException("mem chunk reader does not support this method");
  }

  @Override
  public void close() {
    // Do nothing because mem chunk reader will not open files
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return this.pageReaderList;
  }

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
