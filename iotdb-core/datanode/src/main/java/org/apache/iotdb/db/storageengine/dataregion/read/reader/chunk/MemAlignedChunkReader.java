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
import org.apache.iotdb.db.utils.datastructure.MergeSortAlignedTVListIterator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

/** To read aligned chunk data in memory. */
public class MemAlignedChunkReader implements IChunkReader {
  private final AlignedReadOnlyMemChunk readableChunk;
  private final MergeSortAlignedTVListIterator timeValuePairIterator;
  private final Filter globalTimeFilter;
  private final List<IPageReader> pageReaderList;

  public MemAlignedChunkReader(AlignedReadOnlyMemChunk readableChunk, Filter globalTimeFilter) {
    this.readableChunk = readableChunk;
    timeValuePairIterator = readableChunk.getMergeSortAlignedTVListIterator().clone();
    this.globalTimeFilter = globalTimeFilter;
    this.pageReaderList = new ArrayList<>();
    initAllPageReaders(
        readableChunk.getDataTypes(),
        readableChunk.getTimeStatisticsList(),
        readableChunk.getValuesStatisticsList(),
        readableChunk.getPageOffsetsList());
  }

  private void initAllPageReaders(
      List<TSDataType> tsDataTypes,
      List<Statistics<? extends Serializable>> timeStatistics,
      List<Statistics<? extends Serializable>[]> valuesStatistics,
      List<int[]> pageOffsetsList) {
    Supplier<TsBlock> tsBlockSupplier = new MemAlignedChunkReader.TsBlockSupplier();
    for (int i = 0; i < timeStatistics.size(); i++) {
      MemAlignedPageReader pageReader =
          new MemAlignedPageReader(
              tsBlockSupplier,
              timeValuePairIterator,
              pageOffsetsList.get(i),
              pageOffsetsList.get(i + 1),
              tsDataTypes,
              timeStatistics.get(i),
              valuesStatistics.get(i),
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
    private int[] pageEndOffsets;

    public TsBlockSupplier() {}

    public void setPageEndOffsets(int[] pageEndOffsets) {
      this.pageEndOffsets = pageEndOffsets;
    }

    @Override
    public TsBlock get() {
      return buildTsBlock();
    }

    private TsBlock buildTsBlock() {
      try {
        List<TSDataType> tsDataTypes = readableChunk.getDataTypes();
        TsBlockBuilder builder = new TsBlockBuilder(tsDataTypes);
        writeValidValuesIntoTsBlock(builder);
        return builder.build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private boolean isOutOfMemPageBounds() {
      if (pageEndOffsets == null) {
        return false;
      }
      int[] currTvListOffsets = timeValuePairIterator.getAlignedTVListOffsets();
      for (int i = 0; i < pageEndOffsets.length; i++) {
        if (currTvListOffsets[i] < pageEndOffsets[i]) {
          return false;
        }
      }
      return true;
    }

    // read one page and write to tsblock
    private synchronized void writeValidValuesIntoTsBlock(TsBlockBuilder builder) {
      List<TSDataType> tsDataTypes = readableChunk.getDataTypes();
      List<List<TimeRange>> valueColumnsDeletionList = readableChunk.getValueColumnsDeletionList();

      int[] timeDeleteCursor = new int[] {0};
      List<int[]> valueColumnDeleteCursor = new ArrayList<>();
      if (valueColumnsDeletionList != null) {
        valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
      }

      while (timeValuePairIterator.hasNextTimeValuePair()) {
        if (isOutOfMemPageBounds()) {
          break;
        }

        TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
        TsPrimitiveType[] values = tvPair.getValue().getVector();

        BitMap bitMap = new BitMap(tsDataTypes.size());
        for (int columnIndex = 0; columnIndex < tsDataTypes.size(); columnIndex++) {
          if (values[columnIndex] == null) {
            bitMap.mark(columnIndex);
          } else if (valueColumnsDeletionList != null
              && isPointDeleted(
                  tvPair.getTimestamp(),
                  valueColumnsDeletionList.get(columnIndex),
                  valueColumnDeleteCursor.get(columnIndex))) {
            values[columnIndex] = null;
            bitMap.mark(columnIndex);
          }
        }
        if (bitMap.isAllMarked()) {
          timeValuePairIterator.step();
          continue;
        }

        // time column
        builder.getTimeColumnBuilder().writeLong(tvPair.getTimestamp());
        // value columns
        for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
          if (values[columnIndex] == null) {
            builder.getColumnBuilder(columnIndex).appendNull();
            continue;
          }
          ColumnBuilder valueBuilder = builder.getColumnBuilder(columnIndex);
          switch (tsDataTypes.get(columnIndex)) {
            case BOOLEAN:
              valueBuilder.writeBoolean(values[columnIndex].getBoolean());
              break;
            case INT32:
            case DATE:
              valueBuilder.writeInt(values[columnIndex].getInt());
              break;
            case INT64:
            case TIMESTAMP:
              valueBuilder.writeLong(values[columnIndex].getLong());
              break;
            case FLOAT:
              valueBuilder.writeFloat(values[columnIndex].getFloat());
              break;
            case DOUBLE:
              valueBuilder.writeDouble(values[columnIndex].getDouble());
              break;
            case TEXT:
            case BLOB:
            case STRING:
              valueBuilder.writeBinary(values[columnIndex].getBinary());
              break;
            default:
              break;
          }
        }
        builder.declarePosition();
      }
      if (builder.getPositionCount() > readableChunk.getMaxNumberOfPointsInPage()) {
        throw new RuntimeException(
            String.format(
                "Points in current page %d is larger than %d",
                builder.getPositionCount(), readableChunk.getMaxNumberOfPointsInPage()));
      }
    }
  }
}
