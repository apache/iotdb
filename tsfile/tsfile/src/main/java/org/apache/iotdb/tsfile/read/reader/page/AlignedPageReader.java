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

package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class AlignedPageReader implements IPageReader {

  private final TimePageReader timePageReader;
  private final List<ValuePageReader> valuePageReaderList;
  private final int valueCount;

  private final Filter globalTimeFilter;
  private Filter pushDownFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  private boolean isModified;
  private TsBlockBuilder builder;

  private static final int MASK = 0x80;

  @SuppressWarnings("squid:S107")
  public AlignedPageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
      List<ByteBuffer> valuePageDataList,
      List<TSDataType> valueDataTypeList,
      List<Decoder> valueDecoderList,
      Filter globalTimeFilter) {
    timePageReader = new TimePageReader(timePageHeader, timePageData, timeDecoder);
    isModified = timePageReader.isModified();
    valuePageReaderList = new ArrayList<>(valuePageHeaderList.size());
    for (int i = 0; i < valuePageHeaderList.size(); i++) {
      if (valuePageHeaderList.get(i) != null) {
        ValuePageReader valuePageReader =
            new ValuePageReader(
                valuePageHeaderList.get(i),
                valuePageDataList.get(i),
                valueDataTypeList.get(i),
                valueDecoderList.get(i));
        valuePageReaderList.add(valuePageReader);
        isModified = isModified || valuePageReader.isModified();
      } else {
        valuePageReaderList.add(null);
      }
    }
    this.globalTimeFilter = globalTimeFilter;
    this.valueCount = valuePageReaderList.size();
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(TSDataType.VECTOR, ascending, false);
    int timeIndex = -1;
    Object[] rowValues = new Object[valueCount];
    while (timePageReader.hasNextTime()) {
      long timestamp = timePageReader.nextTime();
      timeIndex++;

      TsPrimitiveType[] v = new TsPrimitiveType[valueCount];
      for (int i = 0; i < valueCount; i++) {
        ValuePageReader pageReader = valuePageReaderList.get(i);
        if (pageReader != null) {
          v[i] = pageReader.nextValue(timestamp, timeIndex);
          rowValues[i] = (v[i] == null) ? null : v[i].getValue();
        } else {
          v[i] = null;
          rowValues[i] = null;
        }
      }

      if (satisfyRecordFilter(timestamp, rowValues)) {
        pageData.putVector(timestamp, v);
      }
    }
    return pageData.flip();
  }

  private boolean satisfyRecordFilter(long timestamp, Object[] rowValues) {
    return (globalTimeFilter == null || globalTimeFilter.satisfyRow(timestamp, rowValues))
        && (pushDownFilter == null || pushDownFilter.satisfyRow(timestamp, rowValues));
  }

  @Override
  public boolean timeAllSelected() {
    for (int index = 0; index < getMeasurementCount(); index++) {
      if (!hasNullValue(index)) {
        // When there is any value page point number that is the same as the time page,
        // it means that all timestamps in time page will be selected.
        return true;
      }
    }
    return false;
  }

  @Override
  public int getMeasurementCount() {
    return valueCount;
  }

  public IPointReader getLazyPointReader() throws IOException {
    return new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaderList);
  }

  private boolean allPageDataSatisfy() {
    return !isModified
        && timeAllSelected()
        && globalTimeFilterAllSatisfy()
        && pushDownFilterAllSatisfy();
  }

  private boolean globalTimeFilterAllSatisfy() {
    return globalTimeFilter == null || globalTimeFilter.allSatisfy(this);
  }

  private boolean pushDownFilterAllSatisfy() {
    return pushDownFilter == null || pushDownFilter.allSatisfy(this);
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    long[] timeBatch = timePageReader.getNextTimeBatch();

    if (allPageDataSatisfy()) {
      buildResultWithoutAnyFilterAndDelete(timeBatch);
      return builder.build();
    }

    // if all the sub sensors' value are null in current row, just discard it
    // if !filter.satisfy, discard this row
    boolean[] keepCurrentRow = new boolean[timeBatch.length];
    boolean globalTimeFilterAllSatisfy = globalTimeFilterAllSatisfy();
    if (globalTimeFilterAllSatisfy) {
      Arrays.fill(keepCurrentRow, true);
    } else {
      updateKeepCurrentRowThroughGlobalTimeFilter(keepCurrentRow, timeBatch);
    }

    boolean[][] isDeleted = null;
    if ((isModified || !timeAllSelected()) && valueCount != 0) {
      // using bitMap in valuePageReaders to indicate whether columns of current row are all null.
      byte[] bitmask = new byte[(timeBatch.length - 1) / 8 + 1];
      Arrays.fill(bitmask, (byte) 0x00);
      isDeleted = new boolean[valueCount][timeBatch.length];

      fillIsDeletedAndBitMask(timeBatch, isDeleted, bitmask);

      updateKeepCurrentRowThroughBitmask(keepCurrentRow, bitmask);
    }

    boolean pushDownFilterAllSatisfy = pushDownFilterAllSatisfy();

    // construct time column
    // when pushDownFilterAllSatisfy = true, we can skip rows by OFFSET & LIMIT
    int readEndIndex = buildTimeColumn(timeBatch, keepCurrentRow, pushDownFilterAllSatisfy);

    // construct value columns
    buildValueColumns(readEndIndex, keepCurrentRow, isDeleted);

    if (pushDownFilterAllSatisfy) {
      // OFFSET & LIMIT has been consumed in buildTimeColumn
      return builder.build();
    }
    return applyPushDownFilter();
  }

  private void buildResultWithoutAnyFilterAndDelete(long[] timeBatch) {
    if (paginationController.hasCurOffset(timeBatch.length)) {
      paginationController.consumeOffset(timeBatch.length);
    } else {
      int readStartIndex = 0;
      if (paginationController.hasCurOffset()) {
        readStartIndex = (int) paginationController.getCurOffset();
        // consume the remaining offset
        paginationController.consumeOffset(readStartIndex);
      }

      // not included
      int readEndIndex = timeBatch.length;
      if (paginationController.hasCurLimit() && paginationController.getCurLimit() > 0) {
        readEndIndex =
            Math.min(readEndIndex, readStartIndex + (int) paginationController.getCurLimit());
        paginationController.consumeLimit((long) readEndIndex - readStartIndex);
      }

      // construct time column
      for (int i = readStartIndex; i < readEndIndex; i++) {
        builder.getTimeColumnBuilder().writeLong(timeBatch[i]);
        builder.declarePosition();
      }

      // construct value columns
      for (int i = 0; i < valueCount; i++) {
        ValuePageReader pageReader = valuePageReaderList.get(i);
        if (pageReader != null) {
          pageReader.writeColumnBuilderWithNextBatch(
              readStartIndex, readEndIndex, builder.getColumnBuilder(i));
        } else {
          builder.getColumnBuilder(i).appendNull(readEndIndex - readStartIndex);
        }
      }
    }
  }

  private int buildTimeColumn(
      long[] timeBatch, boolean[] keepCurrentRow, boolean pushDownFilterAllSatisfy) {
    if (pushDownFilterAllSatisfy) {
      return buildTimeColumnWithPagination(timeBatch, keepCurrentRow);
    } else {
      return buildTimeColumnWithoutPagination(timeBatch, keepCurrentRow);
    }
  }

  private int buildTimeColumnWithPagination(long[] timeBatch, boolean[] keepCurrentRow) {
    int readEndIndex = timeBatch.length;
    for (int rowIndex = 0; rowIndex < timeBatch.length; rowIndex++) {
      if (keepCurrentRow[rowIndex]) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
          keepCurrentRow[rowIndex] = false;
        } else if (paginationController.hasCurLimit()) {
          builder.getTimeColumnBuilder().writeLong(timeBatch[rowIndex]);
          builder.declarePosition();
          paginationController.consumeLimit();
        } else {
          readEndIndex = rowIndex;
          break;
        }
      }
    }
    return readEndIndex;
  }

  private int buildTimeColumnWithPagination(TsBlock unFilteredBlock, boolean[] keepCurrentRow) {
    int readEndIndex = unFilteredBlock.getPositionCount();
    for (int rowIndex = 0; rowIndex < readEndIndex; rowIndex++) {
      if (keepCurrentRow[rowIndex]) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
          keepCurrentRow[rowIndex] = false;
        } else if (paginationController.hasCurLimit()) {
          builder.getTimeColumnBuilder().writeLong(unFilteredBlock.getTimeByIndex(rowIndex));
          builder.declarePosition();
          paginationController.consumeLimit();
        } else {
          readEndIndex = rowIndex;
          break;
        }
      }
    }
    return readEndIndex;
  }

  private int buildTimeColumnWithoutPagination(long[] timeBatch, boolean[] keepCurrentRow) {
    int readEndIndex = 0;
    for (int i = 0; i < timeBatch.length; i++) {
      if (keepCurrentRow[i]) {
        builder.getTimeColumnBuilder().writeLong(timeBatch[i]);
        builder.declarePosition();
        readEndIndex = i;
      }
    }
    return readEndIndex + 1;
  }

  private void buildValueColumns(
      int readEndIndex, boolean[] keepCurrentRow, boolean[][] isDeleted) {
    for (int i = 0; i < valueCount; i++) {
      ValuePageReader pageReader = valuePageReaderList.get(i);
      if (pageReader != null) {
        if (pageReader.isModified()) {
          pageReader.writeColumnBuilderWithNextBatch(
              readEndIndex,
              builder.getColumnBuilder(i),
              keepCurrentRow,
              Objects.requireNonNull(isDeleted)[i]);
        } else {
          pageReader.writeColumnBuilderWithNextBatch(
              readEndIndex, builder.getColumnBuilder(i), keepCurrentRow);
        }
      } else {
        for (int j = 0; j < readEndIndex; j++) {
          if (keepCurrentRow[j]) {
            builder.getColumnBuilder(i).appendNull();
          }
        }
      }
    }
  }

  private void fillIsDeletedAndBitMask(long[] timeBatch, boolean[][] isDeleted, byte[] bitmask) {
    for (int columnIndex = 0; columnIndex < valueCount; columnIndex++) {
      ValuePageReader pageReader = valuePageReaderList.get(columnIndex);
      if (pageReader != null) {
        byte[] bitmap = pageReader.getBitmap();

        if (pageReader.isModified()) {
          pageReader.fillIsDeleted(timeBatch, isDeleted[columnIndex]);
          updateBitmapThroughIsDeleted(bitmap, isDeleted[columnIndex]);
        }

        for (int i = 0, n = bitmask.length; i < n; i++) {
          bitmask[i] = (byte) (bitmap[i] | bitmask[i]);
        }
      }
    }
  }

  private void updateBitmapThroughIsDeleted(byte[] bitmap, boolean[] isDeleted) {
    for (int i = 0, n = isDeleted.length; i < n; i++) {
      if (isDeleted[i]) {
        int shift = i % 8;
        bitmap[i / 8] = (byte) (bitmap[i / 8] & (~(MASK >>> shift)));
      }
    }
  }

  private void updateKeepCurrentRowThroughGlobalTimeFilter(
      boolean[] keepCurrentRow, long[] timeBatch) {
    for (int i = 0, n = timeBatch.length; i < n; i++) {
      keepCurrentRow[i] = globalTimeFilter.satisfy(timeBatch[i], null);
    }
  }

  private void updateKeepCurrentRowThroughBitmask(boolean[] keepCurrentRow, byte[] bitmask) {
    for (int i = 0, n = bitmask.length; i < n; i++) {
      if (bitmask[i] == (byte) 0xFF) {
        // 8 rows are not all null, do nothing
      } else if (bitmask[i] == (byte) 0x00) {
        Arrays.fill(keepCurrentRow, i * 8, Math.min(i * 8 + 8, keepCurrentRow.length), false);
      } else {
        for (int j = 0; j < 8 && (i * 8 + j < keepCurrentRow.length); j++) {
          if (((bitmask[i] & 0xFF) & (MASK >>> j)) == 0) {
            keepCurrentRow[i * 8 + j] = false;
          }
        }
      }
    }
  }

  private TsBlock applyPushDownFilter() {
    TsBlock unFilteredBlock = builder.build();
    builder.reset();

    boolean[] keepCurrentRow = pushDownFilter.satisfyTsBlock(unFilteredBlock);

    // construct time column
    int readEndIndex = buildTimeColumnWithPagination(unFilteredBlock, keepCurrentRow);

    // construct value columns
    for (int i = 0; i < valueCount; i++) {
      for (int rowIndex = 0; rowIndex < readEndIndex; rowIndex++) {
        if (keepCurrentRow[rowIndex]) {
          if (unFilteredBlock.getValueColumns()[i].isNull(rowIndex)) {
            builder.getColumnBuilder(i).appendNull();
          } else {
            builder
                .getColumnBuilder(i)
                .writeObject(unFilteredBlock.getValueColumns()[i].getObject(rowIndex));
          }
        }
      }
    }
    return builder.build();
  }

  public void setDeleteIntervalList(List<List<TimeRange>> list) {
    for (int i = 0; i < valueCount; i++) {
      if (valuePageReaderList.get(i) != null) {
        valuePageReaderList.get(i).setDeleteIntervalList(list.get(i));
      }
    }
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return valuePageReaderList.size() == 1 && valuePageReaderList.get(0) != null
        ? valuePageReaderList.get(0).getStatistics()
        : timePageReader.getStatistics();
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return timePageReader.getStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    ValuePageReader valuePageReader = valuePageReaderList.get(measurementIndex);
    return Optional.ofNullable(valuePageReader == null ? null : valuePageReader.getStatistics());
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    long rowCount = getTimeStatistics().getCount();
    Optional<Statistics<? extends Serializable>> statistics =
        getMeasurementStatistics(measurementIndex);
    return statistics.map(stat -> stat.hasNullValue(rowCount)).orElse(true);
  }

  @Override
  public void addRecordFilter(Filter filter) {
    this.pushDownFilter = filter;
  }

  @Override
  public void setLimitOffset(PaginationController paginationController) {
    this.paginationController = paginationController;
  }

  @Override
  public boolean isModified() {
    return isModified;
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    if (paginationController.hasCurLimit()) {
      builder =
          new TsBlockBuilder(
              (int)
                  Math.min(
                      paginationController.getCurLimit(),
                      timePageReader.getStatistics().getCount()),
              dataTypes);
    } else {
      builder = new TsBlockBuilder((int) timePageReader.getStatistics().getCount(), dataTypes);
    }
  }
}
