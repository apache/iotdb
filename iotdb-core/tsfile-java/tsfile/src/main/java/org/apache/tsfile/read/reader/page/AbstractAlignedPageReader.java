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

package org.apache.tsfile.read.reader.page;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.TsBlockUtil;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public abstract class AbstractAlignedPageReader implements IPageReader {

  protected final TimePageReader timePageReader;
  protected final List<ValuePageReader> valuePageReaderList;
  protected final int valueCount;

  protected final Filter globalTimeFilter;
  protected Filter pushDownFilter;
  protected PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  protected boolean isModified;
  protected TsBlockBuilder builder;

  protected static final int MASK = 0x80;

  @SuppressWarnings("squid:S107")
  AbstractAlignedPageReader(
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

  @SuppressWarnings("squid:S107")
  AbstractAlignedPageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
      // The reason for using Array here, rather than passing in
      // List<LazyLoadPageData> as a parameter, is that after type erasure, it would
      // conflict with the existing constructor.
      LazyLoadPageData[] lazyLoadPageDataArray,
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
                lazyLoadPageDataArray[i],
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
      // if all the sub sensors' value are null in current row, just discard it
      boolean hasNotNullValues = false;
      for (int i = 0; i < valueCount; i++) {
        ValuePageReader pageReader = valuePageReaderList.get(i);
        if (pageReader != null) {
          v[i] = pageReader.nextValue(timestamp, timeIndex);
          rowValues[i] = (v[i] == null) ? null : v[i].getValue();
        } else {
          v[i] = null;
          rowValues[i] = null;
        }
        if (rowValues[i] != null) {
          hasNotNullValues = true;
        }
      }

      if (keepCurrentRow(hasNotNullValues, timestamp, rowValues)
          && !timePageReader.isDeleted(timestamp)) {
        pageData.putVector(timestamp, v);
      }
    }
    return pageData.flip();
  }

  abstract boolean keepCurrentRow(boolean hasNotNullValues, long timestamp, Object[] rowValues);

  protected boolean satisfyRecordFilter(long timestamp, Object[] rowValues) {
    return (globalTimeFilter == null || globalTimeFilter.satisfyRow(timestamp, rowValues))
        && (pushDownFilter == null || pushDownFilter.satisfyRow(timestamp, rowValues));
  }

  @Override
  public int getMeasurementCount() {
    return valueCount;
  }

  abstract boolean allPageDataSatisfy();

  boolean globalTimeFilterAllSatisfy() {
    return globalTimeFilter == null || globalTimeFilter.allSatisfy(this);
  }

  boolean pushDownFilterAllSatisfy() {
    return pushDownFilter == null || pushDownFilter.allSatisfy(this);
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    long[] timeBatch = timePageReader.getNextTimeBatch();

    if (allPageDataSatisfy()) {
      buildResultWithoutAnyFilterAndDelete(timeBatch);
      return builder.build();
    }

    // if !filter.satisfy, discard this row
    boolean[] keepCurrentRow = new boolean[timeBatch.length];
    boolean globalTimeFilterAllSatisfy = globalTimeFilterAllSatisfy();
    if (globalTimeFilterAllSatisfy) {
      Arrays.fill(keepCurrentRow, true);
    } else {
      updateKeepCurrentRowThroughGlobalTimeFilter(keepCurrentRow, timeBatch);
    }

    if (timePageReader.isModified()) {
      updateKeepCurrentRowThroughDeletion(keepCurrentRow, timeBatch);
    }

    boolean pushDownFilterAllSatisfy = pushDownFilterAllSatisfy();

    constructResult(keepCurrentRow, timeBatch, pushDownFilterAllSatisfy);

    TsBlock unFilteredBlock = builder.build();
    if (pushDownFilterAllSatisfy) {
      // OFFSET & LIMIT has been consumed in buildTimeColumn
      return unFilteredBlock;
    }
    builder.reset();
    return TsBlockUtil.applyFilterAndLimitOffsetToTsBlock(
        unFilteredBlock, builder, pushDownFilter, paginationController);
  }

  private void buildResultWithoutAnyFilterAndDelete(long[] timeBatch) throws IOException {
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

  abstract void constructResult(
      boolean[] keepCurrentRow, long[] timeBatch, boolean pushDownFilterAllSatisfy)
      throws IOException;

  private void updateKeepCurrentRowThroughGlobalTimeFilter(
      boolean[] keepCurrentRow, long[] timeBatch) {
    for (int i = 0, n = timeBatch.length; i < n; i++) {
      keepCurrentRow[i] = globalTimeFilter.satisfy(timeBatch[i], null);
    }
  }

  private void updateKeepCurrentRowThroughDeletion(boolean[] keepCurrentRow, long[] timeBatch) {
    for (int i = 0, n = timeBatch.length; i < n; i++) {
      if (keepCurrentRow[i]) {
        keepCurrentRow[i] = !timePageReader.isDeleted(timeBatch[i]);
      }
    }
  }

  protected int buildTimeColumn(
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
    if (paginationController.hasLimit()) {
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

  public TimePageReader getTimePageReader() {
    return timePageReader;
  }

  public List<ValuePageReader> getValuePageReaderList() {
    return valuePageReaderList;
  }
}
