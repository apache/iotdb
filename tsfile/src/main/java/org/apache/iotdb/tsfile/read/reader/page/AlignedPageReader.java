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
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class AlignedPageReader implements IPageReader, IAlignedPageReader {

  private final TimePageReader timePageReader;
  private final List<ValuePageReader> valuePageReaderList;
  private final int valueCount;

  private Filter filter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  private boolean isModified;
  private TsBlockBuilder builder;

  private static final int MASK = 0x80;

  public AlignedPageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
      List<ByteBuffer> valuePageDataList,
      List<TSDataType> valueDataTypeList,
      List<Decoder> valueDecoderList,
      Filter filter) {
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
    this.filter = filter;
    this.valueCount = valuePageReaderList.size();
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(TSDataType.VECTOR, ascending, false);
    int timeIndex = -1;
    while (timePageReader.hasNextTime()) {
      long timestamp = timePageReader.nextTime();
      timeIndex++;
      // if all the sub sensors' value are null in current row, just discard it
      boolean isNull = true;
      Object notNullObject = null;
      TsPrimitiveType[] v = new TsPrimitiveType[valueCount];
      for (int i = 0; i < v.length; i++) {
        ValuePageReader pageReader = valuePageReaderList.get(i);
        v[i] = pageReader == null ? null : pageReader.nextValue(timestamp, timeIndex);
        if (v[i] != null) {
          isNull = false;
          notNullObject = v[i].getValue();
        }
      }
      // Currently, if it's a value filter, it will only accept AlignedPath with only one sub
      // sensor
      if (!isNull && (filter == null || filter.satisfy(timestamp, notNullObject))) {
        pageData.putVector(timestamp, v);
      }
    }
    return pageData.flip();
  }

  private boolean pageSatisfy() {
    if (filter != null) {
      // TODO accept valueStatisticsList to filter
      return filter.satisfy(getStatistics());
    } else {
      // For aligned series, When we only query some measurements under an aligned device, if the
      // values of these queried measurements at a timestamp are all null, the timestamp will not be
      // selected.
      // NOTE: if we change the query semantic in the future for aligned series, we need to remove
      // this check here.
      long rowCount = getTimeStatistics().getCount();
      for (Statistics statistics : getValueStatisticsList()) {
        if (statistics == null || statistics.hasNullValue(rowCount)) {
          return true;
        }
      }
      // When the number of points in all value pages is the same as that in the time page, it means
      // that there is no null value, and all timestamps will be selected.
      if (paginationController.hasCurOffset(rowCount)) {
        paginationController.consumeOffset(rowCount);
        return false;
      }
    }
    return true;
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    builder.reset();
    if (!pageSatisfy()) {
      return builder.build();
    }

    long[] timeBatch = timePageReader.getNextTimeBatch();

    // if all the sub sensors' value are null in current row, just discard it
    // if !filter.satisfy, discard this row
    boolean[] keepCurrentRow = new boolean[timeBatch.length];
    if (filter == null) {
      Arrays.fill(keepCurrentRow, true);
    } else {
      for (int i = 0, n = timeBatch.length; i < n; i++) {
        keepCurrentRow[i] = filter.satisfy(timeBatch[i], null);
      }
    }

    // using bitMap in valuePageReaders to indicate whether columns of current row are all null.
    byte[] bitmask = new byte[(timeBatch.length - 1) / 8 + 1];
    Arrays.fill(bitmask, (byte) 0x00);
    boolean[][] isDeleted = new boolean[valueCount][timeBatch.length];
    for (int columnIndex = 0; columnIndex < valueCount; columnIndex++) {
      ValuePageReader pageReader = valuePageReaderList.get(columnIndex);
      if (pageReader != null) {
        byte[] bitmap = pageReader.getBitmap();
        pageReader.fillIsDeleted(timeBatch, isDeleted[columnIndex]);

        for (int i = 0, n = isDeleted[columnIndex].length; i < n; i++) {
          if (isDeleted[columnIndex][i]) {
            int shift = i % 8;
            bitmap[i / 8] = (byte) (bitmap[i / 8] & (~(MASK >>> shift)));
          }
        }
        for (int i = 0, n = bitmask.length; i < n; i++) {
          bitmask[i] = (byte) (bitmap[i] | bitmask[i]);
        }
      }
    }

    for (int i = 0, n = bitmask.length; i < n; i++) {
      if (bitmask[i] == (byte) 0xFF) {
        // 8 rows are not all null, do nothing
      } else if (bitmask[i] == (byte) 0x00) {
        for (int j = 0; j < 8 && (i * 8 + j < keepCurrentRow.length); j++) {
          keepCurrentRow[i * 8 + j] = false;
        }
      } else {
        for (int j = 0; j < 8 && (i * 8 + j < keepCurrentRow.length); j++) {
          if (((bitmask[i] & 0xFF) & (MASK >>> j)) == 0) {
            keepCurrentRow[i * 8 + j] = false;
          }
        }
      }
    }

    // construct time column
    int readEndIndex = timeBatch.length;
    for (int i = 0; i < timeBatch.length; i++) {
      if (!keepCurrentRow[i]) {
        continue;
      }
      if (paginationController.hasCurOffset()) {
        paginationController.consumeOffset();
        keepCurrentRow[i] = false;
        continue;
      }
      if (paginationController.hasCurLimit()) {
        builder.getTimeColumnBuilder().writeLong(timeBatch[i]);
        builder.declarePosition();
        paginationController.consumeLimit();
      } else {
        readEndIndex = i;
        break;
      }
    }

    // construct value columns
    for (int i = 0; i < valueCount; i++) {
      ValuePageReader pageReader = valuePageReaderList.get(i);
      if (pageReader != null) {
        pageReader.writeColumnBuilderWithNextBatch(
            readEndIndex, builder.getColumnBuilder(i), keepCurrentRow, isDeleted[i]);
      } else {
        for (int j = 0; j < readEndIndex; j++) {
          if (keepCurrentRow[j]) {
            builder.getColumnBuilder(i).appendNull();
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
  public Statistics getStatistics() {
    return valuePageReaderList.size() == 1 && valuePageReaderList.get(0) != null
        ? valuePageReaderList.get(0).getStatistics()
        : timePageReader.getStatistics();
  }

  @Override
  public Statistics getStatistics(int index) {
    ValuePageReader valuePageReader = valuePageReaderList.get(index);
    return valuePageReader == null ? null : valuePageReader.getStatistics();
  }

  @Override
  public Statistics getTimeStatistics() {
    return timePageReader.getStatistics();
  }

  private List<Statistics> getValueStatisticsList() {
    List<Statistics> valueStatisticsList = new ArrayList<>();
    for (ValuePageReader v : valuePageReaderList) {
      valueStatisticsList.add(v == null ? null : v.getStatistics());
    }
    return valueStatisticsList;
  }

  @Override
  public void setFilter(Filter filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = new AndFilter(this.filter, filter);
    }
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
    builder = new TsBlockBuilder(dataTypes);
  }
}
