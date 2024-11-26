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
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class AlignedPageReader extends AbstractAlignedPageReader {

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
    super(
        timePageHeader,
        timePageData,
        timeDecoder,
        valuePageHeaderList,
        valuePageDataList,
        valueDataTypeList,
        valueDecoderList,
        globalTimeFilter);
  }

  @SuppressWarnings("squid:S107")
  public AlignedPageReader(
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
    super(
        timePageHeader,
        timePageData,
        timeDecoder,
        valuePageHeaderList,
        lazyLoadPageDataArray,
        valueDataTypeList,
        valueDecoderList,
        globalTimeFilter);
  }

  @Override
  boolean keepCurrentRow(boolean hasNotNullValues, long timestamp, Object[] rowValues) {
    return hasNotNullValues && satisfyRecordFilter(timestamp, rowValues);
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

  public IPointReader getLazyPointReader() throws IOException {
    return new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaderList);
  }

  @Override
  boolean allPageDataSatisfy() {
    return !isModified
        && timeAllSelected()
        && globalTimeFilterAllSatisfy()
        && pushDownFilterAllSatisfy();
  }

  @Override
  void constructResult(boolean[] keepCurrentRow, long[] timeBatch, boolean pushDownFilterAllSatisfy)
      throws IOException {

    // if all the sub sensors' value are null in current row, just discard it
    boolean[][] isDeleted = null;
    if ((isModified || !timeAllSelected()) && valueCount != 0) {
      // using bitMap in valuePageReaders to indicate whether columns of current row are all null.
      byte[] bitmask = new byte[(timeBatch.length - 1) / 8 + 1];
      Arrays.fill(bitmask, (byte) 0x00);
      isDeleted = new boolean[valueCount][timeBatch.length];

      fillIsDeletedAndBitMask(timeBatch, isDeleted, bitmask);

      updateKeepCurrentRowThroughBitmask(keepCurrentRow, bitmask);
    }

    // construct time column
    // when pushDownFilterAllSatisfy = true, we can skip rows by OFFSET & LIMIT
    int readEndIndex = buildTimeColumn(timeBatch, keepCurrentRow, pushDownFilterAllSatisfy);

    // construct value columns
    buildValueColumns(readEndIndex, keepCurrentRow, isDeleted);
  }

  private void buildValueColumns(int readEndIndex, boolean[] keepCurrentRow, boolean[][] isDeleted)
      throws IOException {
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

  private void fillIsDeletedAndBitMask(long[] timeBatch, boolean[][] isDeleted, byte[] bitmask)
      throws IOException {
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
}
