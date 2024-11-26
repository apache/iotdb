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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

// difference with AlignedPageReader is that TablePageReader works for TableScan and keep all null
// rows
public class TablePageReader extends AbstractAlignedPageReader {

  public TablePageReader(
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

  public TablePageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
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
  public Statistics<? extends Serializable> getStatistics() {
    return timePageReader.getStatistics();
  }

  @Override
  boolean keepCurrentRow(boolean hasNotNullValues, long timestamp, Object[] rowValues) {
    return satisfyRecordFilter(timestamp, rowValues);
  }

  @Override
  boolean allPageDataSatisfy() {
    return !isModified && globalTimeFilterAllSatisfy() && pushDownFilterAllSatisfy();
  }

  @Override
  void constructResult(boolean[] keepCurrentRow, long[] timeBatch, boolean pushDownFilterAllSatisfy)
      throws IOException {
    // construct time column
    // when pushDownFilterAllSatisfy = true, we can skip rows by OFFSET & LIMIT
    int readEndIndex = buildTimeColumn(timeBatch, keepCurrentRow, pushDownFilterAllSatisfy);
    // construct value columns
    buildValueColumns(readEndIndex, keepCurrentRow, timeBatch);
  }

  private void buildValueColumns(int readEndIndex, boolean[] keepCurrentRow, long[] timeBatch)
      throws IOException {
    for (int i = 0; i < valueCount; i++) {
      ValuePageReader pageReader = valuePageReaderList.get(i);

      if (pageReader != null) {
        if (pageReader.isModified()) {
          boolean[] isDeleted = new boolean[timeBatch.length];
          pageReader.fillIsDeleted(timeBatch, isDeleted, keepCurrentRow);
          pageReader.writeColumnBuilderWithNextBatch(
              readEndIndex, builder.getColumnBuilder(i), keepCurrentRow, isDeleted);
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

  public void setDeleteIntervalList(
      List<TimeRange> timeDeletions, List<List<TimeRange>> valueDeletionsList) {
    timePageReader.setDeleteIntervalList(timeDeletions);
    for (int i = 0; i < valueCount; i++) {
      if (valuePageReaderList.get(i) != null) {
        valuePageReaderList.get(i).setDeleteIntervalList(valueDeletionsList.get(i));
      }
    }
  }
}
