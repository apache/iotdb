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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class PageReader implements IPageReader {

  private PageHeader pageHeader;

  protected TSDataType dataType;

  /** decoder for value column */
  protected Decoder valueDecoder;

  /** decoder for time column */
  protected Decoder timeDecoder;

  /** time column in memory */
  protected ByteBuffer timeBuffer;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  protected Filter filter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public PageReader(
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    if (filter == null || filter.satisfy(getStatistics())) {
      while (timeDecoder.hasNext(timeBuffer)) {
        long timestamp = timeDecoder.readLong(timeBuffer);
        switch (dataType) {
          case BOOLEAN:
            boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBoolean))) {
              pageData.putBoolean(timestamp, aBoolean);
            }
            break;
          case INT32:
            int anInt = valueDecoder.readInt(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
              pageData.putInt(timestamp, anInt);
            }
            break;
          case INT64:
            long aLong = valueDecoder.readLong(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
              pageData.putLong(timestamp, aLong);
            }
            break;
          case FLOAT:
            float aFloat = valueDecoder.readFloat(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
              pageData.putFloat(timestamp, aFloat);
            }
            break;
          case DOUBLE:
            double aDouble = valueDecoder.readDouble(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
              pageData.putDouble(timestamp, aDouble);
            }
            break;
          case TEXT:
            Binary aBinary = valueDecoder.readBinary(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
              pageData.putBinary(timestamp, aBinary);
            }
            break;
          default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
      }
    }
    return pageData.flip();
  }

  private boolean pageSatisfy() {
    if (filter != null) {
      return filter.satisfy(getStatistics());
    } else {
      long rowCount = getStatistics().getCount();
      if (paginationController.hasCurOffset(rowCount)) {
        paginationController.consumeOffset(rowCount);
        return false;
      }
    }
    return true;
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(dataType));
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder valueBuilder = builder.getColumnBuilder(0);
    if (pageSatisfy()) {
      switch (dataType) {
        case BOOLEAN:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
            if (isDeleted(timestamp) || (filter != null && !filter.satisfy(timestamp, aBoolean))) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeBoolean(aBoolean);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case INT32:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            int anInt = valueDecoder.readInt(valueBuffer);
            if (isDeleted(timestamp) || (filter != null && !filter.satisfy(timestamp, anInt))) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeInt(anInt);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case INT64:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            long aLong = valueDecoder.readLong(valueBuffer);
            if (isDeleted(timestamp) || (filter != null && !filter.satisfy(timestamp, aLong))) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeLong(aLong);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case FLOAT:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            float aFloat = valueDecoder.readFloat(valueBuffer);
            if (isDeleted(timestamp) || (filter != null && !filter.satisfy(timestamp, aFloat))) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeFloat(aFloat);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case DOUBLE:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            double aDouble = valueDecoder.readDouble(valueBuffer);
            if (isDeleted(timestamp) || (filter != null && !filter.satisfy(timestamp, aDouble))) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeDouble(aDouble);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        case TEXT:
          while (timeDecoder.hasNext(timeBuffer)) {
            long timestamp = timeDecoder.readLong(timeBuffer);
            Binary aBinary = valueDecoder.readBinary(valueBuffer);
            if (isDeleted(timestamp) || (filter != null && !filter.satisfy(timestamp, aBinary))) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timestamp);
              valueBuilder.writeBinary(aBinary);
              builder.declarePosition();
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return builder.build();
  }

  @Override
  public Statistics getStatistics() {
    return pageHeader.getStatistics();
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

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {}

  protected boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }
}
