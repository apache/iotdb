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
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  public void split4CPV(
      long startTime,
      long endTime,
      long interval,
      long curStartTime,
      List<ChunkSuit4CPV> currentChunkList,
      List<ChunkSuit4CPV> futureChunkList,
      ChunkMetadata chunkMetadata)
      throws IOException { // note: [startTime,endTime), [curStartTime,curEndTime]
    Map<Integer, BatchData> splitBatchDataMap = new HashMap<>();
    Map<Integer, ChunkMetadata> splitChunkMetadataMap = new HashMap<>();
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      // prepare corresponding batchData
      if (timestamp < curStartTime) {
        continue;
      }
      if (timestamp >= endTime) {
        break;
      }
      int idx = (int) Math.floor((timestamp - startTime) * 1.0 / interval);
      if (!splitBatchDataMap.containsKey(idx)) {
        // create batchData
        BatchData batch1 = BatchDataFactory.createBatchData(dataType, true, false);
        splitBatchDataMap.put(idx, batch1);
        Statistics statistics = null;
        switch (dataType) {
          case INT32:
            statistics = new IntegerStatistics();
            break;
          case INT64:
            statistics = new LongStatistics();
            break;
          case FLOAT:
            statistics = new FloatStatistics();
            break;
          case DOUBLE:
            statistics = new DoubleStatistics();
            break;
          default:
            break;
        }
        // create chunkMetaData
        ChunkMetadata chunkMetadata1 =
            new ChunkMetadata(
                chunkMetadata.getMeasurementUid(),
                chunkMetadata.getDataType(),
                chunkMetadata.getOffsetOfChunkHeader(),
                statistics);
        chunkMetadata1.setVersion(chunkMetadata.getVersion()); // don't miss this

        // important, used later for candidate point verification
        // (1) candidate point itself whether is in the deleted interval
        // (2) candidate point whether is overlapped by a chunk with a larger version number and
        // the chunk does not have a deleted interval overlapping this candidate point
        chunkMetadata1.setDeleteIntervalList(chunkMetadata.getDeleteIntervalList());
        // not use current Ii to modify deletedIntervalList any more

        splitChunkMetadataMap.put(idx, chunkMetadata1);
      }
      BatchData batchData1 = splitBatchDataMap.get(idx);
      ChunkMetadata chunkMetadata1 = splitChunkMetadataMap.get(idx);
      switch (dataType) {
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
            // update batchData1
            batchData1.putInt(timestamp, anInt);
            // update statistics of chunkMetadata1
            chunkMetadata1.getStatistics().update(timestamp, anInt); // TODO skeptical bug ?
          }
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
            // update batchData1
            batchData1.putLong(timestamp, aLong);
            // update statistics of chunkMetadata1
            chunkMetadata1.getStatistics().update(timestamp, aLong); // TODO skeptical bug ?
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
            // update batchData1
            batchData1.putFloat(timestamp, aFloat);
            // update statistics of chunkMetadata1
            chunkMetadata1.getStatistics().update(timestamp, aFloat); // TODO skeptical bug ?
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
            // update batchData1
            batchData1.putDouble(timestamp, aDouble);
            // update statistics of chunkMetadata1
            chunkMetadata1.getStatistics().update(timestamp, aDouble); // TODO skeptical bug ?
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    int curIdx = (int) Math.floor((curStartTime - startTime) * 1.0 / interval);
    int num = (int) Math.floor((endTime - startTime) * 1.0 / interval);
    for (int i = 0; i < num; i++) {
      if (splitBatchDataMap.containsKey(i) && i == curIdx && !splitBatchDataMap.get(i).isEmpty()) {
        currentChunkList.add(
            new ChunkSuit4CPV(splitChunkMetadataMap.get(i), splitBatchDataMap.get(i).flip()));
      } else if (splitBatchDataMap.containsKey(i)
          && i != curIdx
          && !splitBatchDataMap.get(i).isEmpty()) {
        futureChunkList.add(
            new ChunkSuit4CPV(splitChunkMetadataMap.get(i), splitBatchDataMap.get(i).flip()));
      }
    }
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {

    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);

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
    return pageData.flip();
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
