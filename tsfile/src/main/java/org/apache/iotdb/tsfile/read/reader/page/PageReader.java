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
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.Operation;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
  public ByteBuffer timeBuffer;

  /** value column in memory */
  public ByteBuffer valueBuffer;

  public int timeBufferLength;

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
    timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /** the chunk partially overlaps in time with the current M4 interval Ii */
  public void split4CPV(
      long startTime,
      long endTime,
      long interval,
      long curStartTime,
      List<ChunkSuit4CPV> currentChunkList,
      Map<Integer, List<ChunkSuit4CPV>> splitChunkList,
      ChunkMetadata chunkMetadata)
      throws IOException { // note: [startTime,endTime), [curStartTime,curEndTime)
    // endTime is excluded so -1
    int numberOfSpans =
        (int)
                Math.floor(
                    (Math.min(chunkMetadata.getEndTime(), endTime - 1) - curStartTime)
                        * 1.0
                        / interval)
            + 1;
    for (int n = 0; n < numberOfSpans; n++) {
      long leftEndIncluded = curStartTime + n * interval;
      long rightEndExcluded = curStartTime + (n + 1) * interval;
      ChunkSuit4CPV chunkSuit4CPV = new ChunkSuit4CPV(chunkMetadata, this, true);
      // TODO update FP,LP with the help of stepRegress index. BP/TP not update here.
      int FP_pos = -1;
      int LP_pos = -1;
      // (b) get the closest data point after or before a timestamp
      if (leftEndIncluded > chunkSuit4CPV.statistics.getStartTime()) {
        FP_pos = chunkSuit4CPV.updateFPwithTheClosetPointEqualOrAfter(leftEndIncluded);
      }
      if (rightEndExcluded <= chunkSuit4CPV.statistics.getEndTime()) {
        // -1 is because right end is excluded end
        LP_pos = chunkSuit4CPV.updateLPwithTheClosetPointEqualOrBefore(rightEndExcluded - 1);
      }
      if (FP_pos != -1 && LP_pos != -1 && FP_pos > LP_pos) {
        // means the chunk has no point in this span, do nothing.
        continue;
      } else { // add this chunkSuit4CPV into currentChunkList or splitChunkList
        if (n == 0) {
          currentChunkList.add(chunkSuit4CPV);
        } else {
          int idx =
              (int)
                  Math.floor(
                      (chunkSuit4CPV.statistics.getStartTime() - startTime)
                          * 1.0
                          / interval); // global index TODO debug this
          splitChunkList.computeIfAbsent(idx, k -> new ArrayList<>());
          splitChunkList.get(idx).add(chunkSuit4CPV);
        }
      }
    }
  }

  public void updateBPTP(ChunkSuit4CPV chunkSuit4CPV) {
    long start = System.nanoTime();
    deleteCursor = 0; // TODO DEBUG
    Statistics statistics = null;
    switch (dataType) {
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
    // [startPos,endPos] definitely for curStartTime interval, thanks to split4CPV
    int count = 0; // update here, not in statistics
    for (int pos = chunkSuit4CPV.startPos; pos <= chunkSuit4CPV.endPos; pos++) {
      //      IOMonitor.incPointsTravered();
      IOMonitor2.DCP_D_traversedPointNum++;
      long timestamp = timeBuffer.getLong(pos * 8);
      switch (dataType) {
        case INT64:
          long aLong = valueBuffer.getLong(timeBufferLength + pos * 8);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
            // update statistics of chunkMetadata1
            statistics.updateStats(aLong, timestamp); // TODO DEBUG
            count++;
            // ATTENTION: do not use update() interface which will also update StepRegress!
            // only updateStats, actually only need to update BP and TP
          }
          break;
        case FLOAT:
          float aFloat = valueBuffer.getFloat(timeBufferLength + pos * 8);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
            // update statistics of chunkMetadata1
            statistics.updateStats(aFloat, timestamp);
            count++;
            // ATTENTION: do not use update() interface which will also update StepRegress!
            // only updateStats, actually only need to update BP and TP
          }
          break;
        case DOUBLE:
          double aDouble = valueBuffer.getDouble(timeBufferLength + pos * 8);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
            // update statistics of chunkMetadata1
            statistics.updateStats(aDouble, timestamp);
            count++;
            // ATTENTION: do not use update() interface which will also update StepRegress!
            // only updateStats, actually only need to update BP and TP
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    if (count > 0) {
      chunkSuit4CPV.statistics.setMinInfo(statistics.getMinInfo());
      chunkSuit4CPV.statistics.setMaxInfo(statistics.getMaxInfo());
    } else {
      chunkSuit4CPV.statistics.setCount(0); // otherwise count won't be zero
    }
    IOMonitor2.addMeasure(Operation.SEARCH_ARRAY_c_genBPTP, System.nanoTime() - start);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    long start = System.nanoTime();
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);

    while (timeDecoder.hasNext(timeBuffer)) {
      //      IOMonitor.incPointsTravered();
      IOMonitor2.DCP_D_traversedPointNum++;
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
    IOMonitor2.addMeasure(
        Operation.DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS, System.nanoTime() - start);
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

  public static boolean isDeleted(long timestamp, List<TimeRange> deleteIntervalList) {
    int deleteCursor = 0;
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
