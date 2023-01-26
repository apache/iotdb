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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

public class PageReader implements IPageReader {

  private PageHeader pageHeader;

  protected TSDataType dataType;

  /**
   * decoder for value column
   */
  protected Decoder valueDecoder;

  /**
   * decoder for time column
   */
  protected Decoder timeDecoder;

  /**
   * time column in memory
   */
  public ByteBuffer timeBuffer;

  /**
   * value column in memory
   */
  public ByteBuffer valueBuffer;

  public int timeBufferLength;

  protected Filter filter;

  /**
   * A list of deleted intervals.
   */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public PageReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder,
      Decoder timeDecoder, Filter filter) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReader(PageHeader pageHeader, ByteBuffer pageData, TSDataType dataType,
      Decoder valueDecoder, Decoder timeDecoder, Filter filter) {
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

  /**
   * the chunk partially overlaps in time with the current M4 interval Ii
   */
  public void split4CPV(long startTime, long endTime, long interval, long curStartTime,
      List<ChunkSuit4CPV> currentChunkList, Map<Integer, List<ChunkSuit4CPV>> splitChunkList,
      ChunkMetadata chunkMetadata)
      throws IOException { // note: [startTime,endTime), [curStartTime,curEndTime)
//    int curIdx = (int) Math.floor((curStartTime - startTime) * 1.0 / interval); // global index
    int numberOfSpans = (int) Math.floor(
        (Math.min(chunkMetadata.getEndTime(), endTime - 1)  // endTime is excluded so -1
            - curStartTime) * 1.0 / interval) + 1;
    for (int n = 0; n < numberOfSpans; n++) {
      long leftEndIncluded = curStartTime + n * interval;
      long rightEndExcluded = curStartTime + (n + 1) * interval;
      ChunkSuit4CPV chunkSuit4CPV = new ChunkSuit4CPV(chunkMetadata, this, true);
      // TODO update FP,LP with the help of stepRegress index. BP/TP not update here.
//      MinMaxInfo FP = null; // new FP
//      MinMaxInfo LP = null; // new LP
      int FP_pos = -1;
      int LP_pos = -1;
      if (leftEndIncluded > chunkSuit4CPV.statistics.getStartTime()) {
//        FP = chunkSuit4CPV.findTheClosetPointEqualOrAfter(leftEndIncluded);
//        chunkSuit4CPV.updateFP(FP);
        FP_pos = chunkSuit4CPV.updateFPwithTheClosetPointEqualOrAfter(leftEndIncluded);
      }
      if (rightEndExcluded <= chunkSuit4CPV.statistics.getEndTime()) {
        // -1 is because right end is excluded end
        LP_pos = chunkSuit4CPV.updateLPwithTheClosetPointEqualOrBefore(rightEndExcluded - 1);
//        chunkSuit4CPV.updateLP(LP);
      }
      if (FP_pos != -1 && LP_pos != -1 && FP_pos > LP_pos) {
        // the chunk has no point in this span, do nothing
        continue;
      } else { // add this chunkSuit4CPV into currentChunkList or splitChunkList
        if (n == 0) {
          currentChunkList.add(chunkSuit4CPV);
        } else {
          int idx = (int) Math.floor((chunkSuit4CPV.statistics.getStartTime() - startTime) * 1.0
              / interval); // global index TODO debug this
          splitChunkList.computeIfAbsent(idx, k -> new ArrayList<>());
          splitChunkList.get(idx).add(chunkSuit4CPV);
        }
      }
    }
  }

//    Map<Integer, BatchData> splitBatchDataMap = new HashMap<>();
//    Map<Integer, ChunkMetadata> splitChunkMetadataMap = new HashMap<>();
//    while (timeDecoder.hasNext(timeBuffer)) {
//      long timestamp = timeDecoder.readLong(timeBuffer);
//      // prepare corresponding batchData
//      if (timestamp < curStartTime) {
//        switch (dataType) {
//          case INT32:
//            valueDecoder.readInt(valueBuffer);
//            break;
//          case INT64:
//            valueDecoder.readLong(valueBuffer);
//            break;
//          case FLOAT:
//            valueDecoder.readFloat(valueBuffer);
//            break;
//          case DOUBLE:
//            valueDecoder.readDouble(valueBuffer);
//            break;
//          default:
//            throw new UnSupportedDataTypeException(String.valueOf(dataType));
//        }
//        continue;
//      }
//      if (timestamp >= endTime) {
//        break;
//      }
//      int idx = (int) Math.floor((timestamp - startTime) * 1.0 / interval);
//      if (!splitBatchDataMap.containsKey(idx)) {
//        // create batchData
//        BatchData batch1 = BatchDataFactory.createBatchData(dataType, true, false);
//        splitBatchDataMap.put(idx, batch1);
//        Statistics statistics = null;
//        switch (dataType) {
//          case INT32:
//            statistics = new IntegerStatistics();
//            break;
//          case INT64:
//            statistics = new LongStatistics();
//            break;
//          case FLOAT:
//            statistics = new FloatStatistics();
//            break;
//          case DOUBLE:
//            statistics = new DoubleStatistics();
//            break;
//          default:
//            break;
//        }
//        // create chunkMetaData
//        ChunkMetadata chunkMetadata1 =
//            new ChunkMetadata(
//                chunkMetadata.getMeasurementUid(),
//                chunkMetadata.getDataType(),
//                chunkMetadata.getOffsetOfChunkHeader(),
//                statistics);
//        chunkMetadata1.setVersion(chunkMetadata.getVersion()); // don't miss this
//
//        //        // important, used later for candidate point verification
//        //        // (1) candidate point itself whether is in the deleted interval
//        //        // (2) candidate point whether is overlapped by a chunk with a larger version
//        // number and
//        //        // the chunk does not have a deleted interval overlapping this candidate point
//        //        chunkMetadata1.setDeleteIntervalList(chunkMetadata.getDeleteIntervalList());
//        //        // not use current Ii to modify deletedIntervalList any more
//
//        splitChunkMetadataMap.put(idx, chunkMetadata1);
//      }
//      BatchData batchData1 = splitBatchDataMap.get(idx);
//      ChunkMetadata chunkMetadata1 = splitChunkMetadataMap.get(idx);
//      switch (dataType) {
//        case INT32:
//          int anInt = valueDecoder.readInt(valueBuffer);
//          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
//            // update batchData1
//            batchData1.putInt(timestamp, anInt);
//            // update statistics of chunkMetadata1
//            chunkMetadata1.getStatistics().update(timestamp, anInt);
//          }
//          break;
//        case INT64:
//          long aLong = valueDecoder.readLong(valueBuffer);
//          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
//            // update batchData1
//            batchData1.putLong(timestamp, aLong);
//            // update statistics of chunkMetadata1
//            chunkMetadata1.getStatistics().update(timestamp, aLong);
//          }
//          break;
//        case FLOAT:
//          float aFloat = valueDecoder.readFloat(valueBuffer);
//          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
//            // update batchData1
//            batchData1.putFloat(timestamp, aFloat);
//            // update statistics of chunkMetadata1
//            chunkMetadata1.getStatistics().update(timestamp, aFloat);
//          }
//          break;
//        case DOUBLE:
//          double aDouble = valueDecoder.readDouble(valueBuffer);
//          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
//            // update batchData1
//            batchData1.putDouble(timestamp, aDouble);
//            // update statistics of chunkMetadata1
//            chunkMetadata1.getStatistics().update(timestamp, aDouble);
//          }
//          break;
//        default:
//          throw new UnSupportedDataTypeException(String.valueOf(dataType));
//      }
//    }
//
//    int curIdx = (int) Math.floor((curStartTime - startTime) * 1.0 / interval);
//    for (Integer i : splitBatchDataMap.keySet()) {
//      if (!splitBatchDataMap.get(i).isEmpty()) {
//        if (i == curIdx) {
//          currentChunkList.add(
//              new ChunkSuit4CPV(splitChunkMetadataMap.get(i), splitBatchDataMap.get(i).flip()));
//        } else {
//          splitChunkList.computeIfAbsent(i, k -> new ArrayList<>());
//          splitChunkList
//              .get(i)
//              .add(
//                  new ChunkSuit4CPV(splitChunkMetadataMap.get(i), splitBatchDataMap.get(i).flip()));
//        }
//      }
//    }

  public void updateBPTP(ChunkSuit4CPV chunkSuit4CPV) throws IOException {
    deleteCursor = 0;
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
    for (int pos = chunkSuit4CPV.startPos; pos <= chunkSuit4CPV.endPos; pos++) {
      long timestamp = timeBuffer.getLong(pos * 8);
      switch (dataType) {
        case INT64:
          long aLong = valueBuffer.getLong(timeBufferLength + pos * 8);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
            // update statistics of chunkMetadata1
            statistics.updateStats(aLong, timestamp); //TODO DEBUG
            // ATTENTION: do not use update() interface which will also update StepRegress!
            // only updateStats, actually only need to update BP and TP
          }
          break;
        case FLOAT:
          float aFloat = valueBuffer.getFloat(timeBufferLength + pos * 8);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
            // update statistics of chunkMetadata1
            statistics.updateStats(aFloat, timestamp);
            // ATTENTION: do not use update() interface which will also update StepRegress!
            // only updateStats, actually only need to update BP and TP
          }
          break;
        case DOUBLE:
          double aDouble = valueBuffer.getDouble(timeBufferLength + pos * 8);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
            // update statistics of chunkMetadata1
            statistics.updateStats(aDouble, timestamp);
            // ATTENTION: do not use update() interface which will also update StepRegress!
            // only updateStats, actually only need to update BP and TP
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    chunkSuit4CPV.statistics.setMinInfo(statistics.getMinInfo());
    chunkSuit4CPV.statistics.setMaxInfo(statistics.getMaxInfo());
  }

  /**
   * chunk里点时间戳从小到大递增， 所以遍历直到点的时间戳大于或等于candidateTimestamp即可结束
   *
   * @return true if the point whose time equals candidateTimestamp exists, false if not
   */
  public boolean partialScan(long candidateTimestamp) throws IOException {
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      if (timestamp > candidateTimestamp) {
        return false;
      }
      if (timestamp == candidateTimestamp) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return the returned BatchData may be empty, but never be null
   */
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
