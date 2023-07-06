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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.ValueIndex;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.Operation;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.ValuePoint;
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

  public void updateTP_withValueIndex(ChunkSuit4CPV chunkSuit4CPV) {
    long start = System.nanoTime();
    if (TSFileDescriptor.getInstance().getConfig().isUseValueIndex()) {
      // NOTE: get valueIndex from chunkSuit4CPV.getChunkMetadata().getStatistics(), not
      // chunkSuit4CPV.getStatistics()!
      ValueIndex valueIndex = chunkSuit4CPV.getChunkMetadata().getStatistics().valueIndex;

      // step 1: find threshold
      boolean isFound = false;
      double foundValue = 0;
      // iterate SDT points from value big to small to find the first point not deleted
      for (int n = valueIndex.sortedModelPoints.size() - 1; n >= 0; n--) { // NOTE from big to small
        IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
        ValuePoint valuePoint = valueIndex.sortedModelPoints.get(n);
        int idx = valuePoint.index; // index starting from 1
        int pos = idx - 1; // pos starting from 0
        long time = timeBuffer.getLong(pos * 8);
        // check if deleted
        if ((pos >= chunkSuit4CPV.startPos)
            && (pos <= chunkSuit4CPV.endPos)
            && !isDeleted_NoHistoryDeleteCursor(time)) {
          //  startPos&endPos conveys the virtual deletes of the current M4 time span
          isFound = true;
          foundValue = valuePoint.value;
          break;
        }
      }
      if (!isFound) { // unfortunately all sdt points are deleted
        // this includes the case that all points including model points are deleted,
        // which is handled by updateBPTP
        updateBPTP(chunkSuit4CPV); // then fall back to baseline method
        return;
      }
      double threshold_LB = foundValue - valueIndex.errorBound; // near max LB

      // step 2: calculate pruned intervals for TP: UB<threshold=near max LB
      // increment global chunkSuit4CPV.modelPointsCursor
      int idx2;
      // note that the first and last points of a chunk are stored in model points
      // there must exist idx2-1 >= startPos, otherwise this chunk won't be processed for the
      // current
      // time span
      while ((idx2 = valueIndex.modelPointIdx_list.get(chunkSuit4CPV.modelPointsCursor)) - 1
          < chunkSuit4CPV.startPos) {
        // -1 because idx starting from 1 while pos starting from 0
        chunkSuit4CPV.modelPointsCursor++;
        // pointing to the right end of the first model segment that passes the left endpoint of the
        // current time span
      }
      // increment local cursor starting from chunkSuit4CPV.modelPointsCursor for iterating model
      // segments for the current time span
      // do not increment modelPointsCursor because the model segments for this time span may be
      // iterated multiple times
      int localCursor =
          chunkSuit4CPV.modelPointsCursor; // pointing to the right end of the model segment
      List<Integer> prune_intervals_start = new ArrayList<>();
      List<Integer> prune_intervals_end = new ArrayList<>();
      int interval_start = -1;
      int interval_end = -1;
      int idx1;
      // there must exist idx1-1 <= endPos, otherwise this chunk won't be processed for the current
      // time span
      while (localCursor < valueIndex.modelPointIdx_list.size()
          && (idx1 = valueIndex.modelPointIdx_list.get(localCursor - 1)) - 1
              <= chunkSuit4CPV.endPos) {
        IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
        idx2 = valueIndex.modelPointIdx_list.get(localCursor);
        double v1_UB = valueIndex.modelPointVal_list.get(localCursor - 1) + valueIndex.errorBound;
        double v2_UB = valueIndex.modelPointVal_list.get(localCursor) + valueIndex.errorBound;
        if (v1_UB < threshold_LB && v2_UB < threshold_LB) {
          if (interval_start < 0) {
            interval_start = idx1;
          }
          interval_end = idx2; // continuous
        } else if (v1_UB < threshold_LB && v2_UB >= threshold_LB) {
          if (interval_start < 0) {
            interval_start = idx1;
          }
          prune_intervals_start.add(interval_start);
          prune_intervals_end.add(
              (int) Math.floor((threshold_LB - v1_UB) * (idx2 - idx1) / (v2_UB - v1_UB) + idx1));
          interval_start = -1; // discontinuous
        } else if (v1_UB >= threshold_LB && v2_UB < threshold_LB) {
          interval_start =
              (int) Math.ceil((threshold_LB - v1_UB) * (idx2 - idx1) / (v2_UB - v1_UB) + idx1);
          interval_end = idx2; // continuous
        }
        localCursor++;
      }
      if (interval_start > 0) {
        prune_intervals_start.add(interval_start);
        prune_intervals_end.add(interval_end);
      }

      // step 3: calculate unpruned intervals
      // deal with time span deletes -> update search_startPos and search_endPos
      // note idx starting from 1, pos starting from 0
      int search_startPos = chunkSuit4CPV.startPos;
      int search_endPos = chunkSuit4CPV.endPos;
      if (prune_intervals_start.size() > 0) {
        // deal with time span left virtual delete -> update search_startPos
        int prune_idx1 = prune_intervals_start.get(0);
        if (prune_idx1 - 1 <= chunkSuit4CPV.startPos) {
          // +1 for included, -1 for starting from 0
          search_startPos = Math.max(search_startPos, prune_intervals_end.get(0) + 1 - 1);
          prune_intervals_start.remove(0);
          prune_intervals_end.remove(0);
        }
      }
      if (prune_intervals_start.size() > 0) {
        // deal with time span right virtual delete -> update search_endPos
        int prune_idx2 = prune_intervals_end.get(prune_intervals_end.size() - 1);
        if (prune_idx2 - 1 >= search_endPos) {
          // -1 for included, -1 for starting from 0
          search_endPos =
              Math.min(
                  search_endPos,
                  prune_intervals_start.get(prune_intervals_start.size() - 1) - 1 - 1);
          prune_intervals_start.remove(prune_intervals_start.size() - 1);
          prune_intervals_end.remove(prune_intervals_end.size() - 1);
        }
      }
      // add search_endPos+1 to the end of prune_intervals_start
      // turning into search_intervals_end (excluded endpoints)
      // note that idx&prune_interval&search_interval starting from 1, pos starting from 0
      prune_intervals_start.add(search_endPos + 1 + 1); // +1 for starting from 1, +1 for excluded
      // add search_startPos-1 to the start of prune_intervals_end
      // turning into search_intervals_start (excluded endpoints)
      prune_intervals_end.add(
          0, search_startPos + 1 - 1); // +1 for starting from 1, +1 for excluded

      // step 4: search unpruned intervals
      // also deal with normal delete intervals
      if (dataType == TSDataType.DOUBLE) {
        double candidateTPvalue = -1;
        long candidateTPtime = -1;
        // note that idx&prune_interval&search_interval starting from 1, pos starting from 0
        for (int i = 0; i < prune_intervals_start.size(); i++) {
          int search_interval_start = prune_intervals_end.get(i) + 1; // included
          int search_interval_end = prune_intervals_start.get(i) - 1; // included
          for (int j = search_interval_start;
              j <= search_interval_end;
              j++) { // idx starting from 1
            IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
            double v = valueBuffer.getDouble(timeBufferLength + (j - 1) * 8); // pos starting from 0
            long t = timeBuffer.getLong((j - 1) * 8); // pos starting from 0
            if (v > candidateTPvalue && !isDeleted_NoHistoryDeleteCursor(t)) {
              candidateTPvalue = v;
              candidateTPtime = t;
            }
          }
        }
        chunkSuit4CPV.statistics.setMaxInfo(new MinMaxInfo(candidateTPvalue, candidateTPtime));
      } else if (dataType == TSDataType.INT64) {
        long candidateTPvalue = -1; // NOTE for TP
        long candidateTPtime = -1;
        for (int i = 0; i < prune_intervals_start.size(); i++) {
          int search_interval_start = prune_intervals_end.get(i) + 1; // included
          int search_interval_end = prune_intervals_start.get(i) - 1; // included
          for (int j = search_interval_start; j <= search_interval_end; j++) { // starting from 1
            IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
            long v = valueBuffer.getLong(timeBufferLength + (j - 1) * 8);
            long t = timeBuffer.getLong((j - 1) * 8);
            if (v > candidateTPvalue && !isDeleted_NoHistoryDeleteCursor(t)) {
              candidateTPvalue = v;
              candidateTPtime = t;
            }
          }
        }
        chunkSuit4CPV.statistics.setMaxInfo(new MinMaxInfo(candidateTPvalue, candidateTPtime));
      } else {
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    } else {
      updateBPTP(chunkSuit4CPV);
    }
    IOMonitor2.addMeasure(Operation.SEARCH_ARRAY_c_genBPTP, System.nanoTime() - start);
  }

  public void updateBP_withValueIndex(ChunkSuit4CPV chunkSuit4CPV) {
    long start = System.nanoTime();
    if (TSFileDescriptor.getInstance().getConfig().isUseValueIndex()) {
      // NOTE: get valueIndex from chunkSuit4CPV.getChunkMetadata().getStatistics(), not
      // chunkSuit4CPV.getStatistics()!
      ValueIndex valueIndex = chunkSuit4CPV.getChunkMetadata().getStatistics().valueIndex;

      // step 1: find threshold
      boolean isFound = false;
      double foundValue = 0;
      // iterate SDT points from value small to big to find the first point not deleted
      for (int n = 0; n < valueIndex.sortedModelPoints.size(); n++) { // NOTE from small to big
        IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
        ValuePoint valuePoint = valueIndex.sortedModelPoints.get(n);
        int idx = valuePoint.index; // index starting from 1
        int pos = idx - 1; // pos starting from 0
        long time = timeBuffer.getLong(pos * 8);
        // check if deleted
        if ((pos >= chunkSuit4CPV.startPos)
            && (pos <= chunkSuit4CPV.endPos)
            && !isDeleted_NoHistoryDeleteCursor(time)) {
          //  startPos&endPos conveys the virtual deletes of the current M4 time span
          isFound = true;
          foundValue = valuePoint.value;
          break;
        }
      }
      if (!isFound) { // unfortunately all sdt points are deleted
        // this includes the case that all points including model points are deleted,
        // which is handled by updateBPTP
        updateBPTP(chunkSuit4CPV); // then fall back to baseline method
        return;
      }
      double threshold_UB = foundValue + valueIndex.errorBound; // near min UB

      // step 2: calculate pruned intervals for BP: LB>threshold=near min UB
      // increment global chunkSuit4CPV.modelPointsCursor
      int idx2;
      // note that the first and last points of a chunk are stored in model points
      // there must exist idx2-1 >= startPos, otherwise this chunk won't be processed for the
      // current
      // time span
      while ((idx2 = valueIndex.modelPointIdx_list.get(chunkSuit4CPV.modelPointsCursor)) - 1
          < chunkSuit4CPV.startPos) {
        IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
        // -1 because idx starting from 1 while pos starting from 0
        chunkSuit4CPV.modelPointsCursor++;
        // pointing to the right end of the first model segment that passes the left endpoint of the
        // current time span
      }
      // increment local cursor starting from chunkSuit4CPV.modelPointsCursor for iterating model
      // segments for the current time span
      // do not increment modelPointsCursor because the model segments for this time span may be
      // iterated multiple times
      int localCursor =
          chunkSuit4CPV.modelPointsCursor; // pointing to the right end of the model segment
      List<Integer> prune_intervals_start = new ArrayList<>();
      List<Integer> prune_intervals_end = new ArrayList<>();
      int interval_start = -1;
      int interval_end = -1;
      int idx1;
      // there must exist idx1-1 <= endPos, otherwise this chunk won't be processed for the current
      // time span
      while (localCursor < valueIndex.modelPointIdx_list.size()
          && (idx1 = valueIndex.modelPointIdx_list.get(localCursor - 1)) - 1
              <= chunkSuit4CPV.endPos) {
        IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
        idx2 = valueIndex.modelPointIdx_list.get(localCursor);
        double v1_LB = valueIndex.modelPointVal_list.get(localCursor - 1) - valueIndex.errorBound;
        double v2_LB = valueIndex.modelPointVal_list.get(localCursor) - valueIndex.errorBound;
        if (v1_LB > threshold_UB && v2_LB > threshold_UB) {
          if (interval_start < 0) {
            interval_start = idx1;
          }
          interval_end = idx2; // continuous
        } else if (v1_LB > threshold_UB && v2_LB <= threshold_UB) {
          if (interval_start < 0) {
            interval_start = idx1;
          }
          prune_intervals_start.add(interval_start);
          prune_intervals_end.add(
              (int) Math.floor((threshold_UB - v1_LB) * (idx2 - idx1) / (v2_LB - v1_LB) + idx1));
          interval_start = -1; // discontinuous
        } else if (v1_LB <= threshold_UB && v2_LB > threshold_UB) {
          interval_start =
              (int) Math.ceil((threshold_UB - v1_LB) * (idx2 - idx1) / (v2_LB - v1_LB) + idx1);
          interval_end = idx2; // continuous
        }
        localCursor++;
      }
      if (interval_start > 0) {
        prune_intervals_start.add(interval_start);
        prune_intervals_end.add(interval_end);
      }

      // step 3: calculate unpruned intervals
      // deal with time span deletes -> update search_startPos and search_endPos
      // note idx starting from 1, pos starting from 0
      int search_startPos = chunkSuit4CPV.startPos;
      int search_endPos = chunkSuit4CPV.endPos;
      if (prune_intervals_start.size() > 0) {
        // deal with time span left virtual delete -> update search_startPos
        int prune_idx1 = prune_intervals_start.get(0);
        if (prune_idx1 - 1 <= chunkSuit4CPV.startPos) {
          // +1 for included, -1 for starting from 0
          search_startPos = Math.max(search_startPos, prune_intervals_end.get(0) + 1 - 1);
          prune_intervals_start.remove(0);
          prune_intervals_end.remove(0);
        }
      }
      if (prune_intervals_start.size() > 0) {
        // deal with time span right virtual delete -> update search_endPos
        int prune_idx2 = prune_intervals_end.get(prune_intervals_end.size() - 1);
        if (prune_idx2 - 1 >= search_endPos) {
          // -1 for included, -1 for starting from 0
          search_endPos =
              Math.min(
                  search_endPos,
                  prune_intervals_start.get(prune_intervals_start.size() - 1) - 1 - 1);
          prune_intervals_start.remove(prune_intervals_start.size() - 1);
          prune_intervals_end.remove(prune_intervals_end.size() - 1);
        }
      }
      // add search_endPos+1 to the end of prune_intervals_start
      // turning into search_intervals_end (excluded endpoints)
      // note that idx&prune_interval&search_interval starting from 1, pos starting from 0
      prune_intervals_start.add(search_endPos + 1 + 1); // +1 for starting from 1, +1 for excluded
      // add search_startPos-1 to the start of prune_intervals_end
      // turning into search_intervals_start (excluded endpoints)
      prune_intervals_end.add(
          0, search_startPos + 1 - 1); // +1 for starting from 1, +1 for excluded

      // step 4: search unpruned intervals
      // also deal with normal delete intervals
      if (dataType == TSDataType.DOUBLE) {
        double candidateBPvalue = Double.MAX_VALUE; // NOTE for BP
        long candidateBPtime = -1;
        // note that idx&prune_interval&search_interval starting from 1, pos starting from 0
        for (int i = 0; i < prune_intervals_start.size(); i++) {
          int search_interval_start = prune_intervals_end.get(i) + 1; // included
          int search_interval_end = prune_intervals_start.get(i) - 1; // included
          for (int j = search_interval_start;
              j <= search_interval_end;
              j++) { // idx starting from 1
            IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
            double v = valueBuffer.getDouble(timeBufferLength + (j - 1) * 8); // pos starting from 0
            long t = timeBuffer.getLong((j - 1) * 8); // pos starting from 0
            if (v < candidateBPvalue && !isDeleted_NoHistoryDeleteCursor(t)) {
              candidateBPvalue = v;
              candidateBPtime = t;
            }
          }
        }
        chunkSuit4CPV.statistics.setMinInfo(new MinMaxInfo(candidateBPvalue, candidateBPtime));
      } else if (dataType == TSDataType.INT64) {
        long candidateBPvalue = Long.MAX_VALUE; // NOTE for BP
        long candidateBPtime = -1;
        for (int i = 0; i < prune_intervals_start.size(); i++) {
          int search_interval_start = prune_intervals_end.get(i) + 1; // included
          int search_interval_end = prune_intervals_start.get(i) - 1; // included
          for (int j = search_interval_start; j <= search_interval_end; j++) { // starting from 1
            IOMonitor2.DCP_D_valueIndex_traversedPointNum++; // TODO
            long v = valueBuffer.getLong(timeBufferLength + (j - 1) * 8);
            long t = timeBuffer.getLong((j - 1) * 8);
            if (v < candidateBPvalue && !isDeleted_NoHistoryDeleteCursor(t)) {
              candidateBPvalue = v;
              candidateBPtime = t;
            }
          }
        }
        chunkSuit4CPV.statistics.setMinInfo(new MinMaxInfo(candidateBPvalue, candidateBPtime));
      } else {
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    } else {
      updateBPTP(chunkSuit4CPV);
    }
    IOMonitor2.addMeasure(Operation.SEARCH_ARRAY_c_genBPTP, System.nanoTime() - start);
  }

  public void updateBPTP(ChunkSuit4CPV chunkSuit4CPV) {
    long start = System.nanoTime();
    deleteCursor = 0; // TODO DEBUG
    Statistics statistics = null;
    switch (dataType) {
      case INT64:
        statistics = new LongStatistics();
        break;
        //      case FLOAT:
        //        statistics = new FloatStatistics();
        //        break;
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
      IOMonitor2.DCP_D_valueIndex_traversedPointNum++;
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
          //        case FLOAT:
          //          float aFloat = valueBuffer.getFloat(timeBufferLength + pos * 8);
          //          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp,
          // aFloat))) {
          //            // update statistics of chunkMetadata1
          //            statistics.updateStats(aFloat, timestamp);
          //            count++;
          //            // ATTENTION: do not use update() interface which will also update
          // StepRegress!
          //            // only updateStats, actually only need to update BP and TP
          //          }
          //          break;
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
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
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

  protected boolean isDeleted_NoHistoryDeleteCursor(long timestamp) {
    deleteCursor = 0;
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
