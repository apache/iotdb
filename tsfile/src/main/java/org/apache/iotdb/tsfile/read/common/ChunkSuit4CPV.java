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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.StepRegress;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.Operation;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;

public class ChunkSuit4CPV {

  private ChunkMetadata chunkMetadata; // fixed info, including version, dataType, stepRegress

  public Statistics statistics; // dynamically updated, includes FP/LP/BP/TP info

  // [startPos,endPos] definitely for curStartTime interval, thanks to split4CPV
  public int startPos = -1; // the first point position, starting from 0
  public int endPos = -1; // the last point position, starting from 0

  //  public long startTime; // statistics in chunkMetadata is not deepCopied, so store update here
  //
  //  public long endTime;

  //  public int firstValueInt;
  //  public long firstValueLong;
  //  public float firstValueFloat;
  //  public double firstValueDouble;
  //
  //  public int lastValueInt;
  //  public long lastValueLong;
  //  public float lastValueFloat;
  //  public double lastValueDouble;
  //
  //  public MinMaxInfo<Integer> minInfoInt;
  //  public MinMaxInfo<Long> minInfoLong;
  //  public MinMaxInfo<Float> minInfoFloat;
  //  public MinMaxInfo<Double> minInfoDouble;
  //
  //  public MinMaxInfo<Integer> maxInfoInt;
  //  public MinMaxInfo<Long> maxInfoLong;
  //  public MinMaxInfo<Float> maxInfoFloat;
  //  public MinMaxInfo<Double> maxInfoDouble;

  //  private BatchData batchData; // deprecated

  // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
  //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
  //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
  //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
  //  WHICH WILL INTRODUCE BUGS!
  private PageReader pageReader; // bears plain timeBuffer and valueBuffer
  // pageReader does not refer to the same deleteInterval as those in chunkMetadata
  // after chunkMetadata executes insertIntoSortedDeletions

  //  private List<Long> mergeVersionList = new ArrayList<>();
  //  private List<Long> mergeOffsetList = new ArrayList<>();
  private boolean isLazyLoad = false;

  public ChunkSuit4CPV(ChunkMetadata chunkMetadata) {
    this(chunkMetadata, false);
  }

  public ChunkSuit4CPV(ChunkMetadata chunkMetadata, boolean deepCopy) {
    this.chunkMetadata = chunkMetadata;
    // deep copy initialize
    if (deepCopy) {
      deepCopyInitialize(chunkMetadata.getStatistics(), chunkMetadata.getDataType());
    } else {
      statistics = chunkMetadata.getStatistics();
    }
    this.startPos = 0;
    this.endPos = chunkMetadata.getStatistics().getCount() - 1;
  }

  //  public ChunkSuit4CPV(ChunkMetadata chunkMetadata, BatchData batchData) {
  //    this.chunkMetadata = chunkMetadata;
  //    this.batchData = batchData;
  //    // deep copy initialize
  //    deepCopyInitialize(chunkMetadata.getStatistics(), chunkMetadata.getDataType());
  //  }

  public ChunkSuit4CPV(ChunkMetadata chunkMetadata, PageReader pageReader, boolean deepCopy) {
    this.chunkMetadata = chunkMetadata;
    this.pageReader = pageReader;
    // deep copy initialize
    if (deepCopy) {
      deepCopyInitialize(chunkMetadata.getStatistics(), chunkMetadata.getDataType());
    } else {
      statistics = chunkMetadata.getStatistics();
    }
    this.startPos = 0;
    this.endPos = chunkMetadata.getStatistics().getCount() - 1;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public void deepCopyInitialize(Statistics source, TSDataType type) {
    // deep copy initialize
    switch (type) {
      case INT32:
        statistics = new IntegerStatistics();
        ((IntegerStatistics) statistics)
            .initializeStats(
                (int) source.getMinInfo().val,
                source.getMinInfo().timestamp,
                (int) source.getMaxInfo().val,
                source.getMaxInfo().timestamp,
                (int) source.getFirstValue(),
                (int) source.getLastValue(),
                source.getSumLongValue());
        break;
      case INT64:
        statistics = new LongStatistics();
        ((LongStatistics) statistics)
            .initializeStats(
                (long) source.getMinInfo().val,
                source.getMinInfo().timestamp,
                (long) source.getMaxInfo().val,
                source.getMaxInfo().timestamp,
                (long) source.getFirstValue(),
                (long) source.getLastValue(),
                source.getSumDoubleValue());
        break;
      case FLOAT:
        statistics = new FloatStatistics();
        ((FloatStatistics) statistics)
            .initializeStats(
                (float) source.getMinInfo().val,
                source.getMinInfo().timestamp,
                (float) source.getMaxInfo().val,
                source.getMaxInfo().timestamp,
                (float) source.getFirstValue(),
                (float) source.getLastValue(),
                source.getSumDoubleValue());
        break;
      case DOUBLE:
        statistics = new DoubleStatistics();
        ((DoubleStatistics) statistics)
            .initializeStats(
                (double) source.getMinInfo().val,
                source.getMinInfo().timestamp,
                (double) source.getMaxInfo().val,
                source.getMaxInfo().timestamp,
                (double) source.getFirstValue(),
                (double) source.getLastValue(),
                source.getSumDoubleValue());
        break;
      default:
        break;
    }
    statistics.setStartTime(source.getStartTime());
    statistics.setEndTime(source.getEndTime());
    statistics.setCount(source.getCount());
  }

  public void setLazyLoad(boolean lazyLoad) {
    isLazyLoad = lazyLoad;
  }

  public boolean isLazyLoad() {
    return isLazyLoad;
  }

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  //  public BatchData getBatchData() {
  //    return batchData;
  //  }

  public PageReader getPageReader() {
    return pageReader;
  }

  //  public void setBatchData(BatchData batchData) {
  //    this.batchData = batchData;
  //  }

  public void setPageReader(PageReader pageReader) {
    this.pageReader = pageReader;
  }

  public void setChunkMetadata(ChunkMetadata chunkMetadata) {
    this.chunkMetadata = chunkMetadata;
  }

  //  public void addMergeVersionList(long version) {
  //    this.mergeVersionList.add(version);
  //  }
  //
  //  public void addMergeOffsetList(long offset) {
  //    this.mergeOffsetList.add(offset);
  //  }
  //
  //  public List<Long> getMergeVersionList() {
  //    return mergeVersionList;
  //  }
  //
  //  public List<Long> getMergeOffsetList() {
  //    return mergeOffsetList;
  //  }

  public long getVersion() {
    return this.getChunkMetadata().getVersion();
  }

  public long getOffset() {
    return this.getChunkMetadata().getOffsetOfChunkHeader();
  }

  /**
   * Find the point with the closet timestamp equal to or larger than the given timestamp in the
   * chunk.
   *
   * @param targetTimestamp must be within the chunk time range [startTime, endTime]
   * @return the position of the point, starting from 0
   */
  public int updateFPwithTheClosetPointEqualOrAfter(long targetTimestamp) throws IOException {
    long start = System.nanoTime();
    int estimatedPos;
    if (TSFileDescriptor.getInstance().getConfig().isUseChunkIndex()) {
      StepRegress stepRegress = chunkMetadata.getStatistics().getStepRegress();
      // infer position starts from 1, so minus 1 here
      estimatedPos = (int) Math.round(stepRegress.infer(targetTimestamp)) - 1;

      // search from estimatePos in the timeBuffer to find the closet timestamp equal to or larger
      // than the given timestamp
      if (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
        while (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
          estimatedPos++;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
      } else if (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
        while (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
          estimatedPos--;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
        if (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
          estimatedPos++;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        } // else equal
      } // else equal
      this.startPos = estimatedPos; // note this

      // since we have constrained that targetTimestamp must be within the chunk time range
      // [startTime, endTime],
      // we can definitely find such a point with the closet timestamp equal to or larger than the
      // given timestamp in the chunk.
      long timestamp = pageReader.timeBuffer.getLong(estimatedPos * 8);
      statistics.setStartTime(timestamp);
      switch (chunkMetadata.getDataType()) {
          // iotdb的int类型的plain编码用的是自制的不支持random access
          //      case INT32:
          //        return new MinMaxInfo(pageReader.valueBuffer.getInt(estimatedPos * 4),
          //            pageReader.timeBuffer.getLong(estimatedPos * 8));
        case INT64:
          long longVal =
              pageReader.valueBuffer.getLong(pageReader.timeBufferLength + estimatedPos * 8);
          ((LongStatistics) statistics).setFirstValue(longVal);
          break;
        case FLOAT:
          float floatVal =
              pageReader.valueBuffer.getFloat(pageReader.timeBufferLength + estimatedPos * 4);
          ((FloatStatistics) statistics).setFirstValue(floatVal);
          break;
        case DOUBLE:
          double doubleVal =
              pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + estimatedPos * 8);
          ((DoubleStatistics) statistics).setFirstValue(doubleVal);
          break;
        default:
          throw new IOException("Unsupported data type!");
      }
    } else {
      // search from estimatePos in the timeBuffer to find the closet timestamp equal to or larger
      // than the given timestamp
      estimatedPos = -1;
      pageReader.timeBuffer.position(0);
      pageReader.valueBuffer.position(pageReader.timeBufferLength);
      while (pageReader.timeBuffer.remaining() > 0) {
        estimatedPos++;
        //        IOMonitor.incPointsTravered();
        IOMonitor2.DCP_D_traversedPointNum++;
        long t = pageReader.timeBuffer.getLong();
        if (t >= targetTimestamp) {
          break;
        }
      }
      this.startPos = estimatedPos; // note this

      // since we have constrained that targetTimestamp must be within the chunk time range
      // [startTime, endTime],
      // we can definitely find such a point with the closet timestamp equal to or larger than the
      // given timestamp in the chunk.
      long timestamp = pageReader.timeBuffer.getLong(estimatedPos * 8);
      statistics.setStartTime(timestamp);
      switch (chunkMetadata.getDataType()) {
          // iotdb的int类型的plain编码用的是自制的不支持random access
          //      case INT32:
          //        return new MinMaxInfo(pageReader.valueBuffer.getInt(estimatedPos * 4),
          //            pageReader.timeBuffer.getLong(estimatedPos * 8));
        case INT64:
          long longVal =
              pageReader.valueBuffer.getLong(pageReader.timeBufferLength + estimatedPos * 8);
          ((LongStatistics) statistics).setFirstValue(longVal);
          break;
        case FLOAT:
          float floatVal =
              pageReader.valueBuffer.getFloat(pageReader.timeBufferLength + estimatedPos * 4);
          ((FloatStatistics) statistics).setFirstValue(floatVal);
          break;
        case DOUBLE:
          double doubleVal =
              pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + estimatedPos * 8);
          ((DoubleStatistics) statistics).setFirstValue(doubleVal);
          break;
        default:
          throw new IOException("Unsupported data type!");
      }
      return estimatedPos;
    }
    IOMonitor2.addMeasure(Operation.SEARCH_ARRAY_b_genFP, System.nanoTime() - start);
    return estimatedPos;
  }

  /**
   * Find the point with the closet timestamp equal to or smaller than the given timestamp in the
   * chunk.
   *
   * @param targetTimestamp must be within the chunk time range [startTime, endTime]
   * @return the position of the point, starting from 0
   */
  public int updateLPwithTheClosetPointEqualOrBefore(long targetTimestamp) throws IOException {
    long start = System.nanoTime();
    int estimatedPos;
    if (TSFileDescriptor.getInstance().getConfig().isUseChunkIndex()) {
      StepRegress stepRegress = chunkMetadata.getStatistics().getStepRegress();
      // infer position starts from 1, so minus 1 here
      estimatedPos = (int) Math.round(stepRegress.infer(targetTimestamp)) - 1;

      // search from estimatePos in the timeBuffer to find the closet timestamp equal to or smaller
      // than the given timestamp
      if (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
        while (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
          estimatedPos--;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
      } else if (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
        while (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
          estimatedPos++;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
        if (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
          estimatedPos--;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        } // else equal
      } // else equal
      this.endPos = estimatedPos; // note this

      // since we have constrained that targetTimestamp must be within the chunk time range
      // [startTime, endTime],
      // we can definitely find such a point with the closet timestamp equal to or smaller than the
      // given timestamp in the chunk.
      long timestamp = pageReader.timeBuffer.getLong(estimatedPos * 8);
      statistics.setEndTime(timestamp);
      switch (chunkMetadata.getDataType()) {
          // iotdb的int类型的plain编码用的是自制的不支持random access
          //      case INT32:
          //        return new MinMaxInfo(pageReader.valueBuffer.getInt(estimatedPos * 4),
          //            pageReader.timeBuffer.getLong(estimatedPos * 8));
        case INT64:
          long longVal =
              pageReader.valueBuffer.getLong(pageReader.timeBufferLength + estimatedPos * 8);
          ((LongStatistics) statistics).setLastValue(longVal);
          break;
        case FLOAT:
          float floatVal =
              pageReader.valueBuffer.getFloat(pageReader.timeBufferLength + estimatedPos * 4);
          ((FloatStatistics) statistics).setLastValue(floatVal);
          break;
        case DOUBLE:
          double doubleVal =
              pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + estimatedPos * 8);
          ((DoubleStatistics) statistics).setLastValue(doubleVal);
          break;
        default:
          throw new IOException("Unsupported data type!");
      }
    } else {
      // to find the closet timestamp equal to or smaller
      // than the given timestamp
      estimatedPos = -1;
      pageReader.timeBuffer.position(0);
      pageReader.valueBuffer.position(pageReader.timeBufferLength);
      while (pageReader.timeBuffer.remaining() > 0) {
        estimatedPos++;
        //        IOMonitor.incPointsTravered();
        IOMonitor2.DCP_D_traversedPointNum++;
        long t = pageReader.timeBuffer.getLong();
        if (t >= targetTimestamp) {
          break;
        }
      }
      if (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
        //        IOMonitor.incPointsTravered();
        IOMonitor2.DCP_D_traversedPointNum++;
        estimatedPos--;
      } // else equals no need to minus 1

      this.endPos = estimatedPos; // note this

      // since we have constrained that targetTimestamp must be within the chunk time range
      // [startTime, endTime],
      // we can definitely find such a point with the closet timestamp equal to or smaller than the
      // given timestamp in the chunk.
      long timestamp = pageReader.timeBuffer.getLong(estimatedPos * 8);
      statistics.setEndTime(timestamp);
      switch (chunkMetadata.getDataType()) {
          // iotdb的int类型的plain编码用的是自制的不支持random access
          //      case INT32:
          //        return new MinMaxInfo(pageReader.valueBuffer.getInt(estimatedPos * 4),
          //            pageReader.timeBuffer.getLong(estimatedPos * 8));
        case INT64:
          long longVal =
              pageReader.valueBuffer.getLong(pageReader.timeBufferLength + estimatedPos * 8);
          ((LongStatistics) statistics).setLastValue(longVal);
          break;
        case FLOAT:
          float floatVal =
              pageReader.valueBuffer.getFloat(pageReader.timeBufferLength + estimatedPos * 4);
          ((FloatStatistics) statistics).setLastValue(floatVal);
          break;
        case DOUBLE:
          double doubleVal =
              pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + estimatedPos * 8);
          ((DoubleStatistics) statistics).setLastValue(doubleVal);
          break;
        default:
          throw new IOException("Unsupported data type!");
      }
    }
    IOMonitor2.addMeasure(Operation.SEARCH_ARRAY_b_genLP, System.nanoTime() - start);
    return estimatedPos;
  }

  /**
   * Check if there exists the point at the target timestamp in the chunk.
   *
   * @param targetTimestamp must be within the chunk time range [startTime, endTime]
   * @return true if exists; false not exist
   */
  public boolean checkIfExist(long targetTimestamp) throws IOException {
    long start = System.nanoTime();
    boolean exist;
    if (TSFileDescriptor.getInstance().getConfig().isUseChunkIndex()) {
      StepRegress stepRegress = chunkMetadata.getStatistics().getStepRegress();
      // infer position starts from 1, so minus 1 here
      // TODO debug buffer.get(index)
      int estimatedPos = (int) Math.round(stepRegress.infer(targetTimestamp)) - 1;

      // search from estimatePos in the timeBuffer to find the closet timestamp equal to or smaller
      // than the given timestamp
      if (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
        while (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
          estimatedPos--;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
      } else if (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
        while (pageReader.timeBuffer.getLong(estimatedPos * 8) < targetTimestamp) {
          estimatedPos++;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
        if (pageReader.timeBuffer.getLong(estimatedPos * 8) > targetTimestamp) {
          estimatedPos--;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        } // else equal
      } // else equal

      // since we have constrained that targetTimestamp must be within the chunk time range
      // [startTime, endTime],
      // estimatedPos will not be out of range.
      exist = pageReader.timeBuffer.getLong(estimatedPos * 8) == targetTimestamp;
    } else {
      // search from estimatePos in the timeBuffer to find the closet timestamp equal to or smaller
      // than the given timestamp
      int estimatedPos = -1;
      boolean flag = false;
      pageReader.timeBuffer.position(0);
      pageReader.valueBuffer.position(pageReader.timeBufferLength);
      while (pageReader.timeBuffer.remaining() > 0) {
        if (!flag) {
          estimatedPos++;
          //          IOMonitor.incPointsTravered();
          IOMonitor2.DCP_D_traversedPointNum++;
        }
        long t = pageReader.timeBuffer.getLong();
        if (t >= targetTimestamp) {
          flag = true;
        }
      }

      // since we have constrained that targetTimestamp must be within the chunk time range
      // [startTime, endTime],
      // estimatedPos will not be out of range.
      exist = pageReader.timeBuffer.getLong(estimatedPos * 8) == targetTimestamp;
    }
    IOMonitor2.addMeasure(Operation.SEARCH_ARRAY_a_verifBPTP, System.nanoTime() - start);
    return exist;
  }

  public void updateFP(MinMaxInfo point) {
    long timestamp = point.timestamp;
    Object val = point.val;
    switch (chunkMetadata.getDataType()) {
      case INT32:
        statistics.setStartTime(timestamp);
        ((IntegerStatistics) statistics).setFirstValue((int) val);
        break;
      case INT64:
        statistics.setStartTime(timestamp);
        ((LongStatistics) statistics).setFirstValue((long) val);
        break;
      case FLOAT:
        statistics.setStartTime(timestamp);
        ((FloatStatistics) statistics).setFirstValue((float) val);
        break;
      case DOUBLE:
        statistics.setStartTime(timestamp);
        ((DoubleStatistics) statistics).setFirstValue((double) val);
        break;
      default:
        break;
    }
  }

  public void updateLP(MinMaxInfo point) {
    long timestamp = point.timestamp;
    Object val = point.val;
    switch (chunkMetadata.getDataType()) {
      case INT32:
        statistics.setEndTime(timestamp);
        ((IntegerStatistics) statistics).setLastValue((int) val);
        break;
      case INT64:
        statistics.setEndTime(timestamp);
        ((LongStatistics) statistics).setLastValue((long) val);
        break;
      case FLOAT:
        statistics.setEndTime(timestamp);
        ((FloatStatistics) statistics).setLastValue((float) val);
        break;
      case DOUBLE:
        statistics.setEndTime(timestamp);
        ((DoubleStatistics) statistics).setLastValue((double) val);
        break;
      default:
        break;
    }
  }

  public void updateBP(MinMaxInfo point) {
    long timestamp = point.timestamp;
    Object val = point.val;
    switch (chunkMetadata.getDataType()) {
      case INT32:
        ((IntegerStatistics) statistics).setMinInfo(timestamp, (int) val);
        break;
      case INT64:
        ((LongStatistics) statistics).setMinInfo(timestamp, (long) val);
        break;
      case FLOAT:
        ((FloatStatistics) statistics).setMinInfo(timestamp, (float) val);
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics).setMinInfo(timestamp, (double) val);
        break;
      default:
        break;
    }
  }

  public void updateTP(MinMaxInfo point) {
    long timestamp = point.timestamp;
    Object val = point.val;
    switch (chunkMetadata.getDataType()) {
      case INT32:
        ((IntegerStatistics) statistics).setMaxInfo(timestamp, (int) val);
        break;
      case INT64:
        ((LongStatistics) statistics).setMaxInfo(timestamp, (long) val);
        break;
      case FLOAT:
        ((FloatStatistics) statistics).setMaxInfo(timestamp, (float) val);
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics).setMaxInfo(timestamp, (double) val);
        break;
      default:
        break;
    }
  }
}
