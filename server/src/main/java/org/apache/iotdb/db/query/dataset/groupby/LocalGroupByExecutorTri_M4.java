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

package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.query.simpiece.TimeSeries;
import org.apache.iotdb.db.query.simpiece.TimeSeriesReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4Tri;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class LocalGroupByExecutorTri_M4 implements GroupByExecutor {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  //  private static final Logger M4_CHUNK_METADATA = LoggerFactory.getLogger("M4_CHUNK_METADATA");

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();

  //  private List<ChunkSuit4Tri> currentChunkList;
  //  private final List<ChunkSuit4Tri> futureChunkList = new ArrayList<>();

  private Filter timeFilter;

  private final int N1;

  private TimeSeries timeSeries;

  public LocalGroupByExecutorTri_M4(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    //    long start = System.nanoTime();

    // get all data sources
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, this.timeFilter);

    // update filter by TTL
    this.timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesReader seriesReader =
        new SeriesReader(
            path,
            allSensors,
            // fix bug: here use the aggregation type as the series data type,
            // not using pageReader.getAllSatisfiedPageData is ok
            dataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);

    GroupByFilter groupByFilter = (GroupByFilter) timeFilter;
    long startTime = groupByFilter.getStartTime();
    long endTime = groupByFilter.getEndTime();
    long interval = groupByFilter.getInterval();
    N1 = (int) Math.floor((endTime * 1.0 - startTime) / interval); // 分桶数

    try {
      List<ChunkSuit4Tri> futureChunkList = new ArrayList<>();
      futureChunkList.addAll(seriesReader.getAllChunkMetadatas4Tri());
      // order futureChunkList by chunk startTime
      futureChunkList.sort(
          new Comparator<ChunkSuit4Tri>() {
            public int compare(ChunkSuit4Tri o1, ChunkSuit4Tri o2) {
              return ((Comparable) (o1.chunkMetadata.getStartTime()))
                  .compareTo(o2.chunkMetadata.getStartTime());
            }
          });

      timeSeries = TimeSeriesReader.getTimeSeriesFromTsFiles(futureChunkList, startTime, endTime);

      //      if (M4_CHUNK_METADATA.isDebugEnabled()) {
      //        if (timeFilter instanceof GroupByFilter) {
      //          M4_CHUNK_METADATA.debug(
      //              "M4_QUERY_PARAM,{},{},{}",
      //              ((GroupByFilter) timeFilter).getStartTime(),
      //              ((GroupByFilter) timeFilter).getEndTime(),
      //              ((GroupByFilter) timeFilter).getInterval());
      //        }
      //        for (ChunkSuit4Tri ChunkSuit4Tri : futureChunkList) {
      //          Statistics statistics = ChunkSuit4Tri.chunkMetadata.getStatistics();
      //          long FP_t = statistics.getStartTime();
      //          long LP_t = statistics.getEndTime();
      //          long BP_t = statistics.getBottomTimestamp();
      //          long TP_t = statistics.getTopTimestamp();
      //          switch (statistics.getType()) {
      //            case INT32:
      //              int FP_v_int = ((IntegerStatistics) statistics).getFirstValue();
      //              int LP_v_int = ((IntegerStatistics) statistics).getLastValue();
      //              int BP_v_int = ((IntegerStatistics) statistics).getMinValue();
      //              int TP_v_int = ((IntegerStatistics) statistics).getMaxValue();
      //              M4_CHUNK_METADATA.debug(
      //                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
      //                  FP_t,
      //                  LP_t,
      //                  BP_t,
      //                  TP_t,
      //                  FP_v_int,
      //                  LP_v_int,
      //                  BP_v_int,
      //                  TP_v_int,
      //                  ChunkSuit4Tri.chunkMetadata.getVersion(),
      //                  ChunkSuit4Tri.chunkMetadata.getOffsetOfChunkHeader(),
      //                  statistics.getCount());
      //              break;
      //            case INT64:
      //              long FP_v_long = ((LongStatistics) statistics).getFirstValue();
      //              long LP_v_long = ((LongStatistics) statistics).getLastValue();
      //              long BP_v_long = ((LongStatistics) statistics).getMinValue();
      //              long TP_v_long = ((LongStatistics) statistics).getMaxValue();
      //              M4_CHUNK_METADATA.debug(
      //                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
      //                  FP_t,
      //                  LP_t,
      //                  BP_t,
      //                  TP_t,
      //                  FP_v_long,
      //                  LP_v_long,
      //                  BP_v_long,
      //                  TP_v_long,
      //                  ChunkSuit4Tri.chunkMetadata.getVersion(),
      //                  ChunkSuit4Tri.chunkMetadata.getOffsetOfChunkHeader(),
      //                  statistics.getCount());
      //              break;
      //            case FLOAT:
      //              float FP_v_float = ((FloatStatistics) statistics).getFirstValue();
      //              float LP_v_float = ((FloatStatistics) statistics).getLastValue();
      //              float BP_v_float = ((FloatStatistics) statistics).getMinValue();
      //              float TP_v_float = ((FloatStatistics) statistics).getMaxValue();
      //              M4_CHUNK_METADATA.debug(
      //                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
      //                  FP_t,
      //                  LP_t,
      //                  BP_t,
      //                  TP_t,
      //                  FP_v_float,
      //                  LP_v_float,
      //                  BP_v_float,
      //                  TP_v_float,
      //                  ChunkSuit4Tri.chunkMetadata.getVersion(),
      //                  ChunkSuit4Tri.chunkMetadata.getOffsetOfChunkHeader(),
      //                  statistics.getCount());
      //              break;
      //            case DOUBLE:
      //              double FP_v_double = ((DoubleStatistics) statistics).getFirstValue();
      //              double LP_v_double = ((DoubleStatistics) statistics).getLastValue();
      //              double BP_v_double = ((DoubleStatistics) statistics).getMinValue();
      //              double TP_v_double = ((DoubleStatistics) statistics).getMaxValue();
      //              M4_CHUNK_METADATA.debug(
      //                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
      //                  FP_t,
      //                  LP_t,
      //                  BP_t,
      //                  TP_t,
      //                  FP_v_double,
      //                  LP_v_double,
      //                  BP_v_double,
      //                  TP_v_double,
      //                  ChunkSuit4Tri.chunkMetadata.getVersion(),
      //                  ChunkSuit4Tri.chunkMetadata.getOffsetOfChunkHeader(),
      //                  statistics.getCount());
      //              break;
      //            default:
      //              throw new QueryProcessException("unsupported data type!");
      //          }
      //        }
      //      }

    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }

    //    IOMonitor2.addMeasure(Operation.M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS, System.nanoTime() -
    // start);
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  /*
  private void getCurrentChunkListFromFutureChunkList(long curStartTime, long curEndTime) {
    //    IOMonitor2.M4_LSM_status = Operation.M4_LSM_MERGE_M4_TIME_SPAN;

    // empty currentChunkList
    currentChunkList = new ArrayList<>();

    // iterate futureChunkList
    ListIterator<ChunkSuit4Tri> itr = futureChunkList.listIterator();
    while (itr.hasNext()) {
      ChunkSuit4Tri chunkSuit4Tri = itr.next();
      ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
      long chunkMinTime = chunkMetadata.getStartTime();
      long chunkMaxTime = chunkMetadata.getEndTime();
      if (chunkMaxTime < curStartTime) {
        // the chunk falls on the left side of the current M4 interval Ii
        itr.remove();
      } else if (chunkMinTime >= curEndTime) {
        // the chunk falls on the right side of the current M4 interval Ii,
        // and since futureChunkList is ordered by the startTime of chunkMetadata,
        // the loop can be terminated early.
        break;
      } else if (chunkMaxTime < curEndTime) {
        // this chunk is not related to buckets later
        currentChunkList.add(chunkSuit4Tri);
        itr.remove();
      } else {
        // this chunk is overlapped with the right border of the current bucket
        currentChunkList.add(chunkSuit4Tri);
        // still keep it in the futureChunkList
      }
    }
  }
   */

  @Override
  public List<AggregateResult> calcResult(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException {
    StringBuilder series = new StringBuilder();

    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }

    series.append(CONFIG.getP1v()).append("[").append(CONFIG.getP1t()).append("]").append(",");

    int tmpLastReadPos = 0;
    // use list without the notion of chunks
    for (int b = 0; b < N1; b++) {
      long localCurStartTime = startTime + (b) * interval;
      long localCurEndTime = startTime + (b + 1) * interval;

      double minValue = Double.MAX_VALUE;
      long bottomTime = -1;
      double maxValue = -Double.MAX_VALUE; // Double.MIN_VALUE is positive so do not use it!!!
      long topTime = -1;
      long firstTime = -1;
      double firstValue = 0;
      long lastTime = -1;
      double lastValue = 0;

      for (int i = tmpLastReadPos; i < timeSeries.data.size(); i++) {
        IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
        long timestamp = timeSeries.data.get(i).getTimestamp();
        if (timestamp < localCurStartTime) {
          continue;
        } else if (timestamp >= localCurEndTime) {
          tmpLastReadPos = i;
          break;
        } else {
          double v = timeSeries.data.get(i).getValue();
          if (firstTime < 0) {
            firstTime = timestamp;
            firstValue = v;
          }
          lastTime = timestamp;
          lastValue = v;
          if (v < minValue) {
            minValue = v;
            bottomTime = timestamp;
          }
          if (v > maxValue) {
            maxValue = v;
            topTime = timestamp;
          }
        }
      }

      if (topTime >= 0) {
        // minValue[bottomTime],maxValue[topTime],firstValue[firstTime],lastValue[lastTime]
        series
            .append(minValue)
            .append("[")
            .append(bottomTime)
            .append("]")
            .append(",")
            .append(maxValue)
            .append("[")
            .append(topTime)
            .append("]")
            .append(",")
            .append(firstValue)
            .append("[")
            .append(firstTime)
            .append("]")
            .append(",")
            .append(lastValue)
            .append("[")
            .append(lastTime)
            .append("]")
            .append(",");
      } else {
        // minValue[bottomTime],maxValue[topTime],firstValue[firstTime],lastValue[lastTime]
        series.append("null[null],null[null],null[null],null[null],");
      }

      /*
      getCurrentChunkListFromFutureChunkList(localCurStartTime, localCurEndTime);

      if (currentChunkList.size() == 0) {
        // minValue[bottomTime],maxValue[topTime],firstValue[firstTime],lastValue[lastTime]
        series.append("null[null],null[null],null[null],null[null],");
        continue;
      }

      calculateM4(currentChunkList, localCurStartTime, localCurEndTime, series);
       */
    }

    series.append(CONFIG.getPnv()).append("[").append(CONFIG.getPnt()).append("]").append(",");

    MinValueAggrResult minValueAggrResult = (MinValueAggrResult) results.get(0);
    minValueAggrResult.updateResult(new MinMaxInfo<>(series.toString(), 0));

    return results;
  }

  private void calculateM4(
      List<ChunkSuit4Tri> currentChunkList,
      long curStartTime,
      long curEndTime,
      StringBuilder series)
      throws IOException {
    double minValue = Double.MAX_VALUE;
    long bottomTime = -1;
    double maxValue = -Double.MAX_VALUE; // Double.MIN_VALUE is positive so do not use it!!!
    long topTime = -1;
    long firstTime = -1;
    double firstValue = 0;
    long lastTime = -1;
    double lastValue = 0;

    for (ChunkSuit4Tri chunkSuit4Tri : currentChunkList) {

      Statistics statistics = chunkSuit4Tri.chunkMetadata.getStatistics();

      if (canUseStatistics(chunkSuit4Tri, curStartTime, curEndTime)) {
        // update BP
        double chunkMinValue = (double) statistics.getMinValue();
        if (chunkMinValue < minValue) {
          minValue = chunkMinValue;
          bottomTime = statistics.getBottomTimestamp();
        }
        // update TP
        double chunkMaxValue = (double) statistics.getMaxValue();
        if (chunkMaxValue > maxValue) {
          maxValue = chunkMaxValue;
          topTime = statistics.getTopTimestamp();
        }
        // update FP
        if (firstTime < 0) {
          firstTime = statistics.getStartTime();
          firstValue = (double) statistics.getFirstValue();
        }
        lastTime = statistics.getEndTime(); // assume sequential data
        lastValue = (double) statistics.getLastValue();
      } else { // cannot use statistics directly

        // 1. load page data if it hasn't been loaded
        TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
        if (dataType != TSDataType.DOUBLE) {
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
        if (chunkSuit4Tri.pageReader == null) {
          chunkSuit4Tri.pageReader =
              FileLoaderUtils.loadPageReaderList4CPV(chunkSuit4Tri.chunkMetadata, this.timeFilter);
          //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
          //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
          //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
          //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
          //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
        }

        int count = chunkSuit4Tri.chunkMetadata.getStatistics().getCount();
        PageReader pageReader = chunkSuit4Tri.pageReader;
        int i;
        for (i = chunkSuit4Tri.lastReadPos; i < count; i++) {
          IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
          long timestamp = pageReader.timeBuffer.getLong(i * 8);
          if (timestamp < curStartTime) {
            // 2. read from lastReadPos until the first point fallen within this bucket (if it
            // exists)
            continue;
          } else if (timestamp >= curEndTime) {
            // 3. traverse until the first point fallen right this bucket, also remember to update
            // lastReadPos
            chunkSuit4Tri.lastReadPos = i;
            break;
          } else {
            // 4. update MinMax by traversing points fallen within this bucket
            ByteBuffer valueBuffer = pageReader.valueBuffer;
            double v = valueBuffer.getDouble(pageReader.timeBufferLength + i * 8);
            if (firstTime < 0) {
              firstTime = timestamp;
              firstValue = v;
            }
            lastTime = timestamp;
            lastValue = v;
            if (v < minValue) {
              minValue = v;
              bottomTime = timestamp;
            }
            if (v > maxValue) {
              maxValue = v;
              topTime = timestamp;
            }
          }
        }
      }
    }
    if (topTime >= 0) {
      // minValue[bottomTime],maxValue[topTime],firstValue[firstTime],lastValue[lastTime]
      series
          .append(minValue)
          .append("[")
          .append(bottomTime)
          .append("]")
          .append(",")
          .append(maxValue)
          .append("[")
          .append(topTime)
          .append("]")
          .append(",")
          .append(firstValue)
          .append("[")
          .append(firstTime)
          .append("]")
          .append(",")
          .append(lastValue)
          .append("[")
          .append(lastTime)
          .append("]")
          .append(",");
    } else {
      // empty bucket although statistics cover
      // minValue[bottomTime],maxValue[topTime],firstValue[firstTime],lastValue[lastTime]
      series.append("null[null],null[null],null[null],null[null],");
    }
  }

  public boolean canUseStatistics(ChunkSuit4Tri chunkSuit4Tri, long curStartTime, long curEndTime) {
    return false;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
      throws IOException {
    throw new IOException("no implemented");
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    throw new IOException("no implemented");
  }
}
