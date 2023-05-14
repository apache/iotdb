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
package org.apache.iotdb.db.mpp.statistics;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class QueryStatistics {

  private static final long QUERY_STATISTICS_PRINT_INTERVAL_IN_MS = 100_000;

  private static final Logger QUERY_STATISTICS_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.QUERY_STATISTICS_LOGGER_NAME);

  private static final DecimalFormat format = new DecimalFormat("#,###");

  private final AtomicBoolean tracing = new AtomicBoolean(true);

  private final Map<String, OperationStatistic> operationStatistics = new ConcurrentHashMap<>();

  public static final String LOCAL_EXECUTION_PLANNER = "LocalExecutionPlanner";

  public static final String CREATE_FI_CONTEXT = "CreateFIContext";

  public static final String CREATE_FI_EXEC = "CreateFIExec";

  public static final String NODE_TO_OPERATOR = "ToOpTree";

  public static final String CHECK_MEMORY = "CheckMem";

  public static final String QUERY_RESOURCE_INIT = "QueryResourceInit";

  public static final String INIT_SOURCE_OP = "InitSourceOp";

  public static final String QUERY_RESOURCE_LIST = "TsFileList";
  public static final String ADD_REFERENCE = "AddRef";

  public static final String LOCAL_SOURCE_HANDLE_GET_TSBLOCK = "LocalSourceHandleGetTsBlock";

  public static final String LOCAL_SOURCE_HANDLE_SER_TSBLOCK = "LocalSourceHandleSerializeTsBlock";

  public static final String WAIT_FOR_RESULT = "WaitForResult";

  public static final String AGG_SCAN_OPERATOR = "AbstractSeriesAggregationScanOperator";

  public static final String CAL_NEXT_AGG_RES = "CalcNextAggRes";

  public static final String CAL_AGG_FROM_RAW_DATA = "CalcAggFromRawData";

  public static final String CAL_AGG_FROM_STAT = "CalcAggFromStat";

  public static final String AGGREGATOR_PROCESS_TSBLOCK = "AggProcTsBlock";

  public static final String CAL_AGG_FROM_PAGE = "CalcAggFromPage";

  public static final String CAL_AGG_FROM_CHUNK = "CalcAggFromChunk";

  public static final String CAL_AGG_FROM_FILE = "CalcAggFromFile";

  public static final String CAL_AGG_FROM_FILE_STAT = "CalcAggFromFileStat";
  public static final String CAL_AGG_FROM_CHUNK_STAT = "CalcAggFromChunkStat";
  public static final String CAL_AGG_FROM_PAGE_STAT = "CalcAggFromPageStat";

  public static final String BUILD_AGG_RES = "BuildAggRes";

  public static final String PARSER = "Parser";

  public static final String CREATE_QUERY_EXEC = "CreateQueryExec";

  public static final String SERIALIZE_TSBLOCK = "SerTsBlock";

  public static final String ANALYZER = "Analyzer";
  public static final String SCHEMA_FETCHER = "SchemaFetcher";
  public static final String PARTITION_FETCHER = "PartitionFetcher";
  public static final String LOGICAL_PLANNER = "LogicalPlanner";
  public static final String DISTRIBUTION_PLANNER = "DistributionPlanner";
  public static final String DISPATCHER = "Dispatcher";

  public static final String DISPATCH_READ = "DispatchRead";

  public static final String DRIVER_CLOSE = "CloseDriver";

  public static final String DRIVER_INTERNAL_PROCESS = "DriverInternalProcess";

  public static final String SEND_TSBLOCK = "SendTsBlock";

  public static final String RESERVE_MEMORY = "ReserveMem";

  public static final String NOTIFY_NEW_TSBLOCK = "NotifyNewTsBlock";

  public static final String NOTIFY_END = "NotifyEnd";

  public static final String FREE_MEM = "FreeMem";

  public static final String SINK_HANDLE_END_LISTENER = "SinkHandleEndListener";

  public static final String SINK_HANDLE_FINISH_LISTENER = "SinkHandleFinishListener";

  public static final String CHECK_AND_INVOKE_ON_FINISHED = "CheckAndInvokeOnFinished";

  public static final String SET_NO_MORE_TSBLOCK = "SetNoMoreTsBlock";

  public static final String SERVER_RPC_RT = "ServerRpcRT";

  public static final String LOAD_TIME_SERIES_METADATA = "loadTimeSeriesMetadata";
  public static final String LOAD_CHUNK_METADATA_LIST = "loadChunkMetadataList";
  public static final String LOAD_PAGE_READER_LIST = "loadPageReaderList";
  public static final String LOAD_CHUNK = "loadChunk";
  public static final String INIT_PAGE_READERS = "initAllPageReaders";
  public static final String PAGE_READER = "IPageReader";

  public static final String HAS_NEXT_FILE = "hasNextFile";
  public static final String FILTER_FIRST_TIMESERIES_METADATA = "filterFirstTimeSeriesMetadata";
  public static final String FIND_END_TIME = "findEndTime";
  public static final String PICK_FIRST_TIMESERIES_METADATA = "pickFirstTimeSeriesMetadata";

  public static final String HAS_NEXT_CHUNK = "hasNextChunk";
  public static final String FILTER_FIRST_CHUNK_METADATA = "filterFirstChunkMetadata";

  public static final String HAS_NEXT_PAGE = "hasNextPage";
  public static final String HAS_NEXT_OVERLAPPED_PAGE = "hasNextOverlappedPage";
  public static final String MERGE_READER_ADD_READER = "mergeReader#addReader";
  public static final String MERGE_READER_NEXT = "mergeReader#nextTimeValuePair";
  public static final String MERGE_READER_UPDATE_HEAP = "mergeReader#updateHeap";
  public static final String MERGE_READER_FILL_NULL_VALUE = "mergeReader#fillNullValue";
  public static final String MERGE_READER_BUILD_RES = "mergeReader#buildRes";

  private QueryStatistics() {
    ScheduledExecutorService scheduledExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, "Query-Statistics-Print");
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduledExecutor,
        this::printQueryStatistics,
        0,
        QUERY_STATISTICS_PRINT_INTERVAL_IN_MS,
        TimeUnit.MILLISECONDS);
  }

  private void printQueryStatistics() {
    if (tracing.get()) {

      StringBuilder builder = new StringBuilder(System.lineSeparator());
      builder
          .append("Client Connection Thread:")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      builder
          .append("ServerRpcRT ")
          .append(operationStatistics.get(SERVER_RPC_RT))
          .append(System.lineSeparator());
      builder
          .append("|___CreateQueryExec ")
          .append(operationStatistics.get(CREATE_QUERY_EXEC))
          .append(System.lineSeparator());
      builder
          .append("|   |___Parser ")
          .append(operationStatistics.get(PARSER))
          .append(System.lineSeparator());
      builder
          .append("|   |___Analyzer ")
          .append(operationStatistics.get(ANALYZER))
          .append(System.lineSeparator());
      builder
          .append("|   |   |___PartitionFetcher ")
          .append(operationStatistics.get(PARTITION_FETCHER))
          .append(System.lineSeparator());
      builder
          .append("|   |   |___SchemaFetcher ")
          .append(operationStatistics.get(SCHEMA_FETCHER))
          .append(System.lineSeparator());
      builder
          .append("|   |___LogicalPlanner ")
          .append(operationStatistics.get(LOGICAL_PLANNER))
          .append(System.lineSeparator());
      builder
          .append("|   |___DistributionPlanner ")
          .append(operationStatistics.get(DISTRIBUTION_PLANNER))
          .append(System.lineSeparator());
      builder
          .append("|   |___Dispatcher ")
          .append(operationStatistics.get(DISPATCHER))
          .append(System.lineSeparator());
      builder
          .append("|       |___DispatchRead ")
          .append(operationStatistics.get(DISPATCH_READ))
          .append(System.lineSeparator());
      builder
          .append("|           |___LocalExecPlanner ")
          .append(operationStatistics.get(LOCAL_EXECUTION_PLANNER))
          .append(System.lineSeparator());
      builder
          .append("|               |___FIContext ")
          .append(operationStatistics.get(CREATE_FI_CONTEXT))
          .append(System.lineSeparator());
      builder
          .append("|               |___ToOpTree ")
          .append(operationStatistics.get(NODE_TO_OPERATOR))
          .append(System.lineSeparator());
      builder
          .append("|               |___CheckMem ")
          .append(operationStatistics.get(CHECK_MEMORY))
          .append(System.lineSeparator());
      builder
          .append("|               |___FIExec ")
          .append(operationStatistics.get(CREATE_FI_EXEC))
          .append(System.lineSeparator());
      builder
          .append("|___SerTsBlock ")
          .append(operationStatistics.get(SERIALIZE_TSBLOCK))
          .append(System.lineSeparator());
      builder
          .append("    |___WaitForResult ")
          .append(operationStatistics.get(WAIT_FOR_RESULT))
          .append(System.lineSeparator());
      builder
          .append("    |___GetTsBlock ")
          .append(operationStatistics.get(LOCAL_SOURCE_HANDLE_GET_TSBLOCK))
          .append(System.lineSeparator());
      builder
          .append("        |___FreeMem ")
          .append(operationStatistics.get(FREE_MEM))
          .append(System.lineSeparator());

      builder
          .append("Query Execution Thread:")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      builder
          .append("|___QueryResourceInit ")
          .append(operationStatistics.get(QUERY_RESOURCE_INIT))
          .append(System.lineSeparator());
      builder
          .append("|   |___TsFileList ")
          .append(operationStatistics.get(QUERY_RESOURCE_LIST))
          .append(System.lineSeparator());
      builder
          .append("|   |___AddRef ")
          .append(operationStatistics.get(ADD_REFERENCE))
          .append(System.lineSeparator());
      builder
          .append("|   |___InitSourceOp ")
          .append(operationStatistics.get(INIT_SOURCE_OP))
          .append(System.lineSeparator());
      builder
          .append("|___DriverInternalProcess ")
          .append(operationStatistics.get(DRIVER_INTERNAL_PROCESS))
          .append(System.lineSeparator());
      builder
          .append("|   |___AggScanOperator ")
          .append(operationStatistics.get(AGG_SCAN_OPERATOR))
          .append(System.lineSeparator());

      builder.append("|   |    |[FileLoaderInterface]").append(System.lineSeparator());
      builder
          .append("|   |       |___loadTSMetadata ")
          .append(operationStatistics.get(LOAD_TIME_SERIES_METADATA))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___loadChunkMetaList ")
          .append(operationStatistics.get(LOAD_CHUNK_METADATA_LIST))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___loadPageReaderList ")
          .append(operationStatistics.get(LOAD_PAGE_READER_LIST))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___loadChunk ")
          .append(operationStatistics.get(LOAD_CHUNK))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___initPageReaders ")
          .append(operationStatistics.get(INIT_PAGE_READERS))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___pageReader ")
          .append(operationStatistics.get(PAGE_READER))
          .append(System.lineSeparator());

      builder.append("|   |    |[AggregatorInterface]").append(System.lineSeparator());
      builder
          .append("|   |       |___AggFromStat ")
          .append(operationStatistics.get(CAL_AGG_FROM_STAT))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___AggFromRawData ")
          .append(operationStatistics.get(CAL_AGG_FROM_RAW_DATA))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___AggProcTsBlock ")
          .append(operationStatistics.get(AGGREGATOR_PROCESS_TSBLOCK))
          .append(System.lineSeparator());

      builder.append("|   |    |[OperatorMethods]").append(System.lineSeparator());
      builder
          .append("|   |       |___CalcNextAggRes ")
          .append(operationStatistics.get(CAL_NEXT_AGG_RES))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___AggFromFile ")
          .append(operationStatistics.get(CAL_AGG_FROM_FILE))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___TryAggFromFileStat ")
          .append(operationStatistics.get(CAL_AGG_FROM_FILE_STAT))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___AggFromChunk ")
          .append(operationStatistics.get(CAL_AGG_FROM_CHUNK))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___TryAggFromChunkStat ")
          .append(operationStatistics.get(CAL_AGG_FROM_CHUNK_STAT))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___AggFromPage ")
          .append(operationStatistics.get(CAL_AGG_FROM_PAGE))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___TryAggFromPageStat ")
          .append(operationStatistics.get(CAL_AGG_FROM_PAGE_STAT))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___BuildAggRes ")
          .append(operationStatistics.get(BUILD_AGG_RES))
          .append(System.lineSeparator());

      builder.append("|   |    |[SeriesScanUtilCost]").append(System.lineSeparator());
      builder
          .append("|   |       |___hasNextFile ")
          .append(operationStatistics.get(HAS_NEXT_FILE))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___findEndTime ")
          .append(operationStatistics.get(FIND_END_TIME))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___pickFirstTimeSeriesMetadata ")
          .append(operationStatistics.get(PICK_FIRST_TIMESERIES_METADATA))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___filterFirstTimeSeriesMetadata ")
          .append(operationStatistics.get(FILTER_FIRST_TIMESERIES_METADATA))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___hasNextChunk ")
          .append(operationStatistics.get(HAS_NEXT_CHUNK))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___filterFirstChunkMetadata ")
          .append(operationStatistics.get(FILTER_FIRST_CHUNK_METADATA))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___hasNextPage ")
          .append(operationStatistics.get(HAS_NEXT_PAGE))
          .append(System.lineSeparator());
      builder
          .append("|   |       |___hasNextOverlappedPage ")
          .append(operationStatistics.get(HAS_NEXT_OVERLAPPED_PAGE))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___mergeReader#nextTimeValuePair ")
          .append(operationStatistics.get(MERGE_READER_NEXT))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___mergeReader#updateHeap ")
          .append(operationStatistics.get(MERGE_READER_UPDATE_HEAP))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___mergeReader#fillNullValue ")
          .append(operationStatistics.get(MERGE_READER_FILL_NULL_VALUE))
          .append(System.lineSeparator());
      builder
          .append("|   |       |   |___buildTsblock ")
          .append(operationStatistics.get(MERGE_READER_BUILD_RES))
          .append(System.lineSeparator());

      builder
          .append("|   |___SendTsBlock ")
          .append(operationStatistics.get(SEND_TSBLOCK))
          .append(System.lineSeparator());
      builder
          .append("|       |___ReserveMem ")
          .append(operationStatistics.get(RESERVE_MEMORY))
          .append(System.lineSeparator());
      builder
          .append("|       |___NotifyNewTsBlock ")
          .append(operationStatistics.get(NOTIFY_NEW_TSBLOCK))
          .append(System.lineSeparator());
      builder
          .append("|___SetNoMoreTsBlock ")
          .append(operationStatistics.get(SET_NO_MORE_TSBLOCK))
          .append(System.lineSeparator());
      builder
          .append("    |___NotifyEnd ")
          .append(operationStatistics.get(NOTIFY_END))
          .append(System.lineSeparator());
      builder
          .append("    |___EndListener ")
          .append(operationStatistics.get(SINK_HANDLE_END_LISTENER))
          .append(System.lineSeparator());
      builder
          .append("    |___CkAndInvOnFinished ")
          .append(operationStatistics.get(CHECK_AND_INVOKE_ON_FINISHED))
          .append(System.lineSeparator());
      builder
          .append("        |___FinishListener ")
          .append(operationStatistics.get(SINK_HANDLE_FINISH_LISTENER))
          .append(System.lineSeparator());

      QUERY_STATISTICS_LOGGER.info(builder.toString());
      QUERY_STATISTICS_LOGGER.info("");
    }
  }

  public static QueryStatistics getInstance() {
    return QueryStatisticsHolder.INSTANCE;
  }

  public void addCost(String key, long costTimeInNanos) {
    if (tracing.get()) {
      operationStatistics
          .computeIfAbsent(key, k -> new OperationStatistic())
          .addTimeCost(costTimeInNanos);
    }
  }

  private static class OperationStatistic {
    // accumulated operation time in ns
    private final AtomicLong totalTime;
    private final AtomicLong totalCount;

    public OperationStatistic() {
      this.totalTime = new AtomicLong(0);
      this.totalCount = new AtomicLong(0);
    }

    public void addTimeCost(long costTimeInNanos) {
      totalTime.addAndGet(costTimeInNanos);
      totalCount.incrementAndGet();
    }

    @Override
    public String toString() {
      long time = totalTime.get() / 1_000;
      long count = totalCount.get();
      return "{"
          + "totalTime="
          + format.format(time)
          + "us"
          + ", totalCount="
          + format.format(count)
          + ", avgOpTime="
          + format.format(time / count)
          + "us"
          + '}';
    }
  }

  private static class QueryStatisticsHolder {

    private static final QueryStatistics INSTANCE = new QueryStatistics();

    private QueryStatisticsHolder() {}
  }
}
