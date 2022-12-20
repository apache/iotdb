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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class QueryStatistics {

  private static final long QUERY_STATISTICS_PRINT_INTERVAL_IN_MS = 10_000;

  private static final Logger QUERY_STATISTICS_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.QUERY_STATISTICS_LOGGER_NAME);

  private final AtomicBoolean tracing = new AtomicBoolean(false);

  private final Map<String, OperationStatistic> operationStatistics = new ConcurrentHashMap<>();

  public static final String LOCAL_EXECUTION_PLANNER = "LocalExecutionPlanner";

  public static final String CREATE_FI_CONTEXT = "CreateFIContext";

  public static final String CREATE_FI_EXEC = "CreateFIExec";

  public static final String NODE_TO_OPERATOR = "ToOpTree";

  public static final String CHECK_MEMORY = "CheckMem";

  public static final String ALLOC_EX_MEMORY = "AllocExchangeMem";

  public static final String QUERY_EXECUTION = "QueryExecution";

  public static final String QUERY_RESOURCE_INIT = "QueryResourceInit";

  public static final String INIT_SOURCE_OP = "InitSourceOp";

  public static final String QUERY_RESOURCE_LIST = "TsFileList";
  public static final String ADD_REFERENCE = "AddRef";

  public static final String LOCAL_SOURCE_HANDLE_GET_TSBLOCK = "LocalSourceHandleGetTsBlock";

  public static final String LOCAL_SOURCE_HANDLE_SER_TSBLOCK = "LocalSourceHandleSerializeTsBlock";

  public static final String REMOTE_SOURCE_HANDLE_GET_TSBLOCK = "RemoteSourceHandleGetTsBlock";

  public static final String REMOTE_SOURCE_HANDLE_DESER_TSBLOCK =
      "RemoteSourceHandleDeserializeTsBlock";

  public static final String WAIT_FOR_RESULT = "WaitForResult";

  public static final String SERIES_SCAN_OPERATOR = "SeriesScanOperator";

  public static final String ALIGNED_SERIES_SCAN_OPERATOR = "AlignedSeriesScanOperator";

  public static final String AGG_SCAN_OPERATOR = "AbstractSeriesAggregationScanOperator";

  public static final String CAL_NEXT_AGG_RES = "CalcNextAggRes";

  public static final String CAL_AGG_FROM_RAW_DATA = "CalcAggFromRawData";

  public static final String AGGREGATOR_PROCESS_TSBLOCK = "AggProcTsBlock";

  public static final String CAL_AGG_FROM_PAGE = "CalcAggFromPage";

  public static final String CAL_AGG_FROM_CHUNK = "CalcAggFromChunk";

  public static final String CAL_AGG_FROM_FILE = "CalcAggFromFile";

  public static final String BUILD_AGG_RES = "BuildAggRes";

  public static final String FILTER_AND_PROJECT_OPERATOR = "FilterAndProjectOperator";

  public static final String SINGLE_INPUT_AGG_OPERATOR = "SingleInputAggregationOperator";

  public static final String PAGE_READER = "IPageReader";
  public static final String PARSER = "Parser";

  public static final String CREATE_QUERY_EXEC = "CreateQueryExec";

  public static final String SERIALIZE_TSBLOCK = "SerTsBlock";

  public static final String ANALYZER = "Analyzer";
  public static final String SCHEMA_FETCHER = "SchemaFetcher";
  public static final String PARTITION_FETCHER = "PartitionFetcher";
  public static final String LOGICAL_PLANNER = "LogicalPlanner";
  public static final String DISTRIBUTION_PLANNER = "DistributionPlanner";
  public static final String DISPATCHER = "Dispatcher";

  public static final String WAIT_FOR_DISPATCH = "WaitForDispatch";

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

  public static final String LOAD_TIME_SERIES_METADATA_ALIGNED = "loadTimeSeriesMetadata-aligned";
  public static final String LOAD_TIME_SERIES_METADATA = "loadTimeSeriesMetadata";
  public static final String LOAD_CHUNK_METADATA_LIST = "loadChunkMetadataList";
  public static final String LOAD_PAGE_READER_LIST = "loadPageReaderList";
  public static final String TIME_SERIES_METADATA_CACHE_MISS = "TimeSeriesMetadataCacheMiss";
  public static final String CHUNK_CACHE_MISS = "ChunkCacheMiss";

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

      String res =
          System.lineSeparator()
              + "Client Connection Thread:"
              + System.lineSeparator()
              + System.lineSeparator()
              + "ServerRpcRT "
              + operationStatistics.get(SERVER_RPC_RT)
              + System.lineSeparator()
              + "|___CreateQueryExec "
              + operationStatistics.get(CREATE_QUERY_EXEC)
              + System.lineSeparator()
              + "|   |___Parser "
              + operationStatistics.get(PARSER)
              + System.lineSeparator()
              + "|   |___Analyzer "
              + operationStatistics.get(ANALYZER)
              + System.lineSeparator()
              + "|   |   |___PartitionFetcher "
              + operationStatistics.get(PARTITION_FETCHER)
              + System.lineSeparator()
              + "|   |   |___SchemaFetcher "
              + operationStatistics.get(SCHEMA_FETCHER)
              + System.lineSeparator()
              + "|   |___LogicalPlanner "
              + operationStatistics.get(LOGICAL_PLANNER)
              + System.lineSeparator()
              + "|   |___DistributionPlanner "
              + operationStatistics.get(DISTRIBUTION_PLANNER)
              + System.lineSeparator()
              + "|   |___Dispatcher "
              + operationStatistics.get(DISPATCHER)
              + System.lineSeparator()
              + "|       |___DispatchRead "
              + operationStatistics.get(DISPATCH_READ)
              + System.lineSeparator()
              + "|           |___LocalExecPlanner "
              + operationStatistics.get(LOCAL_EXECUTION_PLANNER)
              + System.lineSeparator()
              + "|               |___FIContext "
              + operationStatistics.get(CREATE_FI_CONTEXT)
              + System.lineSeparator()
              + "|               |___ToOpTree "
              + operationStatistics.get(NODE_TO_OPERATOR)
              + System.lineSeparator()
              + "|               |___CheckMem "
              + operationStatistics.get(CHECK_MEMORY)
              + System.lineSeparator()
              + "|               |___AllocExcgMem "
              + operationStatistics.get(ALLOC_EX_MEMORY)
              + System.lineSeparator()
              + "|               |___FIExec "
              + operationStatistics.get(CREATE_FI_EXEC)
              + System.lineSeparator()
              + "|___SerTsBlock "
              + operationStatistics.get(SERIALIZE_TSBLOCK)
              + System.lineSeparator()
              + "    |___WaitForResult "
              + operationStatistics.get(WAIT_FOR_RESULT)
              + System.lineSeparator()
              + "    |___GetTsBlock "
              + operationStatistics.get(LOCAL_SOURCE_HANDLE_GET_TSBLOCK)
              + System.lineSeparator()
              + "        |___FreeMem "
              + operationStatistics.get(FREE_MEM)
              + System.lineSeparator()
              + "Query Execution Thread:"
              + System.lineSeparator()
              + System.lineSeparator()
              + "|___QueryResourceInit "
              + operationStatistics.get(QUERY_RESOURCE_INIT)
              + System.lineSeparator()
              + "|   |___TsFileList "
              + operationStatistics.get(QUERY_RESOURCE_LIST)
              + System.lineSeparator()
              + "|   |___AddRef "
              + operationStatistics.get(ADD_REFERENCE)
              + System.lineSeparator()
              + "|   |___InitSourceOp "
              + operationStatistics.get(INIT_SOURCE_OP)
              + System.lineSeparator()
              + "|___DriverInternalProcess "
              + operationStatistics.get(DRIVER_INTERNAL_PROCESS)
              + System.lineSeparator()
              + "|   |___AggScanOperator "
              + operationStatistics.get(AGG_SCAN_OPERATOR)
              + System.lineSeparator()
              + "|   |   |___CalcNextAggRes "
              + operationStatistics.get(CAL_NEXT_AGG_RES)
              + System.lineSeparator()
              + "|   |       |___loadTSMeta "
              + operationStatistics.get(LOAD_TIME_SERIES_METADATA_ALIGNED)
              + System.lineSeparator()
              + "|   |       |___AggFromFile "
              + operationStatistics.get(CAL_AGG_FROM_FILE)
              + System.lineSeparator()
              + "|   |       |   |___loadChunkMeta "
              + operationStatistics.get(LOAD_CHUNK_METADATA_LIST)
              + System.lineSeparator()
              + "|   |       |   |___AggFromChunk "
              + operationStatistics.get(CAL_AGG_FROM_CHUNK)
              + System.lineSeparator()
              + "|   |       |       |___loadChunk "
              + operationStatistics.get(LOAD_PAGE_READER_LIST)
              + System.lineSeparator()
              + "|   |       |       |___AggFromPage "
              + operationStatistics.get(CAL_AGG_FROM_PAGE)
              + System.lineSeparator()
              + "|   |       |           |___loadPage "
              + operationStatistics.get(PAGE_READER)
              + System.lineSeparator()
              + "|   |       |           |___AggFromRawData "
              + operationStatistics.get(CAL_AGG_FROM_RAW_DATA)
              + System.lineSeparator()
              + "|   |       |               |___AggProcTsBlock "
              + operationStatistics.get(AGGREGATOR_PROCESS_TSBLOCK)
              + System.lineSeparator()
              + "|   |       |___BuildAggRes "
              + operationStatistics.get(BUILD_AGG_RES)
              + System.lineSeparator()
              + "|   |___SendTsBlock "
              + operationStatistics.get(SEND_TSBLOCK)
              + System.lineSeparator()
              + "|       |___ReserveMem "
              + operationStatistics.get(RESERVE_MEMORY)
              + System.lineSeparator()
              + "|       |___NotifyNewTsBlock "
              + operationStatistics.get(NOTIFY_NEW_TSBLOCK)
              + System.lineSeparator()
              + "|___SetNoMoreTsBlock "
              + operationStatistics.get(SET_NO_MORE_TSBLOCK)
              + System.lineSeparator()
              + "    |___NotifyEnd "
              + operationStatistics.get(NOTIFY_END)
              + System.lineSeparator()
              + "    |___EndListener "
              + operationStatistics.get(SINK_HANDLE_END_LISTENER)
              + System.lineSeparator()
              + "    |___CkAndInvOnFinished "
              + operationStatistics.get(CHECK_AND_INVOKE_ON_FINISHED)
              + System.lineSeparator()
              + "        |___FinishListener "
              + operationStatistics.get(SINK_HANDLE_FINISH_LISTENER)
              + System.lineSeparator();

      QUERY_STATISTICS_LOGGER.info(res);
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

  public void disableTracing() {
    tracing.set(false);
    operationStatistics.clear();
  }

  public void enableTracing() {
    tracing.set(true);
    operationStatistics.clear();
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
          + time
          + "us"
          + ", totalCount="
          + count
          + ", avgOpTime="
          + (time / count)
          + "us"
          + '}';
    }
  }

  private static class QueryStatisticsHolder {

    private static final QueryStatistics INSTANCE = new QueryStatistics();

    private QueryStatisticsHolder() {}
  }
}
