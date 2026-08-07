/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.statistics.QueryPlanStatistics;

import org.apache.tsfile.read.filter.basic.Filter;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;

/**
 * This class is used to record the context of a query including QueryId, query statement, session
 * info and so on.
 */
public class MPPQueryContext {
  private String sql;
  private final QueryId queryId;

  // LocalQueryId is kept to adapt to the old client, it's unique in current datanode.
  // Now it's only be used by EXPLAIN ANALYZE to get queryExecution.
  private long localQueryId;
  private SessionInfo session;
  private QueryType queryType = QueryType.READ;

  /** the max executing time of query in ms. Unit: millisecond */
  private long timeOut;

  // time unit is ms
  private long startTime;

  private TEndPoint localDataBlockEndpoint;
  private TEndPoint localInternalEndpoint;
  private ResultNodeContext resultNodeContext;

  // Main FragmentInstance, the other FragmentInstance should push data result to this
  // FragmentInstance
  private TRegionReplicaSet mainFragmentLocatedRegion;

  // When some DataNode cannot be connected, its endPoint will be put
  // in this list. And the following retry will avoid planning fragment
  // onto this node.
  // When dispatch FI fails, this structure may be modified concurrently
  private final Set<TEndPoint> endPointBlackList;

  private final TypeProvider typeProvider = new TypeProvider();

  private Filter globalTimeFilter;

  private final Set<SchemaLockType> acquiredLocks = new HashSet<>();

  private boolean isExplainAnalyze = false;

  QueryPlanStatistics queryPlanStatistics = null;

  // To avoid query front-end from consuming too much memory, it needs to reserve memory when
  // constructing some Expression and PlanNode.
  private final MemoryReservationManager memoryReservationManager;

  private static final int minSizeToUseSampledTimeseriesOperandMemCost = 100;
  private static final String RESULT_SET_COLUMN_METADATA_MEMORY_NOT_ENOUGH =
      "Not enough memory while analyzing metadata for query result columns. "
          + "The result set has too many columns. "
          + "Before the failure, IoTDB had matched %,d source columns for result-column "
          + "expansion, expanded %,d source columns, and generated %,d result-set columns. "
          + "%s"
          + "Current series pagination is %s. "
          + "Use SLIMIT/SOFFSET to reduce returned series%s, narrow the path pattern, "
          + "or increase query memory%s. "
          + "Memory details: source-column memory for result expansion %s, "
          + "generated-result-column memory %s, requested this time %s, current free memory %s. "
          + "Original error: %s";
  private static final String RESULT_SET_COLUMNS_EXCEED_MEMORY_CAPACITY =
      "The matched source columns exceed the estimated current memory capacity by "
          + "at least %,d columns. ";
  private static final String SCHEMA_FETCH_METADATA_MEMORY_NOT_ENOUGH =
      "Not enough memory while fetching metadata for query analysis. "
          + "The result set may have too many columns. "
          + "Before the failure, IoTDB had deserialized %,d time-series columns from schema "
          + "fetch results. Schema fetch memory may be reserved before safely deserializing "
          + "the whole fetched metadata, so this count can be lower than the matched schema "
          + "columns. %s"
          + "Current series pagination is %s. "
          + "Use SLIMIT/SOFFSET to reduce returned series%s, narrow the path pattern, "
          + "or increase query memory%s. "
          + "Memory details: fetched schema tree estimated memory %s, "
          + "fetched schema tree reserved memory %s, requested this time %s, "
          + "current free memory %s. Original error: %s";
  private static final String SCHEMA_FETCH_COLUMNS_EXCEED_MEMORY_CAPACITY =
      "The fetched schema columns exceed the estimated current memory capacity by "
          + "at least %,d columns. ";
  private static final String USE_ALIGN_BY_DEVICE_TO_REDUCE_RESULT_COLUMNS =
      ", use ALIGN BY DEVICE to reduce cross-device result columns";
  private static final String BY_AT_LEAST_MEMORY_SIZE = " by at least %s";
  private static final String FOR_QUERY_ENGINE_OPERATOR_MEMORY_POOL =
      " for the query engine/operator memory pool";
  private static final String SERIES_PAGINATION_FOR_DIAGNOSTICS = "SLIMIT=%s, SOFFSET=%,d";
  private static final String NOT_SET = "not set";
  private static final String UNKNOWN = "unknown";
  private double avgTimeseriesOperandMemCost = 0;
  private int numsOfSampledTimeseriesOperand = 0;
  // When there is no view in a last query and no device exists in multiple regions,
  // the updateScanNum process in distributed planning can be skipped.
  private boolean needUpdateScanNumForLastQuery = false;

  private long reservedMemoryCostForSchemaTree = 0;
  private boolean releaseSchemaTreeAfterAnalyzing = true;
  private LongConsumer reserveMemoryForSchemaTreeFunc = null;

  private boolean reservingMemoryForSchemaTree = false;

  private boolean resultSetColumnMemoryTrackingEnabled = false;
  private boolean alignByDeviceForResultSetColumnTracking = false;
  private long seriesLimitForResultSetColumnTracking = 0;
  private long seriesOffsetForResultSetColumnTracking = 0;
  private long matchedSourceColumnsForResultSet = 0;
  private long expandedSourceColumnsForResultSet = 0;
  private long sourceColumnMemoryCostForResultSet = 0;
  private long generatedResultSetColumns = 0;
  private long generatedResultSetColumnMemoryCost = 0;
  private long schemaFetchEstimatedMemoryCost = 0;
  private long schemaFetchReservedMemoryCost = 0;
  private long schemaFetchDeserializedColumnCount = 0;

  private boolean userQuery = false;

  /**
   * When true (e.g. SHOW QUERIES), operator and exchange memory may use fallback when pool is
   * insufficient. Set from analysis via {@link #setNeedSetHighestPriority(boolean)}.
   */
  private boolean needSetHighestPriority = false;

  private boolean debug = false;

  @TestOnly
  public MPPQueryContext(QueryId queryId) {
    this.queryId = queryId;
    this.endPointBlackList = ConcurrentHashMap.newKeySet();
    this.memoryReservationManager =
        new NotThreadSafeMemoryReservationManager(queryId, this.getClass().getName());
  }

  @TestOnly
  public MPPQueryContext(
      String sql,
      QueryId queryId,
      SessionInfo session,
      TEndPoint localDataBlockEndpoint,
      TEndPoint localInternalEndpoint) {
    this(sql, queryId, -1, session, localDataBlockEndpoint, localInternalEndpoint);
  }

  public MPPQueryContext(
      String sql,
      QueryId queryId,
      long localQueryId,
      SessionInfo session,
      TEndPoint localDataBlockEndpoint,
      TEndPoint localInternalEndpoint) {
    this(queryId);
    this.sql = sql;
    this.session = session;
    this.localQueryId = localQueryId;
    this.localDataBlockEndpoint = localDataBlockEndpoint;
    this.localInternalEndpoint = localInternalEndpoint;
    this.initResultNodeContext();
  }

  public void setReserveMemoryForSchemaTreeFunc(LongConsumer reserveMemoryForSchemaTreeFunc) {
    this.reserveMemoryForSchemaTreeFunc = reserveMemoryForSchemaTreeFunc;
  }

  public void reserveMemoryForSchemaTree(long memoryCost) {
    if (reserveMemoryForSchemaTreeFunc == null) {
      return;
    }
    schemaFetchEstimatedMemoryCost += memoryCost;
    reservingMemoryForSchemaTree = true;
    try {
      reserveMemoryForSchemaTreeFunc.accept(memoryCost);
    } catch (MemoryNotEnoughException e) {
      throw enrichSchemaFetchMemoryNotEnoughException(e, memoryCost);
    } finally {
      reservingMemoryForSchemaTree = false;
    }
    this.reservedMemoryCostForSchemaTree += memoryCost;
    this.schemaFetchReservedMemoryCost += memoryCost;
  }

  public void setReleaseSchemaTreeAfterAnalyzing(boolean releaseSchemaTreeAfterAnalyzing) {
    this.releaseSchemaTreeAfterAnalyzing = releaseSchemaTreeAfterAnalyzing;
  }

  public boolean releaseSchemaTreeAfterAnalyzing() {
    return releaseSchemaTreeAfterAnalyzing;
  }

  public void releaseMemoryForSchemaTree() {
    if (reservedMemoryCostForSchemaTree <= 0) {
      return;
    }
    this.memoryReservationManager.releaseMemoryCumulatively(reservedMemoryCostForSchemaTree);
    reservedMemoryCostForSchemaTree = 0;
  }

  public void prepareForRetry() {
    this.initResultNodeContext();
    this.releaseAllMemoryReservedForFrontEnd();
    this.resetResultSetColumnMemoryTracking();
  }

  private void initResultNodeContext() {
    this.resultNodeContext = new ResultNodeContext(queryId);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public long getLocalQueryId() {
    return localQueryId;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  /** the max executing time of query in ms. Unit: millisecond */
  public long getTimeOut() {
    return timeOut;
  }

  /** the max executing time of query in ms. Unit: millisecond */
  public void setTimeOut(long timeOut) {
    this.timeOut = timeOut;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public ResultNodeContext getResultNodeContext() {
    return resultNodeContext;
  }

  public TEndPoint getLocalDataBlockEndpoint() {
    return localDataBlockEndpoint;
  }

  public TEndPoint getLocalInternalEndpoint() {
    return localInternalEndpoint;
  }

  public SessionInfo getSession() {
    return session;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void addFailedEndPoint(TEndPoint endPoint) {
    this.endPointBlackList.add(endPoint);
  }

  public Set<TEndPoint> getEndPointBlackList() {
    return endPointBlackList;
  }

  public TRegionReplicaSet getMainFragmentLocatedRegion() {
    return this.mainFragmentLocatedRegion;
  }

  public void setMainFragmentLocatedRegion(TRegionReplicaSet region) {
    this.mainFragmentLocatedRegion = region;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public String getSql() {
    return sql;
  }

  public Set<SchemaLockType> getAcquiredLocks() {
    return acquiredLocks;
  }

  public boolean addAcquiredLock(final SchemaLockType lockType) {
    return acquiredLocks.add(lockType);
  }

  public void generateGlobalTimeFilter(Analysis analysis) {
    this.globalTimeFilter =
        PredicateUtils.convertPredicateToTimeFilter(analysis.getGlobalTimePredicate());
  }

  public Filter getGlobalTimeFilter() {
    // time filter may be stateful, so we need to copy it
    return globalTimeFilter != null ? globalTimeFilter.copy() : null;
  }

  public ZoneId getZoneId() {
    return session.getZoneId();
  }

  public void setExplainAnalyze(boolean explainAnalyze) {
    isExplainAnalyze = explainAnalyze;
  }

  public boolean isExplainAnalyze() {
    return isExplainAnalyze;
  }

  public long getAnalyzeCost() {
    return queryPlanStatistics.getAnalyzeCost();
  }

  public long getDistributionPlanCost() {
    return queryPlanStatistics.getDistributionPlanCost();
  }

  public long getFetchPartitionCost() {
    return queryPlanStatistics.getFetchPartitionCost();
  }

  public long getFetchSchemaCost() {
    return queryPlanStatistics.getFetchSchemaCost();
  }

  public long getLogicalPlanCost() {
    return queryPlanStatistics.getLogicalPlanCost();
  }

  public long getLogicalOptimizationCost() {
    return queryPlanStatistics.getLogicalOptimizationCost();
  }

  public void recordDispatchCost(long dispatchCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.recordDispatchCost(dispatchCost);
  }

  public long getDispatchCost() {
    return queryPlanStatistics.getDispatchCost();
  }

  public void setAnalyzeCost(long analyzeCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.setAnalyzeCost(analyzeCost);
  }

  public void setDistributionPlanCost(long distributionPlanCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.setDistributionPlanCost(distributionPlanCost);
  }

  public void setFetchPartitionCost(long fetchPartitionCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.setFetchPartitionCost(fetchPartitionCost);
  }

  public void setFetchSchemaCost(long fetchSchemaCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.setFetchSchemaCost(fetchSchemaCost);
  }

  public void setLogicalPlanCost(long logicalPlanCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.setLogicalPlanCost(logicalPlanCost);
  }

  public void setLogicalOptimizationCost(long logicalOptimizeCost) {
    if (queryPlanStatistics == null) {
      queryPlanStatistics = new QueryPlanStatistics();
    }
    queryPlanStatistics.setLogicalOptimizationCost(logicalOptimizeCost);
  }

  // region =========== FE memory related, make sure its not called concurrently ===========

  /**
   * This method does not require concurrency control because the query plan is generated in a
   * single-threaded manner.
   */
  public void reserveMemoryForFrontEnd(final long bytes) {
    try {
      this.memoryReservationManager.reserveMemoryCumulatively(bytes);
    } catch (MemoryNotEnoughException e) {
      if (reservingMemoryForSchemaTree) {
        throw e;
      }
      throw enrichResultSetColumnMemoryNotEnoughException(e, bytes);
    }
  }

  public void reserveMemoryForFrontEndImmediately() {
    try {
      this.memoryReservationManager.reserveMemoryImmediately();
    } catch (MemoryNotEnoughException e) {
      if (reservingMemoryForSchemaTree) {
        throw e;
      }
      throw enrichResultSetColumnMemoryNotEnoughException(e, extractRequestedMemory(e));
    }
  }

  public void releaseAllMemoryReservedForFrontEnd() {
    this.memoryReservationManager.releaseAllReservedMemory();
  }

  public void releaseMemoryReservedForFrontEnd(final long bytes) {
    this.memoryReservationManager.releaseMemoryCumulatively(bytes);
  }

  public void initResultSetColumnMemoryTracking(
      long seriesLimit, long seriesOffset, boolean alignByDevice) {
    resetResultSetColumnMemoryTracking();
    resultSetColumnMemoryTrackingEnabled = true;
    seriesLimitForResultSetColumnTracking = seriesLimit;
    seriesOffsetForResultSetColumnTracking = seriesOffset;
    alignByDeviceForResultSetColumnTracking = alignByDevice;
  }

  public void recordMatchedSourceColumnsForResultSet(long columnCount) {
    if (resultSetColumnMemoryTrackingEnabled && columnCount > 0) {
      matchedSourceColumnsForResultSet += columnCount;
    }
  }

  public void recordExpandedSourceColumnForResultSet(long memoryCost) {
    if (!resultSetColumnMemoryTrackingEnabled) {
      return;
    }
    expandedSourceColumnsForResultSet++;
    sourceColumnMemoryCostForResultSet += Math.max(memoryCost, 0);
  }

  public void recordGeneratedResultSetColumn(long memoryCost) {
    if (!resultSetColumnMemoryTrackingEnabled) {
      return;
    }
    generatedResultSetColumns++;
    generatedResultSetColumnMemoryCost += Math.max(memoryCost, 0);
  }

  public void recordSchemaFetchDeserializedColumns(long columnCount) {
    if (columnCount > 0) {
      schemaFetchDeserializedColumnCount += columnCount;
    }
  }

  private void resetResultSetColumnMemoryTracking() {
    resultSetColumnMemoryTrackingEnabled = false;
    alignByDeviceForResultSetColumnTracking = false;
    seriesLimitForResultSetColumnTracking = 0;
    seriesOffsetForResultSetColumnTracking = 0;
    matchedSourceColumnsForResultSet = 0;
    expandedSourceColumnsForResultSet = 0;
    sourceColumnMemoryCostForResultSet = 0;
    generatedResultSetColumns = 0;
    generatedResultSetColumnMemoryCost = 0;
    schemaFetchEstimatedMemoryCost = 0;
    schemaFetchReservedMemoryCost = 0;
    schemaFetchDeserializedColumnCount = 0;
  }

  private MemoryNotEnoughException enrichResultSetColumnMemoryNotEnoughException(
      MemoryNotEnoughException e, long requestedBytes) {
    if (!resultSetColumnMemoryTrackingEnabled
        || (matchedSourceColumnsForResultSet == 0
            && expandedSourceColumnsForResultSet == 0
            && generatedResultSetColumns == 0)) {
      return e;
    }

    long freeBytes = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    long shortageBytes =
        requestedBytes > 0 && requestedBytes > freeBytes ? requestedBytes - freeBytes : -1;
    long exceededColumns = estimateExceededColumns(freeBytes, requestedBytes);

    return new MemoryNotEnoughException(
        String.format(
            Locale.ROOT,
            RESULT_SET_COLUMN_METADATA_MEMORY_NOT_ENOUGH,
            matchedSourceColumnsForResultSet,
            expandedSourceColumnsForResultSet,
            generatedResultSetColumns,
            exceededColumns > 0
                ? String.format(
                    Locale.ROOT, RESULT_SET_COLUMNS_EXCEED_MEMORY_CAPACITY, exceededColumns)
                : "",
            formatSeriesPaginationForDiagnostics(),
            alignByDeviceForResultSetColumnTracking
                ? ""
                : USE_ALIGN_BY_DEVICE_TO_REDUCE_RESULT_COLUMNS,
            shortageBytes > 0
                ? String.format(Locale.ROOT, BY_AT_LEAST_MEMORY_SIZE, formatBytes(shortageBytes))
                : FOR_QUERY_ENGINE_OPERATOR_MEMORY_POOL,
            formatBytes(sourceColumnMemoryCostForResultSet),
            formatBytes(generatedResultSetColumnMemoryCost),
            formatBytes(requestedBytes),
            formatBytes(freeBytes),
            e.getMessage()));
  }

  private MemoryNotEnoughException enrichSchemaFetchMemoryNotEnoughException(
      MemoryNotEnoughException e, long requestedBytes) {
    long freeBytes = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    if (!resultSetColumnMemoryTrackingEnabled && schemaFetchDeserializedColumnCount == 0) {
      return e;
    }

    long shortageBytes =
        requestedBytes > 0 && requestedBytes > freeBytes ? requestedBytes - freeBytes : -1;
    long exceededColumns = estimateExceededSchemaFetchColumns(freeBytes, requestedBytes);

    return new MemoryNotEnoughException(
        String.format(
            Locale.ROOT,
            SCHEMA_FETCH_METADATA_MEMORY_NOT_ENOUGH,
            schemaFetchDeserializedColumnCount,
            exceededColumns > 0
                ? String.format(
                    Locale.ROOT, SCHEMA_FETCH_COLUMNS_EXCEED_MEMORY_CAPACITY, exceededColumns)
                : "",
            formatSeriesPaginationForDiagnostics(),
            alignByDeviceForResultSetColumnTracking
                ? ""
                : USE_ALIGN_BY_DEVICE_TO_REDUCE_RESULT_COLUMNS,
            shortageBytes > 0
                ? String.format(Locale.ROOT, BY_AT_LEAST_MEMORY_SIZE, formatBytes(shortageBytes))
                : FOR_QUERY_ENGINE_OPERATOR_MEMORY_POOL,
            formatBytes(schemaFetchEstimatedMemoryCost),
            formatBytes(schemaFetchReservedMemoryCost),
            formatBytes(requestedBytes),
            formatBytes(freeBytes),
            e.getMessage()));
  }

  private long estimateExceededColumns(long freeBytes, long requestedBytes) {
    long avgColumnMemory;
    if (expandedSourceColumnsForResultSet > 0 && sourceColumnMemoryCostForResultSet > 0) {
      avgColumnMemory =
          Math.max(1, sourceColumnMemoryCostForResultSet / expandedSourceColumnsForResultSet);
    } else if (requestedBytes > 0) {
      avgColumnMemory = requestedBytes;
    } else {
      return -1;
    }
    long estimatedCapacity =
        (sourceColumnMemoryCostForResultSet + Math.max(freeBytes, 0)) / avgColumnMemory;
    long columnsToCompare =
        Math.max(matchedSourceColumnsForResultSet, expandedSourceColumnsForResultSet + 1);
    return Math.max(0, columnsToCompare - estimatedCapacity);
  }

  private long estimateExceededSchemaFetchColumns(long freeBytes, long requestedBytes) {
    if (schemaFetchDeserializedColumnCount <= 0) {
      return -1;
    }

    long avgColumnMemory;
    long columnsToCompare = schemaFetchDeserializedColumnCount;
    if (schemaFetchReservedMemoryCost > 0) {
      avgColumnMemory =
          Math.max(
              1, divideCeil(schemaFetchReservedMemoryCost, schemaFetchDeserializedColumnCount));
      if (requestedBytes > 0) {
        columnsToCompare += Math.max(1, divideCeil(requestedBytes, avgColumnMemory));
      }
    } else if (requestedBytes > 0) {
      avgColumnMemory = Math.max(1, divideCeil(requestedBytes, schemaFetchDeserializedColumnCount));
    } else {
      return -1;
    }

    long estimatedCapacity =
        (schemaFetchReservedMemoryCost + Math.max(freeBytes, 0)) / avgColumnMemory;
    return Math.max(0, columnsToCompare - estimatedCapacity);
  }

  private static long divideCeil(long dividend, long divisor) {
    return dividend / divisor + (dividend % divisor == 0 ? 0 : 1);
  }

  private String formatSeriesPaginationForDiagnostics() {
    return String.format(
        Locale.ROOT,
        SERIES_PAGINATION_FOR_DIAGNOSTICS,
        seriesLimitForResultSetColumnTracking > 0
            ? String.format(Locale.ROOT, "%,d", seriesLimitForResultSetColumnTracking)
            : NOT_SET,
        seriesOffsetForResultSetColumnTracking);
  }

  private static long extractRequestedMemory(MemoryNotEnoughException e) {
    String message = e.getMessage();
    if (message == null) {
      return -1;
    }
    String marker = "the memory requested this time is ";
    int start = message.indexOf(marker);
    if (start < 0) {
      return -1;
    }
    start += marker.length();
    int end = message.indexOf('B', start);
    if (end < 0) {
      return -1;
    }
    try {
      return Long.parseLong(message.substring(start, end));
    } catch (NumberFormatException ignored) {
      return -1;
    }
  }

  private static String formatBytes(long bytes) {
    if (bytes < 0) {
      return UNKNOWN;
    }
    if (bytes < 1024) {
      return bytes + " B";
    }
    double value = bytes;
    String[] units = {"B", "KB", "MB", "GB", "TB"};
    int unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }
    return String.format(Locale.ROOT, "%.2f %s (%d B)", value, units[unitIndex], bytes);
  }

  public boolean useSampledAvgTimeseriesOperandMemCost() {
    return numsOfSampledTimeseriesOperand >= minSizeToUseSampledTimeseriesOperandMemCost;
  }

  public long getAvgTimeseriesOperandMemCost() {
    return (long) avgTimeseriesOperandMemCost;
  }

  public void calculateAvgTimeseriesOperandMemCost(long current) {
    numsOfSampledTimeseriesOperand++;
    avgTimeseriesOperandMemCost +=
        (current - avgTimeseriesOperandMemCost) / numsOfSampledTimeseriesOperand;
  }

  // endregion

  public boolean needUpdateScanNumForLastQuery() {
    return needUpdateScanNumForLastQuery;
  }

  public void setNeedUpdateScanNumForLastQuery(boolean needUpdateScanNumForLastQuery) {
    this.needUpdateScanNumForLastQuery = needUpdateScanNumForLastQuery;
  }

  public boolean isUserQuery() {
    return userQuery;
  }

  public void setUserQuery(boolean userQuery) {
    this.userQuery = userQuery;
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public boolean needSetHighestPriority() {
    return needSetHighestPriority;
  }

  public void setNeedSetHighestPriority(boolean needSetHighestPriority) {
    this.needSetHighestPriority = needSetHighestPriority;
  }

  public String getClientHostName() {
    if (session == null || session.getCliHostname() == null) {
      return "UNKNOWN";
    }
    return session.getCliHostname();
  }
}
