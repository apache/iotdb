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
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.statistics.QueryPlanStatistics;

import org.apache.tsfile.read.filter.basic.Filter;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
  private long timeOut;
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
  private final List<TEndPoint> endPointBlackList;

  private final TypeProvider typeProvider = new TypeProvider();

  private Filter globalTimeFilter;

  private final Set<SchemaLockType> acquiredLocks = new HashSet<>();

  private boolean isExplainAnalyze = false;

  QueryPlanStatistics queryPlanStatistics = null;

  // To avoid query front-end from consuming too much memory, it needs to reserve memory when
  // constructing some Expression and PlanNode.
  private final MemoryReservationManager memoryReservationManager;

  private boolean isTableQuery = false;

  public MPPQueryContext(QueryId queryId) {
    this.queryId = queryId;
    this.endPointBlackList = new LinkedList<>();
    this.memoryReservationManager =
        new NotThreadSafeMemoryReservationManager(queryId, this.getClass().getName());
  }

  // TODO too many callers just pass a null SessionInfo which should be forbidden
  public MPPQueryContext(
      String sql,
      QueryId queryId,
      SessionInfo session,
      TEndPoint localDataBlockEndpoint,
      TEndPoint localInternalEndpoint) {
    this(queryId);
    this.sql = sql;
    this.session = session;
    this.localDataBlockEndpoint = localDataBlockEndpoint;
    this.localInternalEndpoint = localInternalEndpoint;
    this.initResultNodeContext();
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

  public void prepareForRetry() {
    this.initResultNodeContext();
    this.releaseAllMemoryReservedForFrontEnd();
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

  public long getTimeOut() {
    return timeOut;
  }

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

  public List<TEndPoint> getEndPointBlackList() {
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

  // used for tree model
  public void generateGlobalTimeFilter(Analysis analysis) {
    this.globalTimeFilter =
        PredicateUtils.convertPredicateToTimeFilter(analysis.getGlobalTimePredicate());
  }

  // used for table model
  public void setGlobalTimeFilter(Filter globalTimeFilter) {
    this.globalTimeFilter = globalTimeFilter;
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
    if (queryPlanStatistics == null) {
      return 0;
    }
    return queryPlanStatistics.getFetchPartitionCost();
  }

  public long getFetchSchemaCost() {
    if (queryPlanStatistics == null) {
      return 0;
    }
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
    this.memoryReservationManager.reserveMemoryCumulatively(bytes);
  }

  public void reserveMemoryForFrontEndImmediately() {
    this.memoryReservationManager.reserveMemoryImmediately();
  }

  public void releaseAllMemoryReservedForFrontEnd() {
    this.memoryReservationManager.releaseAllReservedMemory();
  }

  public void releaseMemoryReservedForFrontEnd(final long bytes) {
    this.memoryReservationManager.releaseMemoryCumulatively(bytes);
  }

  // endregion

  public boolean isTableQuery() {
    return isTableQuery;
  }

  public void setTableQuery(boolean tableQuery) {
    isTableQuery = tableQuery;
  }

  public Optional<String> getDatabaseName() {
    return session.getDatabaseName();
  }
}
