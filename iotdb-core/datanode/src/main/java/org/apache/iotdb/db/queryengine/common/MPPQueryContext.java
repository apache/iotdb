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
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.statistics.QueryPlanStatistics;
import org.apache.iotdb.db.utils.cte.CteDataStore;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;

/**
 * This class is used to record the context of a query including QueryId, query statement, session
 * info and so on.
 */
public class MPPQueryContext implements IAuditEntity {
  private String sql;
  private final QueryId queryId;

  /** The type of explanation for a query. */
  public enum ExplainType {
    NONE,
    EXPLAIN,
    EXPLAIN_ANALYZE,
  }

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
  // When dispatch FI fails, this structure may be modified concurrently
  private final Set<TEndPoint> endPointBlackList;

  private final TypeProvider typeProvider = new TypeProvider();

  private Filter globalTimeFilter;

  private final Set<SchemaLockType> acquiredLocks = new HashSet<>();

  // Determines the explanation mode for the query:
  // - NONE: Normal query execution without explanation
  // - EXPLAIN: Show the logical and physical query plan without execution
  // - EXPLAIN_ANALYZE: Execute the query and collect detailed execution statistics
  private ExplainType explainType = ExplainType.NONE;
  private boolean isVerbose = false;

  private QueryPlanStatistics queryPlanStatistics = null;

  // To avoid query front-end from consuming too much memory, it needs to reserve memory when
  // constructing some Expression and PlanNode.
  private final MemoryReservationManager memoryReservationManager;

  private static final int MIN_SIZE_TO_USE_SAMPLED_TIMESERIES_OPERAND_MEM_COST = 100;
  private double avgTimeseriesOperandMemCost = 0;
  private int numsOfSampledTimeseriesOperand = 0;
  // When there is no view in a last query and no device exists in multiple regions,
  // the updateScanNum process in distributed planning can be skipped.
  private boolean needUpdateScanNumForLastQuery = false;

  private long reservedMemoryCostForSchemaTree = 0;
  private boolean releaseSchemaTreeAfterAnalyzing = true;
  private LongConsumer reserveMemoryForSchemaTreeFunc = null;

  private boolean userQuery = false;

  private Map<NodeRef<Table>, Query> cteQueries = new HashMap<>();

  // Stores the EXPLAIN/EXPLAIN ANALYZE results for Common Table Expressions (CTEs)
  // Key: CTE table reference
  // Value: Pair containing (max line length of the explain output, list of formatted explain lines)
  // This ensures consistent formatting between the main query and its CTE sub-queries
  private final Map<NodeRef<Table>, Pair<Integer, List<String>>> cteExplainResults =
      new LinkedHashMap<>();
  // Tracks the materialization time cost (in nanoseconds) for each CTE to help optimize query
  // planning
  private final Map<NodeRef<Table>, Long> cteMaterializationCosts = new HashMap<>();

  // Indicates whether this query context is for a sub-query triggered by the main query.
  // Sub-queries are independent queries spawned from the main query (e.g., CTE sub-queries).
  // When true, CTE materialization is skipped as it's handled by the main query context.
  private boolean innerTriggeredQuery = false;

  // Tables in the subquery
  private final Map<NodeRef<Query>, List<Identifier>> subQueryTables = new HashMap<>();

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

  public void setReserveMemoryForSchemaTreeFunc(LongConsumer reserveMemoryForSchemaTreeFunc) {
    this.reserveMemoryForSchemaTreeFunc = reserveMemoryForSchemaTreeFunc;
  }

  public void reserveMemoryForSchemaTree(long memoryCost) {
    if (reserveMemoryForSchemaTreeFunc == null) {
      return;
    }
    reserveMemoryForSchemaTreeFunc.accept(memoryCost);
    this.reservedMemoryCostForSchemaTree += memoryCost;
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
    if (!isInnerTriggeredQuery()) {
      cleanUpCte();
    }
    this.initResultNodeContext();
    this.releaseAllMemoryReservedForFrontEnd();
  }

  private void cleanUpCte() {
    cteQueries.clear();
    cteExplainResults.clear();
    cteMaterializationCosts.clear();
    subQueryTables.clear();
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

  public void setSession(SessionInfo session) {
    this.session = session;
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

  public void setExplainType(ExplainType explainType) {
    this.explainType = explainType;
  }

  public ExplainType getExplainType() {
    return explainType;
  }

  public boolean isExplainAnalyze() {
    return explainType == ExplainType.EXPLAIN_ANALYZE;
  }

  public boolean isExplain() {
    return explainType == ExplainType.EXPLAIN;
  }

  public void setVerbose(boolean verbose) {
    isVerbose = verbose;
  }

  public boolean isVerbose() {
    return isVerbose;
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

  public boolean useSampledAvgTimeseriesOperandMemCost() {
    return numsOfSampledTimeseriesOperand >= MIN_SIZE_TO_USE_SAMPLED_TIMESERIES_OPERAND_MEM_COST;
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

  public Optional<String> getDatabaseName() {
    return session.getDatabaseName();
  }

  public boolean isUserQuery() {
    return userQuery;
  }

  public boolean isQuery() {
    return queryType != QueryType.WRITE;
  }

  public void setUserQuery(boolean userQuery) {
    this.userQuery = userQuery;
  }

  public boolean isInnerTriggeredQuery() {
    return innerTriggeredQuery;
  }

  public void setInnerTriggeredQuery(boolean innerTriggeredQuery) {
    this.innerTriggeredQuery = innerTriggeredQuery;
  }

  public void addCteMaterializationCost(Table table, long cost) {
    cteMaterializationCosts.put(NodeRef.of(table), cost);
  }

  public Map<NodeRef<Table>, Long> getCteMaterializationCosts() {
    return cteMaterializationCosts;
  }

  public void addCteQuery(Table table, Query query) {
    cteQueries.put(NodeRef.of(table), query);
  }

  public Map<NodeRef<Table>, Query> getCteQueries() {
    return cteQueries;
  }

  public CteDataStore getCteDataStore(Table table) {
    Query query = cteQueries.get(NodeRef.of(table));
    if (query == null) {
      return null;
    }
    return query.getCteDataStore();
  }

  public void setCteQueries(Map<NodeRef<Table>, Query> cteQueries) {
    this.cteQueries = cteQueries;
  }

  public void addSubQueryTables(Query query, List<Identifier> tables) {
    subQueryTables.put(NodeRef.of(query), tables);
  }

  public List<Identifier> getTables(Query query) {
    return subQueryTables.getOrDefault(NodeRef.of(query), ImmutableList.of());
  }

  public void addCteExplainResult(Table table, Pair<Integer, List<String>> cteExplainResult) {
    cteExplainResults.put(NodeRef.of(table), cteExplainResult);
  }

  public Map<NodeRef<Table>, Pair<Integer, List<String>>> getCteExplainResults() {
    return cteExplainResults;
  }

  // ================= Authentication Interfaces =========================

  private AuditEventType auditEventType;

  private AuditLogOperation auditLogOperation;

  private List<PrivilegeType> privilegeTypeList;

  private String databaseName;

  private boolean result;

  @Override
  public long getUserId() {
    return session.getUserId();
  }

  @Override
  public String getUsername() {
    return session.getUserName();
  }

  @Override
  public String getCliHostname() {
    return session.getCliHostname();
  }

  @Override
  public AuditEventType getAuditEventType() {
    return auditEventType;
  }

  @Override
  public IAuditEntity setAuditEventType(AuditEventType auditEventType) {
    this.auditEventType = auditEventType;
    return this;
  }

  @Override
  public AuditLogOperation getAuditLogOperation() {
    return auditLogOperation;
  }

  @Override
  public IAuditEntity setAuditLogOperation(AuditLogOperation auditLogOperation) {
    this.auditLogOperation = auditLogOperation;
    return this;
  }

  @Override
  public List<PrivilegeType> getPrivilegeTypes() {
    return privilegeTypeList;
  }

  @Override
  public String getPrivilegeTypeString() {
    return privilegeTypeList.toString();
  }

  @Override
  public IAuditEntity setPrivilegeType(PrivilegeType privilegeType) {
    this.privilegeTypeList = Collections.singletonList(privilegeType);
    return this;
  }

  @Override
  public IAuditEntity setPrivilegeTypes(List<PrivilegeType> privilegeTypes) {
    this.privilegeTypeList = privilegeTypes;
    return this;
  }

  @Override
  public boolean getResult() {
    return result;
  }

  @Override
  public IAuditEntity setResult(boolean result) {
    this.result = result;
    return this;
  }

  @Override
  public String getDatabase() {
    return databaseName;
  }

  @Override
  public IAuditEntity setDatabase(String database) {
    this.databaseName = database;
    return this;
  }

  @Override
  public String getSqlString() {
    return sql;
  }

  @Override
  public IAuditEntity setSqlString(String sqlString) {
    // Do nothing
    return this;
  }

  // ================= Authentication Interfaces =========================
}
