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
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.time.ZoneId;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is used to record the context of a query including QueryId, query statement, session
 * info and so on.
 */
public class MPPQueryContext {
  private String sql;
  private QueryId queryId;
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

  private int acquiredLockNum;

  public MPPQueryContext(QueryId queryId) {
    this.queryId = queryId;
    this.endPointBlackList = new LinkedList<>();
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

  public void prepareForRetry() {
    this.initResultNodeContext();
  }

  private void initResultNodeContext() {
    this.resultNodeContext = new ResultNodeContext(queryId);
  }

  public QueryId getQueryId() {
    return queryId;
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

  public int getAcquiredLockNum() {
    return acquiredLockNum;
  }

  public void addAcquiredLockNum() {
    acquiredLockNum++;
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
}
