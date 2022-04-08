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
package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;

/**
 * This class is used to record the context of a query including QueryId, query statement, session
 * info and so on
 */
public class MPPQueryContext {
  private String sql;
  private QueryId queryId;
  private SessionInfo session;
  private QueryType queryType;

  private Endpoint hostEndpoint;
  private ResultNodeContext resultNodeContext;

  public MPPQueryContext(QueryId queryId) {
    this.queryId = queryId;
  }

  public MPPQueryContext(
      String sql, QueryId queryId, SessionInfo session, QueryType type, Endpoint hostEndpoint) {
    this.sql = sql;
    this.queryId = queryId;
    this.session = session;
    this.queryType = type;
    this.hostEndpoint = hostEndpoint;
    this.resultNodeContext = new ResultNodeContext(queryId);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public Endpoint getHostEndpoint() {
    return hostEndpoint;
  }

  public ResultNodeContext getResultNodeContext() {
    return resultNodeContext;
  }
}
