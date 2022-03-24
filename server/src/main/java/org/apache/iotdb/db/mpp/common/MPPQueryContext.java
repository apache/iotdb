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

import org.apache.iotdb.db.mpp.sql.metadata.MetadataResponse;
import org.apache.iotdb.db.mpp.sql.metadata.PathPatternTree;

/**
 * This class is used to record the context of a query including QueryId, query statement, session
 * info and so on
 */
public class MPPQueryContext {
  private String sql;
  private QueryId queryId;
  private SessionInfo session;

  public MPPQueryContext() {}

  public MPPQueryContext(String sql, QueryId queryId, SessionInfo session) {
    this.sql = sql;
    this.queryId = queryId;
    this.session = session;
  }

  public QueryId getQueryId() {
    return queryId;
  }
  private QuerySession session;

  //
  private PathPatternTree pathPatternTree;

  private MetadataResponse response;

  public PathPatternTree getPathPatternTree() {
    return pathPatternTree;
  }

  public void setPathPatternTree(PathPatternTree pathPatternTree) {
    this.pathPatternTree = pathPatternTree;
  }

  public MetadataResponse getResponse() {
    return response;
  }

  public void setResponse(MetadataResponse response) {
    this.response = response;
  }
}
