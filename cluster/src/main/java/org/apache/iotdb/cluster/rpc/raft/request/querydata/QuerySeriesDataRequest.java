/**
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
package org.apache.iotdb.cluster.rpc.raft.request.querydata;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.request.BasicQueryRequest;

/**
 * Read batch data from series reader from remote query node.
 */
public class QuerySeriesDataRequest extends BasicQueryRequest {

  private static final long serialVersionUID = 7132891920951977625L;

  /**
   * Rounds number of query
   */
  private long queryRounds;

  /**
   * Unique task id which is assigned in coordinator node
   */
  private String taskId;

  /**
   * Series type
   */
  private PathType pathType;

  /**
   * Key is series type, value is series list
   */
  private List<String> seriesPaths = new ArrayList<>();

  private QuerySeriesDataRequest(String groupID, String taskId) {
    super(groupID);
    this.taskId = taskId;
  }

  public static QuerySeriesDataRequest createFetchDataRequest(String groupId, String taskId,
      PathType pathType, List<String> seriesPaths, long queryRounds) {
    QuerySeriesDataRequest request = new QuerySeriesDataRequest(groupId, taskId);
    request.pathType = pathType;
    request.seriesPaths = seriesPaths;
    request.queryRounds = queryRounds;
    return request;
  }

  public long getQueryRounds() {
    return queryRounds;
  }

  public void setQueryRounds(long queryRounds) {
    this.queryRounds = queryRounds;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public PathType getPathType() {
    return pathType;
  }

  public void setPathType(PathType pathType) {
    this.pathType = pathType;
  }

  public List<String> getSeriesPaths() {
    return seriesPaths;
  }

  public void setSeriesPaths(List<String> seriesPaths) {
    this.seriesPaths = seriesPaths;
  }
}
