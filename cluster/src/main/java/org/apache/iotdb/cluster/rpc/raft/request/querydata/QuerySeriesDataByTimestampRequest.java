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

import java.util.List;
import org.apache.iotdb.cluster.rpc.raft.request.BasicQueryRequest;

public class QuerySeriesDataByTimestampRequest extends BasicQueryRequest {

  private static final long serialVersionUID = 4942493162179531133L;
  /**
   * Rounds number of query
   */
  private long queryRounds;

  /**
   * Unique task id which is assigned in coordinator node
   */
  private String taskId;

  /**
   * Batch valid timestamp
   */
  private List<Long> batchTimestamp;

  /**
   * Series to fetch data from remote query node
   */
  private List<String> fetchDataSeries;

  private QuerySeriesDataByTimestampRequest(String groupID) {
    super(groupID);
  }

  public static QuerySeriesDataByTimestampRequest createRequest(String groupId, long queryRounds, String taskId, List<Long> batchTimestamp, List<String> fetchDataSeries){
    QuerySeriesDataByTimestampRequest request = new QuerySeriesDataByTimestampRequest(groupId);
    request.queryRounds = queryRounds;
    request.taskId = taskId;
    request.batchTimestamp = batchTimestamp;
    request.fetchDataSeries = fetchDataSeries;
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

  public List<Long> getBatchTimestamp() {
    return batchTimestamp;
  }

  public void setBatchTimestamp(List<Long> batchTimestamp) {
    this.batchTimestamp = batchTimestamp;
  }

  public List<String> getFetchDataSeries() {
    return fetchDataSeries;
  }

  public void setFetchDataSeries(List<String> fetchDataSeries) {
    this.fetchDataSeries = fetchDataSeries;
  }
}
