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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.request.BasicQueryRequest;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Initially create corresponding series readers in remote query node
 */
public class InitSeriesReaderRequest extends BasicQueryRequest {

  private static final long serialVersionUID = 8374330837710097285L;

  /**
   * Unique task id which is assigned in coordinator node
   */
  private String taskId;

  /**
   * Key is series type, value is query plan
   */
  private Map<PathType, QueryPlan> allQueryPlan = new EnumMap<>(PathType.class);

  /**
   * Represent all filter of leaf node in filter tree while executing a query with value filter.
   */
  private List<Filter> filterList = new ArrayList<>();


  private InitSeriesReaderRequest(String groupID, String taskId) {
    super(groupID);
    this.taskId = taskId;
  }

  public static InitSeriesReaderRequest createInitialQueryRequest(String groupId, String taskId, int readConsistencyLevel,
      Map<PathType, QueryPlan> allQueryPlan, List<Filter> filterList){
    InitSeriesReaderRequest request = new InitSeriesReaderRequest(groupId, taskId);
    request.setReadConsistencyLevel(readConsistencyLevel);
    request.allQueryPlan = allQueryPlan;
    request.filterList = filterList;
    return request;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public Map<PathType, QueryPlan> getAllQueryPlan() {
    return allQueryPlan;
  }

  public void setAllQueryPlan(
      Map<PathType, QueryPlan> allQueryPlan) {
    this.allQueryPlan = allQueryPlan;
  }

  public List<Filter> getFilterList() {
    return filterList;
  }

  public void setFilterList(List<Filter> filterList) {
    this.filterList = filterList;
  }
}
