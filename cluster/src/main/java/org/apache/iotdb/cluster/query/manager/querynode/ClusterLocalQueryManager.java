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
package org.apache.iotdb.cluster.query.manager.querynode;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.query.manager.coordinatornode.IClusterRpcQueryManager;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;

public class ClusterLocalQueryManager implements IClusterLocalQueryManager {

  /**
   * Key is task id which is assigned by coordinator node, value is job id which is assigned by
   * query node(local).
   */
  private static final ConcurrentHashMap<String, Long> TASK_ID_MAP_JOB_ID = new ConcurrentHashMap<>();

  /**
   * Key is job id, value is manager of a client query.
   */
  private static final ConcurrentHashMap<Long, ClusterLocalSingleQueryManager> SINGLE_QUERY_MANAGER_MAP = new ConcurrentHashMap<>();


  private ClusterLocalQueryManager() {
  }

  @Override
  public void createQueryDataSet(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException, FileNodeManagerException, PathErrorException, ProcessorException, QueryFilterOptimizationException {
    long jobId = QueryResourceManager.getInstance().assignJobId();
    String taskId = request.getTaskId();
    TASK_ID_MAP_JOB_ID.put(taskId, jobId);
    ClusterLocalSingleQueryManager localQueryManager = new ClusterLocalSingleQueryManager(jobId);
    localQueryManager.createSeriesReader(request, response);
    SINGLE_QUERY_MANAGER_MAP.put(jobId, localQueryManager);
  }

  @Override
  public void readBatchData(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException {
    long jobId = TASK_ID_MAP_JOB_ID.get(request.getTaskId());
    SINGLE_QUERY_MANAGER_MAP.get(jobId).readBatchData(request, response);
  }

  @Override
  public void readBatchDataByTimestamp(QuerySeriesDataByTimestampRequest request,
      QuerySeriesDataByTimestampResponse response)
      throws IOException {
    long jobId = TASK_ID_MAP_JOB_ID.get(request.getTaskId());
    SINGLE_QUERY_MANAGER_MAP.get(jobId).readBatchDataByTimestamp(request, response);
  }

  @Override
  public void close(String taskId) throws FileNodeManagerException {
    if (TASK_ID_MAP_JOB_ID.containsKey(taskId)) {
      SINGLE_QUERY_MANAGER_MAP.remove(TASK_ID_MAP_JOB_ID.remove(taskId)).close();
    }
  }

  public static final ClusterLocalQueryManager getInstance() {
    return ClusterLocalQueryManager.ClusterLocalQueryManagerHolder.INSTANCE;
  }

  private static class ClusterLocalQueryManagerHolder {

    private static final ClusterLocalQueryManager INSTANCE = new ClusterLocalQueryManager();

    private ClusterLocalQueryManagerHolder() {

    }
  }
}
