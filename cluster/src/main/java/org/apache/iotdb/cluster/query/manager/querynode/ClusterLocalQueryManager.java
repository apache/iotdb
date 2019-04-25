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

import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.InitSeriesReaderRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.InitSeriesReaderResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterLocalQueryManager implements IClusterLocalQueryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterLocalQueryManager.class);

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
  public InitSeriesReaderResponse createQueryDataSet(InitSeriesReaderRequest request)
      throws IOException, FileNodeManagerException, PathErrorException, ProcessorException, QueryFilterOptimizationException {
    long jobId = QueryResourceManager.getInstance().assignJobId();
    String taskId = request.getTaskId();
    TASK_ID_MAP_JOB_ID.put(taskId, jobId);
    ClusterLocalSingleQueryManager localQueryManager = new ClusterLocalSingleQueryManager(jobId,
        new QueryRepeaterTimer(taskId,
            ClusterConstant.QUERY_TIMEOUT_IN_QUERY_NODE));
    SINGLE_QUERY_MANAGER_MAP.put(jobId, localQueryManager);
    return localQueryManager.createSeriesReader(request);
  }

  @Override
  public QuerySeriesDataResponse readBatchData(QuerySeriesDataRequest request)
      throws IOException {
    long jobId = TASK_ID_MAP_JOB_ID.get(request.getTaskId());
    return SINGLE_QUERY_MANAGER_MAP.get(jobId).readBatchData(request);
  }

  @Override
  public QuerySeriesDataByTimestampResponse readBatchDataByTimestamp(
      QuerySeriesDataByTimestampRequest request)
      throws IOException {
    long jobId = TASK_ID_MAP_JOB_ID.get(request.getTaskId());
    return SINGLE_QUERY_MANAGER_MAP.get(jobId).readBatchDataByTimestamp(request);
  }

  @Override
  public void close(String taskId) throws FileNodeManagerException {
    if (TASK_ID_MAP_JOB_ID.containsKey(taskId)) {
      SINGLE_QUERY_MANAGER_MAP.remove(TASK_ID_MAP_JOB_ID.remove(taskId)).close();
    }
  }

  @Override
  public ClusterLocalSingleQueryManager getSingleQuery(String taskId) {
    long jobId = TASK_ID_MAP_JOB_ID.get(taskId);
    return SINGLE_QUERY_MANAGER_MAP.get(jobId);
  }

  public static final ClusterLocalQueryManager getInstance() {
    return ClusterLocalQueryManager.ClusterLocalQueryManagerHolder.INSTANCE;
  }

  private static class ClusterLocalQueryManagerHolder {

    private static final ClusterLocalQueryManager INSTANCE = new ClusterLocalQueryManager();

    private ClusterLocalQueryManagerHolder() {

    }
  }

  @Override
  public Map<String, Integer> getAllReadUsage() {
    Map<String, Integer> readerUsageMap = new HashMap<>();
    SINGLE_QUERY_MANAGER_MAP.values().forEach(singleQueryManager -> {
      String groupId = singleQueryManager.getGroupId();
      readerUsageMap.put(groupId, readerUsageMap.getOrDefault(groupId, 0) + 1);
    });
    return readerUsageMap;
  }

  @OnlyForTest
  public static ConcurrentHashMap<String, Long> getTaskIdMapJobId() {
    return TASK_ID_MAP_JOB_ID;
  }

  @OnlyForTest
  public static ConcurrentHashMap<Long, ClusterLocalSingleQueryManager> getSingleQueryManagerMap() {
    return SINGLE_QUERY_MANAGER_MAP;
  }

  public class QueryRepeaterTimer extends RepeatedTimer {

    private String taskId;

    public QueryRepeaterTimer(String taskId, int timeoutMs) {
      super(taskId, timeoutMs);
      this.taskId = taskId;
    }

    @Override
    protected void onTrigger() {
      try {
        close(taskId);
      } catch (FileNodeManagerException e) {
        LOGGER.error(e.getMessage());
      }
    }
  }
}
