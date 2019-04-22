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
package org.apache.iotdb.cluster.query.manager.coordinatornode;

import com.alipay.sofa.jraft.util.OnlyForTest;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;

public class ClusterRpcQueryManager implements IClusterRpcQueryManager {

  /**
   * Key is job id, value is task id.
   */
  private static final ConcurrentHashMap<Long, String> JOB_ID_MAP_TASK_ID = new ConcurrentHashMap<>();

  /**
   * Key is task id, value is manager of a client query.
   */
  private static final ConcurrentHashMap<String, ClusterRpcSingleQueryManager> SINGLE_QUERY_MANAGER_MAP = new ConcurrentHashMap<>();

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();

  /**
   * Local address
   */
  private static final String LOCAL_ADDR = String
      .format("%s:%d", CLUSTER_CONFIG.getIp(), CLUSTER_CONFIG.getPort());

  @Override
  public void addSingleQuery(long jobId, QueryPlan physicalPlan) {
    String taskId = createJobId(jobId);
    JOB_ID_MAP_TASK_ID.put(jobId, taskId);
    SINGLE_QUERY_MANAGER_MAP.put(taskId, new ClusterRpcSingleQueryManager(taskId, physicalPlan));
  }

  @Override
  public String createJobId(long jobId) {
    return String.format("%s:%d", LOCAL_ADDR, jobId);
  }

  @Override
  public ClusterRpcSingleQueryManager getSingleQuery(long jobId) {
    return SINGLE_QUERY_MANAGER_MAP.get(JOB_ID_MAP_TASK_ID.get(jobId));
  }

  @Override
  public ClusterRpcSingleQueryManager getSingleQuery(String taskId) {
    return SINGLE_QUERY_MANAGER_MAP.get(taskId);
  }

  @Override
  public void releaseQueryResource(long jobId) throws RaftConnectionException {
    if (JOB_ID_MAP_TASK_ID.containsKey(jobId)) {
      SINGLE_QUERY_MANAGER_MAP.remove(JOB_ID_MAP_TASK_ID.remove(jobId)).releaseQueryResource();
    }
  }

  @OnlyForTest
  public static ConcurrentHashMap<Long, String> getJobIdMapTaskId() {
    return JOB_ID_MAP_TASK_ID;
  }

  private ClusterRpcQueryManager() {
  }

  public static final ClusterRpcQueryManager getInstance() {
    return ClusterRpcQueryManagerHolder.INSTANCE;
  }

  private static class ClusterRpcQueryManagerHolder {

    private static final ClusterRpcQueryManager INSTANCE = new ClusterRpcQueryManager();

    private ClusterRpcQueryManagerHolder() {

    }
  }

}
