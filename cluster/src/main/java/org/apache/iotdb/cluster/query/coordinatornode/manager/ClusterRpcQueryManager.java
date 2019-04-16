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
package org.apache.iotdb.cluster.query.coordinatornode.manager;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;

/**
 * Manage all query in cluster
 */
public class ClusterRpcQueryManager{

  /**
   * Key is job id, value is manager of a client query.
   */
  ConcurrentHashMap<Long, ClusterRpcSingleQueryManager> singleQueryManagerMap = new ConcurrentHashMap<>();

  /**
   * Add a query
   */
  public void addSingleQuery(long jobId, QueryPlan physicalPlan){
    singleQueryManagerMap.put(jobId, new ClusterRpcSingleQueryManager(jobId, physicalPlan));
  }

  /**
   * Get query manager by group id
   */
  public ClusterRpcSingleQueryManager getSingleQuery(long jobId) {
    return singleQueryManagerMap.get(jobId);
  }

  public void releaseQueryResource(long jobId){
    if(singleQueryManagerMap.containsKey(jobId)){
     singleQueryManagerMap.remove(jobId).releaseQueryResource();
    }
  }

  private ClusterRpcQueryManager(){
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
