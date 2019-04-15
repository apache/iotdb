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

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class ClusterRpcQueryManager implements IClusterQueryManager{

  Map<Long, Map<String, PhysicalPlan>> selectPathPlan = new HashMap<>();
  Map<Long, Map<String, PhysicalPlan>> filterPathPlan = new HashMap<>();

  private ClusterRpcQueryManager(){
  }

  @Override
  public void registerQuery(Long jobId, PhysicalPlan plan) {

  }

  @Override
  public PhysicalPlan getSelectPathPhysicalPlan(Long jobId, String fullPath) {
    return selectPathPlan.get(jobId).get(fullPath);
  }

  @Override
  public PhysicalPlan getFilterPathPhysicalPlan(Long jobId, String fullPath) {
    return filterPathPlan.get(jobId).get(fullPath);
  }

  @Override
  public void remove(Long jobId) {
    selectPathPlan.remove(jobId);
    filterPathPlan.remove(jobId);
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
