/*
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

package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.query.manage.ClusterSessionManager;
import org.apache.iotdb.cluster.server.basic.ClusterServiceProvider;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.thrift.impl.TSServiceImpl;

/**
 * ClusterTSServiceImpl is the cluster version of TSServiceImpl, which is responsible for the
 * processing of the user requests (sqls and session api). It inherits the basic procedures from
 * TSServiceImpl, but redirect the queries of data and metadata to a MetaGroupMember of the local
 * node.
 */
public class ClusterTSServiceImpl extends TSServiceImpl {

  private final ClusterServiceProvider clusterServiceProvider;

  public ClusterTSServiceImpl() {
    clusterServiceProvider = (ClusterServiceProvider) IoTDB.serviceProvider;
  }

  /** Redirect the plan to the local Coordinator so that it will be processed cluster-wide. */
  @Override
  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    return clusterServiceProvider.executeNonQueryPlan(plan);
  }

  @Override
  public SessionManager getSessionManager() {
    return ClusterSessionManager.getInstance();
  }
}
