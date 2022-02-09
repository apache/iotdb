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

import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.ClusterSessionManager;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineReadonlyException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.thrift.impl.TSServiceImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClusterTSServiceImpl is the cluster version of TSServiceImpl, which is responsible for the
 * processing of the user requests (sqls and session api). It inherits the basic procedures from
 * TSServiceImpl, but redirect the queries of data and metadata to a MetaGroupMember of the local
 * node.
 */
public class ClusterTSServiceImpl extends TSServiceImpl {

  private static final Logger logger = LoggerFactory.getLogger(ClusterTSServiceImpl.class);
  /**
   * The Coordinator of the local node. Through this node queries data and meta from the cluster and
   * performs data manipulations to the cluster.
   */
  private Coordinator coordinator;

  public ClusterTSServiceImpl() throws QueryProcessException {}

  public void setExecutor(MetaGroupMember metaGroupMember) throws QueryProcessException {
    executor = new ClusterPlanExecutor(metaGroupMember);
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /** Redirect the plan to the local Coordinator so that it will be processed cluster-wide. */
  @Override
  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      plan.checkIntegrity();
      if (!(plan instanceof SetSystemModePlan)
          && !(plan instanceof FlushPlan)
          && IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
        return RpcUtils.getStatus(
            TSStatusCode.READ_ONLY_SYSTEM_ERROR, StorageEngineReadonlyException.ERROR_MESSAGE);
      }
    } catch (QueryProcessException e) {
      logger.warn("Illegal plan detected： {}", plan);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }

    return coordinator.executeNonQueryPlan(plan);
  }

  /**
   * Generate and cache a QueryContext using "queryId". In the distributed version, the QueryContext
   * is a RemoteQueryContext.
   *
   * @return a RemoteQueryContext using queryId
   */
  @Override
  public QueryContext genQueryContext(
      long queryId, boolean debug, long startTime, String statement, long timeout) {
    RemoteQueryContext context =
        new RemoteQueryContext(queryId, debug, startTime, statement, timeout);
    ClusterSessionManager.getInstance().putContext(queryId, context);
    return context;
  }
}
