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

package org.apache.iotdb.cluster.server.basic;

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
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.AuditLogUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterServiceProvider extends ServiceProvider {
  private static final Logger logger = LoggerFactory.getLogger(ClusterServiceProvider.class);

  /**
   * The Coordinator of the local node. Through this node queries data and meta from the cluster and
   * performs data manipulations to the cluster.
   */
  private final Coordinator coordinator;

  public ClusterServiceProvider(Coordinator coordinator, MetaGroupMember metaGroupMember)
      throws QueryProcessException {
    super(new ClusterPlanExecutor(metaGroupMember));
    this.coordinator = coordinator;
  }

  /** Redirect the plan to the local Coordinator so that it will be processed cluster-wide. */
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
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

  @Override
  public boolean executeNonQuery(PhysicalPlan plan) {
    AuditLogUtils.writeAuditLog(
        plan.getOperatorName(), String.format("measurements size:%s", plan.getPaths().size()));
    TSStatus tsStatus = executeNonQueryPlan(plan);
    return tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }
}
