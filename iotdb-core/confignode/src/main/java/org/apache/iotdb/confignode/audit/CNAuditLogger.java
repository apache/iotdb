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

package org.apache.iotdb.confignode.audit;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.audit.DataNodeWriteAuditLogHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.mpp.rpc.thrift.TAuditLogReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CNAuditLogger extends AbstractAuditLogger {
  private static final Logger logger = LoggerFactory.getLogger(CNAuditLogger.class);
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  protected final IManager configManager;

  public CNAuditLogger(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public void log(AuditLogFields auditLogFields, String log) {
    if (!IS_AUDIT_LOG_ENABLED) {
      return;
    }
    if (!checkBeforeLog(auditLogFields)) {
      return;
    }
    // find database "__audit"'s data_region
    List<TRegionReplicaSet> auditReplicaSets =
        configManager
            .getPartitionManager()
            .getAllReplicaSets("root.__audit", TConsensusGroupType.DataRegion);
    if (auditReplicaSets.isEmpty()) {
      logger.warn("Database {} does not exist.", "root.__audit");
      return;
    }
    TConsensusGroupId regionId = auditReplicaSets.get(0).getRegionId();
    // use ConfigManager.getLoadManager().getLoadCache().getRegionLeaderMap() to get regionLeaderId
    TDataNodeLocation regionLeader = configManager.getRegionLeaderLocation(regionId);
    TAuditLogReq req =
        new TAuditLogReq(
            auditLogFields.getUsername(),
            auditLogFields.getUserId(),
            auditLogFields.getCliHostname(),
            auditLogFields.getAuditType().toString(),
            auditLogFields.getOperationType().toString(),
            auditLogFields.getPrivilegeType().toString(),
            auditLogFields.isResult(),
            auditLogFields.getDatabase(),
            auditLogFields.getSqlString(),
            log,
            CONF.getConfigNodeId());
    // refer the implementation of HeartbeatService.pingRegisteredDataNode(). By appending a new
    // writeAudtiLog() interface in AsyncDataNodeHeartbeatClientPool, the main thread is not
    // required to wait until the write audit log request to be complete.
    AsyncDataNodeHeartbeatClientPool.getInstance()
        .writeAuditLog(
            regionLeader.getInternalEndPoint(),
            req,
            new DataNodeWriteAuditLogHandler(regionLeader.getDataNodeId()));
  }
}
