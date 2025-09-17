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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.response.partition.GetRegionIdResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.mpp.rpc.thrift.TAuditLogReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CNAuditLogger extends AbstractAuditLogger {
  private static final Logger logger = LoggerFactory.getLogger(CNAuditLogger.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String AUDIT_LOG_DEVICE = "root.__audit.log.%s.%s";

  protected final IManager configManager;

  public CNAuditLogger(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public void log(AuditLogFields auditLogFields, String log) {
    // find database "__audit"'s data_region
    final GetRegionIdPlan plan = new GetRegionIdPlan(TConsensusGroupType.DataRegion);
    plan.setDatabase("root.__audit");
    plan.setStartTimeSlotId(new TTimePartitionSlot(0));
    plan.setEndTimeSlotId(new TTimePartitionSlot(Long.MAX_VALUE));
    TConsensusGroupId regionId;
    try {
      GetRegionIdResp resp = (GetRegionIdResp) configManager.getConsensusManager().read(plan);
      List<TConsensusGroupId> dataRegionIdList = resp.getDataRegionIdList();
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || dataRegionIdList.isEmpty()) {
        logger.error("Failed to get regionId of database root.__audit: {}", resp.getStatus());
        return;
      }
      regionId = dataRegionIdList.get(0);
    } catch (ConsensusException e) {
      logger.error("Failed to get regionId of database root.__audit", e);
      return;
    }
    // use ConfigManager.getLoadManager().getLoadCache().getRegionLeaderMap() to get regionLeaderId
    Map<TConsensusGroupId, Integer> regionLeaderMap =
        configManager.getLoadManager().getLoadCache().getRegionLeaderMap();
    Integer regionLeaderId = regionLeaderMap.get(regionId);
    Map<Integer, TDataNodeLocation> dataNodeMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    TDataNodeLocation regionLeader = dataNodeMap.get(regionLeaderId);
    Map<Integer, TDataNodeLocation> targetDatanodeMap =
        Collections.singletonMap(regionLeaderId, regionLeader);
    TAuditLogReq req =
        new TAuditLogReq(
            auditLogFields.getUsername(),
            auditLogFields.getCliHostname(),
            auditLogFields.getAuditType().toString(),
            auditLogFields.getOperationType().toString(),
            auditLogFields.getPrivilegeType() == null
                ? null
                : auditLogFields.getPrivilegeType().toString(),
            auditLogFields.isResult(),
            auditLogFields.getDatabase(),
            auditLogFields.getSqlString(),
            log);
    DataNodeAsyncRequestContext<TAuditLogReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.WRITE_AUDIT_LOG, req, targetDatanodeMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    if (statusMap.get(regionLeaderId).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logger.error(
          "Failed to write audit log {} to DataNode {}: {}",
          log,
          regionLeaderId,
          statusMap.get(regionLeaderId));
    }
  }
}
