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

package org.apache.iotdb.db.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserDataTransferAuditEvent;
import org.apache.iotdb.commons.audit.UserDataTransferProtectionMethod;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.commons.schema.table.Audit;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;

import javax.annotation.Nullable;

public final class DataNodeUserDataTransferAuditor {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private DataNodeUserDataTransferAuditor() {}

  public static boolean isEnabled() {
    return COMMON_CONFIG.isEnableAuditLog();
  }

  public static void record(
      TEndPoint initiator,
      TEndPoint source,
      TEndPoint target,
      boolean success,
      @Nullable String errorCode,
      @Nullable Throwable error) {
    try {
      if (!isEnabled()) {
        return;
      }
      DNAuditLogger.getInstance()
          .recordUserDataTransferAuditLog(
              new UserDataTransferAuditEvent(
                  initiator,
                  source,
                  target,
                  UserDataTransferProtectionMethod.fromTlsEnabled(
                      COMMON_CONFIG.isEnableInternalSSL()),
                  success,
                  errorCode != null
                      ? errorCode
                      : error == null ? null : error.getClass().getName()));
    } catch (RuntimeException ignored) {
      // Audit recording must not affect user data transfer.
    }
  }

  public static boolean containsUserData(
      ConsensusGroupId consensusGroupId, IConsensusRequest request) {
    if (!(consensusGroupId instanceof DataRegionId)) {
      return false;
    }
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion((DataRegionId) consensusGroupId);
    return dataRegion != null && containsUserData(dataRegion.getDatabaseName(), request);
  }

  public static boolean containsUserData(ConsensusGroupId consensusGroupId) {
    if (!(consensusGroupId instanceof DataRegionId)) {
      return false;
    }
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion((DataRegionId) consensusGroupId);
    return dataRegion != null && containsUserData(dataRegion.getDatabaseName());
  }

  static boolean containsUserData(String database) {
    return !Audit.isAuditDatabase(database);
  }

  static boolean containsUserData(String database, IConsensusRequest request) {
    if (!containsUserData(database)) {
      return false;
    }
    try {
      final PlanNode planNode;
      if (request instanceof PlanNode) {
        planNode = (PlanNode) request;
      } else if (request instanceof IoTConsensusRequest) {
        planNode = WALEntry.deserializeForConsensus(request.serializeToByteBuffer().duplicate());
      } else if (request instanceof ByteBufferConsensusRequest) {
        planNode = PlanNodeType.deserialize(request.serializeToByteBuffer().duplicate());
      } else {
        return false;
      }
      return containsInsertNode(planNode);
    } catch (RuntimeException ignored) {
      // Classification is advisory and must not affect consensus replication.
      return false;
    }
  }

  public static boolean containsInsertNode(PlanNode node) {
    if (node instanceof InsertNode) {
      return true;
    }
    if (node.getChildren() == null) {
      return false;
    }
    for (PlanNode child : node.getChildren()) {
      if (containsInsertNode(child)) {
        return true;
      }
    }
    return false;
  }
}
