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

package com.timecho.iotdb.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeInternalRPCServiceImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl;
import org.apache.iotdb.db.service.DataNode;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaResp;
import org.apache.iotdb.rpc.RpcUtils;

import com.timecho.iotdb.auth.StrictAccessControlImpl;
import com.timecho.iotdb.auth.StrictTreeAccessCheckVisitor;
import com.timecho.iotdb.dataregion.compaction.selector.impl.SharedStorageCompactionSelector;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeInternalRPCServiceImplNew extends DataNodeInternalRPCServiceImpl {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataNodeInternalRPCServiceImplNew.class);

  public DataNodeInternalRPCServiceImplNew(DataNode.DataNodeContext dataNodeContext) {
    super(dataNodeContext);
  }

  @Override
  public TFetchLeaderRemoteReplicaResp fetchLeaderRemoteReplica(TFetchLeaderRemoteReplicaReq req) {
    try {
      return SharedStorageCompactionSelector.selectLeaderFiles(
          req.getDataRegionId(), req.getTimePartition());
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred when select leader files for shared storage compaction, abort this share operation.",
          e);
      return new TFetchLeaderRemoteReplicaResp();
    }
  }

  @Override
  public TDataNodeHeartbeatResp getDataNodeHeartBeat(TDataNodeHeartbeatReq req) throws TException {
    if (req.isSetBooleanVariables1()) {
      byte booleanVariables1 = req.getBooleanVariables1();
      if ((booleanVariables1 & ((byte) 0b00000001)) != 0) {
        if (AuthorityChecker.getAccessControl() instanceof AccessControlImpl) {
          AuthorityChecker.setAccessControl(
              new StrictAccessControlImpl(
                  new ITableAuthCheckerImpl(), new StrictTreeAccessCheckVisitor()));
          SessionManager.getInstance()
              .removeSessions(session -> session.getUserId() == AuthorityChecker.SUPER_USER_ID);
        }
      }
    }
    return super.getDataNodeHeartBeat(req);
  }

  @Override
  public TSStatus enableSeparationOfAdminPower() throws TException {
    if (AuthorityChecker.getAccessControl() instanceof AccessControlImpl) {
      AuthorityChecker.setAccessControl(
          new StrictAccessControlImpl(
              new ITableAuthCheckerImpl(), new StrictTreeAccessCheckVisitor()));

      SessionManager.getInstance()
          .removeSessions(session -> session.getUserId() == AuthorityChecker.SUPER_USER_ID);
    }
    return RpcUtils.SUCCESS_STATUS;
  }
}
