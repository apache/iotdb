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

package com.timecho.iotdb.manager.load.service;

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatReq;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.service.HeartbeatService;
import org.apache.iotdb.confignode.rpc.thrift.TActivationControl;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeActivation;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;

import com.timecho.iotdb.client.async.handlers.heartbeat.TimechoConfigNodeHeartbeatHandler;
import com.timecho.iotdb.commons.commission.Lottery;
import com.timecho.iotdb.manager.ITimechoManager;
import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.TimechoPermissionManager;
import com.timecho.iotdb.manager.load.TimechoLoadManager;
import com.timecho.iotdb.manager.regulate.RegulateManager;

public class TimechoHeartbeatService extends HeartbeatService {

  private ITimechoManager timechoConfigManager;

  public TimechoHeartbeatService(ITimechoManager timechoConfigManager, LoadCache loadCache) {
    super(timechoConfigManager, loadCache);
  }

  @Override
  protected void setConfigManager(IManager configManager) {
    this.timechoConfigManager = (TimechoConfigManager) configManager;
    super.configManager = this.timechoConfigManager;
  }

  @Override
  protected TDataNodeHeartbeatReq genHeartbeatReq() {
    TDataNodeHeartbeatReq req = super.genHeartbeatReq();
    boolean enableSeparationOfAdminPowers =
        ((TimechoPermissionManager) configManager.getPermissionManager())
            .isEnableSeparationOfAdminPowers();
    byte bits = 0b00000000;
    if (enableSeparationOfAdminPowers) {
      bits |= (byte) 0b00000001;
    }
    req.setBooleanVariables1(bits);
    return req;
  }

  @Override
  protected void setActivationRelatedInfoForDataNodeReq(TDataNodeHeartbeatReq req) {
    req.setActivation(
        new TDataNodeActivation(timechoConfigManager.getActivationManager().isActivated(), 0, 0));
  }

  @Override
  protected void setActivationRelatedInfoForAINodeReq(TAIHeartbeatReq req) {
    req.setActivated(timechoConfigManager.getActivationManager().isActivated());
  }

  @Override
  protected void setActivationRelatedInfoForConfigNodeReq(TConfigNodeHeartbeatReq req) {
    RegulateManager regulateManager = timechoConfigManager.getActivationManager();
    Lottery lottery = regulateManager.getLicense();
    TimechoLoadManager loadManager = timechoConfigManager.getLoadManager();
    loadManager.updateActivationStatusCache();
    if (loadManager.someConfigNodeNotSentHeartbeatYet()) {
      if (regulateManager.isActive()
          || loadManager.activeNodeLive()
          || regulateManager.activeNodeExistForLeader()) {
        req.setLicence(lottery.toTLicense());
      }
    } else {
      if (regulateManager.isActive()
          || loadManager.activeNodeLive()
          || regulateManager.activeNodeExistForLeader()) {
        req.setLicence(lottery.toTLicense());
      } else if (loadManager.activeNodeDisconnect()) {
        // do nothing
      } else {
        req.setActivationControl(TActivationControl.ALL_LICENSE_FILE_DELETED);
        regulateManager.giveUpLicenseBecauseLeaderBelieveThereIsNoActiveNodeInCluster();
      }
    }
  }

  @Override
  protected ConfigNodeHeartbeatHandler getConfigNodeHeartbeatHandler(int configNodeId) {
    return new TimechoConfigNodeHeartbeatHandler(
        configNodeId, timechoConfigManager, timechoConfigManager.getLoadManager());
  }
}
