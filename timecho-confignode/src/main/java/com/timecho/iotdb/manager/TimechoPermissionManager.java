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

package com.timecho.iotdb.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.EditionGate;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.conf.IoTDBGatedFeature;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.persistence.auth.AuthorInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCheckMaxClientNumResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.procedure.consensus.request.write.auth.EnableSeparationOfAdminPowersPlan;
import com.timecho.iotdb.i18n.TimechoConfigNodeMessages;
import com.timecho.iotdb.persistence.auth.TimechoAuthorInfo;

import java.util.Map;

public class TimechoPermissionManager extends PermissionManager {
  public TimechoPermissionManager(ConfigManager configManager, AuthorInfo authorInfo) {
    super(configManager, authorInfo);
  }

  @Override
  public TSStatus enableSeparationOfPowers(
      String systemAdminUsername, String securityAdminUsername, String auditAdminUsername) {
    if (EditionGate.isIoTDB()) {
      TSStatus res = new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode());
      res.setMessage(
          String.format(
              TimechoConfigNodeMessages.MESSAGE_ARG_IS_NOT_AVAILABLE_IN_THIS_EDITION_7C713053,
              IoTDBGatedFeature.SEPARATION_OF_POWERS.getDisplayName()));
      return res;
    }
    try {
      return getConsensusManager()
          .write(
              new EnableSeparationOfAdminPowersPlan(
                  IoTDBConstant.DEFAULT_SYSTEM_ADMIN_USERNAME,
                  IoTDBConstant.DEFAULT_SECURITY_ADMIN_USERNAME,
                  IoTDBConstant.DEFAULT_AUDIT_ADMIN_USERNAME));
    } catch (final ConsensusException e) {
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public boolean isEnableSeparationOfAdminPowers() {
    return ((TimechoAuthorInfo) this.authorInfo).isEnableSeparationOfAdminPowers();
  }

  public TSStatus checkSessionNumOnConnect(
      Map<String, Integer> currentSessionInfo, int rpcMaxConcurrentClientNum) {
    return ((TimechoAuthorInfo) authorInfo)
        .checkSessionNumOnConnect(currentSessionInfo, rpcMaxConcurrentClientNum);
  }

  public TCheckMaxClientNumResp checkMaxClientNumValid(int maxConcurrentClientNum)
      throws AuthException {
    return ((TimechoAuthorInfo) authorInfo).checkMaxClientNumValid(maxConcurrentClientNum);
  }
}
