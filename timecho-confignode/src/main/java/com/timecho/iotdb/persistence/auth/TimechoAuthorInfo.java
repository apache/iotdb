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

package com.timecho.iotdb.persistence.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.EditionGate;
import org.apache.iotdb.commons.conf.IoTDBGatedFeature;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.auth.AuthorInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCheckMaxClientNumResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.i18n.TimechoConfigNodeMessages;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ENABLE_SEPARATION_OF_ADMIN_POWERS;

public class TimechoAuthorInfo extends AuthorInfo {
  private static final String ENABLE_SEPARATION_OF_ADMIN_POWERS_FILE_NAME =
      "separation_of_admin_power_info.bin";

  private volatile boolean separationOfPowersEnabled = false;

  public TimechoAuthorInfo(ConfigManager configManager) {
    super(configManager);
  }

  public TSStatus enableSeparationOfAdminPowers(
      String systemAdminUsername, String securityAdminUsername, String auditAdminUsername) {
    if (separationOfPowersEnabled) {
      return RpcUtils.SUCCESS_STATUS;
    }
    try {
      authorizer.enableSeparationOfPowers(
          systemAdminUsername, securityAdminUsername, auditAdminUsername);
    } catch (AuthException e) {
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    }
    separationOfPowersEnabled = true;
    authorPlanExecutor = new StrictAuthorPlanExecutor(authorizer, configManager);
    ConfigurationFileUtils.updateAppliedProperties(ENABLE_SEPARATION_OF_ADMIN_POWERS, "true");
    return RpcUtils.SUCCESS_STATUS;
  }

  public boolean isEnableSeparationOfAdminPowers() {
    return separationOfPowersEnabled;
  }

  // Edition gate on the per-user connection limit (an IoTDB-edition-disabled feature). Overriding
  // the dispatch choke point (AuthorInfo.authorNonQuery) catches the plan regardless of which
  // author plan executor is active. StrictAuthorPlanExecutor is only installed after separation of
  // powers is enabled, so the gate cannot live there — it would never run in the IoTDB edition.
  @Override
  public TSStatus authorNonQuery(AuthorTreePlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    if (EditionGate.isIoTDB()
        && (authorType == ConfigPhysicalPlanType.UpdateUserMaxSession
            || authorType == ConfigPhysicalPlanType.UpdateUserMinSession)) {
      return RpcUtils.getStatus(
          TSStatusCode.SEMANTIC_ERROR,
          String.format(
              TimechoConfigNodeMessages.MESSAGE_ARG_IS_NOT_AVAILABLE_IN_THIS_EDITION_7C713053,
              IoTDBGatedFeature.CONNECTION_LIMIT.getDisplayName()));
    }
    return super.authorNonQuery(authorPlan);
  }

  @Override
  public TSStatus authorNonQuery(AuthorRelationalPlan authorPlan) {
    ConfigPhysicalPlanType authorType = authorPlan.getAuthorType();
    if (EditionGate.isIoTDB()
        && (authorType == ConfigPhysicalPlanType.RUpdateUserMaxSession
            || authorType == ConfigPhysicalPlanType.RUpdateUserMinSession)) {
      return RpcUtils.getStatus(
          TSStatusCode.SEMANTIC_ERROR,
          String.format(
              TimechoConfigNodeMessages.MESSAGE_ARG_IS_NOT_AVAILABLE_IN_THIS_EDITION_7C713053,
              IoTDBGatedFeature.CONNECTION_LIMIT.getDisplayName()));
    }
    return super.authorNonQuery(authorPlan);
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    // generate mark in snapshot dir
    if (separationOfPowersEnabled) {
      if (!new File(snapshotDir, ENABLE_SEPARATION_OF_ADMIN_POWERS_FILE_NAME).createNewFile()) {
        return false;
      }
    }
    return super.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    super.processLoadSnapshot(snapshotDir);
    File markInSnapshotDir = new File(snapshotDir, ENABLE_SEPARATION_OF_ADMIN_POWERS_FILE_NAME);
    separationOfPowersEnabled = markInSnapshotDir.exists();
    if (separationOfPowersEnabled) {
      authorPlanExecutor = new StrictAuthorPlanExecutor(authorizer, configManager);
    }
  }

  public TSStatus checkSessionNumOnConnect(
      Map<String, Integer> currentSessionInfo, int rpcMaxConcurrentClientNum) {
    return authorPlanExecutor.checkSessionNumOnConnect(
        currentSessionInfo, rpcMaxConcurrentClientNum);
  }

  public TCheckMaxClientNumResp checkMaxClientNumValid(int maxConcurrentClientNum)
      throws AuthException {
    return authorPlanExecutor.checkMaxClientNumValid(maxConcurrentClientNum);
  }
}
