/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.consensus.request.read.ttl.ShowTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.response.ttl.ShowTTLResp;
import org.apache.iotdb.confignode.persistence.TTLInfo;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class TTLManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TTLManager.class);
  private final IManager configManager;

  private final TTLInfo ttlInfo;

  private static final int ttlCountThreshold =
      CommonDescriptor.getInstance().getConfig().getTTlRuleCapacity();

  public TTLManager(IManager configManager, TTLInfo ttlInfo) {
    this.configManager = configManager;
    this.ttlInfo = ttlInfo;
  }

  public TSStatus setTTL(SetTTLPlan setTTLPlan) {
    PartialPath path = new PartialPath(setTTLPlan.getPathPattern());
    if (!checkIsPathValidated(path)) {
      TSStatus errorStatus = new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
      errorStatus.setMessage(
          String.format(
              "Illegal pattern path: %s, pattern path should end with **, otherwise, it should be a specific database or device path without *",
              path.getFullPath()));
      return errorStatus;
    }
    if (ttlInfo.getTTLCount() >= ttlCountThreshold) {
      TSStatus errorStatus = new TSStatus(TSStatusCode.OVERSIZE_TTL.getStatusCode());
      errorStatus.setMessage(
          "The number of TTL stored in the system has reached threshold %d, please increase the ttl_count parameter.");
      return errorStatus;
    }

    // if path matches database, then set both path and path.**
    setTTLPlan.setDataBase(configManager.getPartitionManager().isDatabaseExist(path.getFullPath()));

    long ttl =
        CommonDateTimeUtils.convertMilliTimeWithPrecision(
            setTTLPlan.getTTL(),
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    ttl = ttl <= 0 ? Long.MAX_VALUE : ttl;
    setTTLPlan.setTTL(ttl);

    return configManager.getProcedureManager().setTTL(setTTLPlan);
  }

  public TSStatus unsetTTL(SetTTLPlan setTTLPlan) {
    PartialPath path = new PartialPath(setTTLPlan.getPathPattern());
    if (!checkIsPathValidated(path)) {
      TSStatus errorStatus = new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
      errorStatus.setMessage(
          String.format(
              "Illegal pattern path: %s, pattern path should end with **, otherwise, it should be a specific database or device path without *",
              path.getFullPath()));
      return errorStatus;
    }
    // if path matches database, then unset both path and path.**
    setTTLPlan.setDataBase(configManager.getPartitionManager().isDatabaseExist(path.getFullPath()));

    return configManager.getProcedureManager().setTTL(setTTLPlan);
  }

  public DataSet showAllTTL(ShowTTLPlan showTTLPlan) {
    try {
      return configManager.getConsensusManager().read(showTTLPlan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus tsStatus = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      tsStatus.setMessage(e.getMessage());
      ShowTTLResp resp = new ShowTTLResp();
      resp.setStatus(tsStatus);
      return resp;
    }
  }

  public Map<String, Long> getAllTTL() {
    return ((ShowTTLResp) showAllTTL(new ShowTTLPlan())).getPathTTLMap();
  }

  private boolean checkIsPathValidated(PartialPath path) {
    return path.isPrefixPath() || !path.getFullPath().contains(ONE_LEVEL_PATH_WILDCARD);
  }
}
