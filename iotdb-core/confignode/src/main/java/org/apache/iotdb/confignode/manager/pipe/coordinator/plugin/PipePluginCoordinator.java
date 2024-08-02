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

package org.apache.iotdb.confignode.manager.pipe.coordinator.plugin;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginTablePlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipePluginInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class PipePluginCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginCoordinator.class);

  private final ConfigManager configManager;
  private final PipePluginInfo pipePluginInfo;

  public PipePluginCoordinator(ConfigManager configManager, PipePluginInfo pipePluginInfo) {
    this.configManager = configManager;
    this.pipePluginInfo = pipePluginInfo;
  }

  public PipePluginInfo getPipePluginInfo() {
    return pipePluginInfo;
  }

  public void lock() {
    pipePluginInfo.acquirePipePluginInfoLock();
  }

  public void unlock() {
    pipePluginInfo.releasePipePluginInfoLock();
  }

  public TSStatus createPipePlugin(TCreatePipePluginReq req) {
    final String pluginName = req.getPluginName().toUpperCase();
    final String className = req.getClassName();
    final String jarName = req.getJarName();
    final String jarMD5 = req.getJarMD5();
    final PipePluginMeta pipePluginMeta =
        new PipePluginMeta(pluginName, className, false, jarName, jarMD5);

    return configManager
        .getProcedureManager()
        .createPipePlugin(
            pipePluginMeta,
            req.getJarFile(),
            req.isSetIfNotExistsCondition() && req.isIfNotExistsCondition());
  }

  public TSStatus dropPipePlugin(TDropPipePluginReq req) {
    return configManager.getProcedureManager().dropPipePlugin(req);
  }

  public TGetPipePluginTableResp getPipePluginTable() {
    try {
      return ((PipePluginTableResp)
              configManager.getConsensusManager().read(new GetPipePluginTablePlan()))
          .convertToThriftResponse();
    } catch (IOException | ConsensusException e) {
      LOGGER.error("Fail to get PipePluginTable", e);
      return new TGetPipePluginTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) {
    try {
      return ((JarResp)
              configManager
                  .getConsensusManager()
                  .read(new GetPipePluginJarPlan(req.getJarNameList())))
          .convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new JarResp(res, Collections.emptyList()).convertToThriftResponse();
    }
  }
}
