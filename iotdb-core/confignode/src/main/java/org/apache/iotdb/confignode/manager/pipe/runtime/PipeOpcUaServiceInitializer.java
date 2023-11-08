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

package org.apache.iotdb.confignode.manager.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.HashMap;
import java.util.Map;

public class PipeOpcUaServiceInitializer {

  private static final String OPC_NAME = "__opc_security";

  private final ConfigManager configManager;

  PipeOpcUaServiceInitializer(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void start() {
    PipeTaskInfo pipeTaskinfo =
        configManager.getPipeManager().getPipeTaskCoordinator().tryLock().get();

    if (CommonDescriptor.getInstance().getConfig().isEnableOpcUaService()) {
      // Start opc service on the leader if it's not started.
      // Create pipe
      try {
        Map<String, String> connectorAttributes = new HashMap<>();
        connectorAttributes.put(
            PipeConnectorConstant.SINK_KEY, BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName());
        TCreatePipeReq opcCreateReq = new TCreatePipeReq(OPC_NAME, connectorAttributes);
        opcCreateReq.setExtractorAttributes(new HashMap<>());
        opcCreateReq.setProcessorAttributes(new HashMap<>());

        pipeTaskinfo.checkBeforeCreatePipe(opcCreateReq);

        TSStatus result;
        do {
          result = configManager.getPipeManager().getPipeTaskCoordinator().createPipe(opcCreateReq);
          if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            wait(10000);
          }
        } while (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } catch (PipeException | InterruptedException ignore) {
        Thread.currentThread().interrupt();
        // Skip if there are check failure
      }
      // Start pipe
      try {
        pipeTaskinfo.checkBeforeStartPipe(OPC_NAME);

        TSStatus result;
        do {
          result = configManager.getPipeManager().getPipeTaskCoordinator().startPipe(OPC_NAME);
          if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            wait(10000);
          }
        } while (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } catch (PipeException | InterruptedException ignore) {
        Thread.currentThread().interrupt();
        // Skip if there are check failure
      }
    } else {
      // Drop opc service if configured to false
      try {
        pipeTaskinfo.checkBeforeDropPipe(OPC_NAME);

        TSStatus result;
        do {
          result = configManager.getPipeManager().getPipeTaskCoordinator().dropPipe(OPC_NAME);
          if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            wait(10000);
          }
        } while (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } catch (PipeException | InterruptedException ignore) {
        Thread.currentThread().interrupt();
        // The drop pipe currently will not throw failure, the checking is
        // prepared for unification and potential extension
      }
    }
  }
}
