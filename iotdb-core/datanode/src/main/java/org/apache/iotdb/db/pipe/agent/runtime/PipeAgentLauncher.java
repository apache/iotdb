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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.agent.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.agent.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.service.ResourcesInformationHolder;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class PipeAgentLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeAgentLauncher.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private PipeAgentLauncher() {
    // Forbidding instantiation
  }

  public static synchronized void launchPipePluginAgent(
      ResourcesInformationHolder resourcesInformationHolder) throws StartupException {
    initPipePluginRelatedInstances();

    if (resourcesInformationHolder.getPipePluginMetaList() == null
        || resourcesInformationHolder.getPipePluginMetaList().isEmpty()) {
      return;
    }

    final List<PipePluginMeta> uninstalledOrConflictedPipePluginMetaList =
        getUninstalledOrConflictedPipePluginMetaList(resourcesInformationHolder);
    int index = 0;
    while (index < uninstalledOrConflictedPipePluginMetaList.size()) {
      List<PipePluginMeta> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
          && index + offset < uninstalledOrConflictedPipePluginMetaList.size()) {
        curList.add(uninstalledOrConflictedPipePluginMetaList.get(index + offset));
        offset++;
      }
      index += (offset + 1);
      fetchAndSavePipePluginJars(curList);
    }

    // create instances of pipe plugins and do registration
    try {
      for (PipePluginMeta meta : resourcesInformationHolder.getPipePluginMetaList()) {
        if (meta.isBuiltin()) {
          continue;
        }
        PipeDataNodeAgent.plugin().doRegister(meta);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private static void initPipePluginRelatedInstances() throws StartupException {
    try {
      PipePluginExecutableManager.setupAndGetInstance(
          IOTDB_CONFIG.getPipeTemporaryLibDir(), IOTDB_CONFIG.getPipeLibDir());
      PipePluginClassLoaderManager.setupAndGetInstance(IOTDB_CONFIG.getPipeLibDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private static List<PipePluginMeta> getUninstalledOrConflictedPipePluginMetaList(
      ResourcesInformationHolder resourcesInformationHolder) {
    final List<PipePluginMeta> pipePluginMetaList = new ArrayList<>();
    for (PipePluginMeta pipePluginMeta : resourcesInformationHolder.getPipePluginMetaList()) {
      if (pipePluginMeta.isBuiltin()) {
        continue;
      }
      // If jar does not exist, add current pipePluginMeta to list
      if (!PipePluginExecutableManager.getInstance()
          .hasPluginFileUnderInstallDir(
              pipePluginMeta.getPluginName(), pipePluginMeta.getJarName())) {
        pipePluginMetaList.add(pipePluginMeta);
      } else {
        try {
          // local jar has conflicts with jar on config node, add current pipePluginMeta to list
          if (!PipePluginExecutableManager.getInstance().isLocalJarMatched(pipePluginMeta)) {
            pipePluginMetaList.add(pipePluginMeta);
          }
        } catch (PipeException e) {
          pipePluginMetaList.add(pipePluginMeta);
        }
      }
    }
    return pipePluginMetaList;
  }

  private static void fetchAndSavePipePluginJars(List<PipePluginMeta> pipePluginMetaList)
      throws StartupException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final List<String> jarNameList =
          pipePluginMetaList.stream().map(PipePluginMeta::getJarName).collect(Collectors.toList());
      final TGetJarInListResp resp =
          configNodeClient.getPipePluginJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get pipe plugin jar from config node.");
      }
      final List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < pipePluginMetaList.size(); i++) {
        PipePluginExecutableManager.getInstance()
            .savePluginToInstallDir(
                jarList.get(i),
                pipePluginMetaList.get(i).getPluginName(),
                pipePluginMetaList.get(i).getJarName());
      }
    } catch (IOException | TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }

  public static synchronized void launchPipeTaskAgent() {
    try (final ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetAllPipeInfoResp getAllPipeInfoResp = configNodeClient.getAllPipeInfo();
      if (getAllPipeInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new StartupException("Failed to get pipe task meta from config node.");
      }

      PipeDataNodeAgent.task()
          .handlePipeMetaChanges(
              getAllPipeInfoResp.getAllPipeInfo().stream()
                  .map(
                      byteBuffer -> {
                        final PipeMeta pipeMeta = PipeMeta.deserialize4TaskAgent(byteBuffer);
                        LOGGER.info(
                            "Pulled pipe meta from config node: {}, recovering ...", pipeMeta);
                        return pipeMeta;
                      })
                  .collect(Collectors.toList()));
    } catch (Exception e) {
      LOGGER.info(
          "Failed to get pipe task meta from config node. Ignore the exception, "
              + "because config node may not be ready yet, and "
              + "meta will be pushed by config node later.",
          e);
    }
  }
}
