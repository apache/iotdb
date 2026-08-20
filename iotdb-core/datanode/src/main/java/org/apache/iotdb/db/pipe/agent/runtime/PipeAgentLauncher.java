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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.service.ResourcesInformationHolder;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    final Set<String> unavailablePipePluginNameSet = new HashSet<>();
    int index = 0;
    while (index < uninstalledOrConflictedPipePluginMetaList.size()) {
      List<PipePluginMeta> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
          && index + offset < uninstalledOrConflictedPipePluginMetaList.size()) {
        curList.add(uninstalledOrConflictedPipePluginMetaList.get(index + offset));
        offset++;
      }
      index += offset;
      unavailablePipePluginNameSet.addAll(fetchAndSavePipePluginJars(curList));
    }

    // create instances of pipe plugins and do registration
    for (PipePluginMeta meta : resourcesInformationHolder.getPipePluginMetaList()) {
      if (meta.isBuiltin()) {
        continue;
      }
      if (unavailablePipePluginNameSet.contains(meta.getPluginName())) {
        continue;
      }
      try {
        PipeDataNodeAgent.plugin().doRegister(meta);
      } catch (Throwable e) {
        PipeDataNodeAgent.plugin().markPluginLoadFailure(meta, e);
        // Ignore a single broken plugin and continue startup.
        LOGGER.error(
            DataNodePipeMessages.FAILURE_WHEN_REGISTER_PIPE_PLUGIN_SKIP_THIS,
            meta.getPluginName(),
            e);
      }
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

  static Set<String> fetchAndSavePipePluginJars(List<PipePluginMeta> pipePluginMetaList) {
    if (pipePluginMetaList.isEmpty()) {
      return new HashSet<>();
    }

    final List<String> pluginNameList =
        pipePluginMetaList.stream().map(PipePluginMeta::getPluginName).collect(Collectors.toList());
    final List<String> jarNameList =
        pipePluginMetaList.stream().map(PipePluginMeta::getJarName).collect(Collectors.toList());
    final TGetJarInListResp resp;

    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      resp = configNodeClient.getPipePluginJar(new TGetJarInListReq(jarNameList));
    } catch (Exception e) {
      LOGGER.error(
          DataNodePipeMessages
              .LOG_FAILED_TO_FETCH_PIPE_PLUGIN_JARS_FROM_CONFIGNODE_PLUGINS_ARG_JARS_ARG_STATUS_ARG_RETRYING_EACH_PLUGIN_INDIVIDUALLY_574C0077,
          pluginNameList,
          jarNameList,
          null,
          e);
      return fetchAndSavePipePluginJarsIndividually(pipePluginMetaList);
    }

    if (resp == null
        || resp.getStatus() == null
        || resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(
          DataNodePipeMessages
              .LOG_FAILED_TO_FETCH_PIPE_PLUGIN_JARS_FROM_CONFIGNODE_PLUGINS_ARG_JARS_ARG_STATUS_ARG_RETRYING_EACH_PLUGIN_INDIVIDUALLY_574C0077,
          pluginNameList,
          jarNameList,
          resp == null ? null : resp.getStatus());
      return fetchAndSavePipePluginJarsIndividually(pipePluginMetaList);
    }

    final List<ByteBuffer> jarList = resp.getJarList();
    if (jarList == null || jarList.size() != pipePluginMetaList.size()) {
      LOGGER.error(
          DataNodePipeMessages
              .LOG_CONFIGNODE_RETURNED_ARG_PIPE_PLUGIN_JARS_FOR_ARG_REQUESTED_PLUGINS_PLUGINS_ARG_JARS_ARG_RETRYING_EACH_PLUGIN_INDIVIDUALLY_27E32FDE,
          jarList == null ? 0 : jarList.size(),
          pipePluginMetaList.size(),
          pluginNameList,
          jarNameList);
      return fetchAndSavePipePluginJarsIndividually(pipePluginMetaList);
    }

    return savePipePluginJars(pipePluginMetaList, jarList);
  }

  private static Set<String> fetchAndSavePipePluginJarsIndividually(
      List<PipePluginMeta> pipePluginMetaList) {
    final Set<String> unavailablePipePluginNameSet = new HashSet<>();
    for (PipePluginMeta pipePluginMeta : pipePluginMetaList) {
      if (!fetchAndSavePipePluginJarIndividually(pipePluginMeta)) {
        unavailablePipePluginNameSet.add(pipePluginMeta.getPluginName());
      }
    }
    return unavailablePipePluginNameSet;
  }

  private static boolean fetchAndSavePipePluginJarIndividually(PipePluginMeta pipePluginMeta) {
    final String pluginName = pipePluginMeta.getPluginName();
    final String jarName = pipePluginMeta.getJarName();
    final TGetJarInListResp resp;
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      resp = configNodeClient.getPipePluginJar(new TGetJarInListReq(List.of(jarName)));
    } catch (Exception e) {
      PipeDataNodeAgent.plugin().markPluginLoadFailure(pipePluginMeta, e);
      LOGGER.error(
          DataNodePipeMessages
              .LOG_FAILED_TO_FETCH_PIPE_PLUGIN_JAR_ARG_FOR_PIPE_PLUGIN_ARG_FROM_CONFIGNODE_4929C5D9,
          jarName,
          pluginName,
          e);
      return false;
    }

    if (resp == null
        || resp.getStatus() == null
        || resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      final PipeException exception =
          new PipeException(
              String.format(
                  DataNodePipeMessages
                      .EXCEPTION_FAILED_TO_FETCH_PIPE_PLUGIN_JAR_FROM_CONFIGNODE_FOR_PLUGIN_ARG_JAR_ARG_STATUS_ARG_B7C7FDE5,
                  pluginName,
                  jarName,
                  resp == null ? null : resp.getStatus()));
      PipeDataNodeAgent.plugin().markPluginLoadFailure(pipePluginMeta, exception);
      LOGGER.error(exception.getMessage(), exception);
      return false;
    }

    final List<ByteBuffer> jarList = resp.getJarList();
    if (jarList == null || jarList.size() != 1) {
      final PipeException exception =
          new PipeException(
              String.format(
                  DataNodePipeMessages
                      .EXCEPTION_CONFIGNODE_RETURNED_ARG_JARS_FOR_PIPE_PLUGIN_ARG_WHILE_ONE_WAS_REQUESTED_A724E582,
                  jarList == null ? 0 : jarList.size(),
                  pluginName));
      PipeDataNodeAgent.plugin().markPluginLoadFailure(pipePluginMeta, exception);
      LOGGER.error(exception.getMessage(), exception);
      return false;
    }

    try {
      PipePluginExecutableManager.getInstance()
          .savePluginToInstallDir(jarList.get(0), pluginName, jarName);
      return true;
    } catch (Exception e) {
      PipeDataNodeAgent.plugin().markPluginLoadFailure(pipePluginMeta, e);
      LOGGER.error(
          DataNodePipeMessages.LOG_FAILED_TO_SAVE_JAR_ARG_FOR_PIPE_PLUGIN_ARG_A64D1530,
          jarName,
          pluginName,
          e);
      return false;
    }
  }

  private static Set<String> savePipePluginJars(
      List<PipePluginMeta> pipePluginMetaList, List<ByteBuffer> jarList) {
    final Set<String> unavailablePipePluginNameSet = new HashSet<>();
    for (int i = 0; i < pipePluginMetaList.size(); i++) {
      final PipePluginMeta pipePluginMeta = pipePluginMetaList.get(i);
      try {
        PipePluginExecutableManager.getInstance()
            .savePluginToInstallDir(
                jarList.get(i), pipePluginMeta.getPluginName(), pipePluginMeta.getJarName());
      } catch (Exception e) {
        PipeDataNodeAgent.plugin().markPluginLoadFailure(pipePluginMeta, e);
        LOGGER.error(
            DataNodePipeMessages.LOG_FAILED_TO_SAVE_JAR_ARG_FOR_PIPE_PLUGIN_ARG_A64D1530,
            pipePluginMeta.getJarName(),
            pipePluginMeta.getPluginName(),
            e);
        unavailablePipePluginNameSet.add(pipePluginMeta.getPluginName());
      }
    }
    return unavailablePipePluginNameSet;
  }

  public static synchronized void launchPipeTaskAgent() {
    try (final ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetAllPipeInfoResp getAllPipeInfoResp = configNodeClient.getAllPipeInfo();
      if (getAllPipeInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(DataNodePipeMessages.FAILED_TO_GET_PIPE_METAS_WILL_BE);
      }

      PipeDataNodeAgent.task()
          .handlePipeMetaChanges(
              getAllPipeInfoResp.getAllPipeInfo().stream()
                  .map(
                      byteBuffer -> {
                        final PipeMeta pipeMeta = PipeMeta.deserialize4TaskAgent(byteBuffer);
                        LOGGER.info(
                            DataNodePipeMessages.PULLED_PIPE_META_FROM_CONFIG_NODE_RECOVERING,
                            pipeMeta);
                        return pipeMeta;
                      })
                  .collect(Collectors.toList()));
    } catch (Exception | Error e) {
      // Ignore unexpected exceptions to ensure that DataNode can start normally
      LOGGER.info(DataNodePipeMessages.FAILED_TO_GET_PIPE_TASK_META_FROM, e);
    }
  }
}
