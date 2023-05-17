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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TPullPipeMetaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.service.ResourcesInformationHolder;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PipeNodeStartHandler {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public void preparePipeTaskResources() throws StartupException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TPullPipeMetaResp resp = configNodeClient.pullPipeMeta();
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get pipe task meta from config node.");
      }
      final List<PipeMeta> pipeMetas = new ArrayList<>();
      for (ByteBuffer byteBuffer : resp.getPipeMetas()) {
        pipeMetas.add(PipeMeta.deserialize(byteBuffer));
      }
      PipeAgent.task().handlePipeMetaChanges(pipeMetas);
    } catch (TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }

  public void preparePipePluginResources(ResourcesInformationHolder resourcesInformationHolder)
      throws StartupException {
    initPipePluginRelatedInstance();
    if (resourcesInformationHolder.getPipePluginMetaList() == null
        || resourcesInformationHolder.getPipePluginMetaList().isEmpty()) {
      return;
    }

    // get jars from config node
    List<PipePluginMeta> pipePluginNeedJarList =
        getJarListForPipePlugin(resourcesInformationHolder);
    int index = 0;
    while (index < pipePluginNeedJarList.size()) {
      List<PipePluginMeta> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
          && index + offset < pipePluginNeedJarList.size()) {
        curList.add(pipePluginNeedJarList.get(index + offset));
        offset++;
      }
      index += (offset + 1);
      getJarOfPipePlugins(curList);
    }

    // create instances of pipe plugins and do registration
    try {
      for (PipePluginMeta meta : resourcesInformationHolder.getPipePluginMetaList()) {
        if (meta.isBuiltin()) {
          continue;
        }
        PipeAgent.plugin().doRegister(meta);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private void initPipePluginRelatedInstance() throws StartupException {
    try {
      PipePluginExecutableManager.setupAndGetInstance(
          config.getPipeTemporaryLibDir(), config.getPipeDir());
      PipePluginClassLoaderManager.setupAndGetInstance(config.getPipeDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private List<PipePluginMeta> getJarListForPipePlugin(
      ResourcesInformationHolder resourcesInformationHolder) {
    List<PipePluginMeta> res = new ArrayList<>();
    for (PipePluginMeta pipePluginMeta : resourcesInformationHolder.getPipePluginMetaList()) {
      if (pipePluginMeta.isBuiltin()) {
        continue;
      }
      // If jar does not exist, add current pipePluginMeta to list
      if (!PipePluginExecutableManager.getInstance()
          .hasFileUnderInstallDir(pipePluginMeta.getJarName())) {
        res.add(pipePluginMeta);
      } else {
        try {
          // local jar has conflicts with jar on config node, add current pipePluginMeta to list
          if (!PipePluginExecutableManager.getInstance().isLocalJarMatched(pipePluginMeta)) {
            res.add(pipePluginMeta);
          }
        } catch (PipeManagementException e) {
          res.add(pipePluginMeta);
        }
      }
    }
    return res;
  }

  private void getJarOfPipePlugins(List<PipePluginMeta> pipePluginMetaList)
      throws StartupException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      List<String> jarNameList =
          pipePluginMetaList.stream().map(PipePluginMeta::getJarName).collect(Collectors.toList());
      TGetJarInListResp resp = configNodeClient.getPipePluginJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get pipe plugin jar from config node.");
      }
      List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < pipePluginMetaList.size(); i++) {
        PipePluginExecutableManager.getInstance()
            .saveToInstallDir(jarList.get(i), pipePluginMetaList.get(i).getJarName());
      }
    } catch (IOException | TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }
}
