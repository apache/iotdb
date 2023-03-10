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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.pipe.plugin.meta.ConfigNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginInfo.class);
  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();
  private static final String SNAPSHOT_FILE_NAME = "pipe_plugin_info.bin";

  private final ReentrantLock pipePluginInfoLock = new ReentrantLock();

  private final ConfigNodePipePluginMetaKeeper pipePluginMetaKeeper;
  private final PipePluginExecutableManager pipePluginExecutableManager;

  public PipePluginInfo() throws IOException {
    pipePluginMetaKeeper = new ConfigNodePipePluginMetaKeeper();
    pipePluginExecutableManager =
        PipePluginExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getPipeTemporaryLibDir(), CONFIG_NODE_CONF.getPipeDir());
  }

  /////////////////////////////// Lock ///////////////////////////////

  public void acquirePipePluginInfoLock() {
    pipePluginInfoLock.lock();
  }

  public void releasePipePluginInfoLock() {
    pipePluginInfoLock.unlock();
  }

  /////////////////////////////// Validator ///////////////////////////////

  public void validateBeforeCreatingPipePlugin(String pluginName, String jarName, String jarMD5) {
    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      throw new PipeManagementException(
          String.format(
              "Failed to create PipePlugin [%s], the same name PipePlugin has been created",
              pluginName));
    }

    if (pipePluginMetaKeeper.jarNameExistsAndMatchesMd5(jarName, jarMD5)) {
      throw new PipeManagementException(
          String.format(
              "Failed to create PipePlugin [%s], the same name Jar [%s] but different MD5 [%s] has existed",
              pluginName, jarName, jarMD5));
    }
  }

  public void validateBeforeDroppingPipePlugin(String pluginName) {
    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      return;
    }

    throw new PipeManagementException(
        String.format("Failed to drop PipePlugin [%s], the PipePlugin does not exist", pluginName));
  }

  public boolean isJarNeededToBeSavedWhenCreatingPipePlugin(String jarName) {
    return !pipePluginMetaKeeper.containsJar(jarName);
  }

  /////////////////////////////// Pipe Plugin Management ///////////////////////////////

  public TSStatus createPipePlugin(CreatePipePluginPlan physicalPlan) {
    try {
      final PipePluginMeta pipePluginMeta = physicalPlan.getPipePluginMeta();
      pipePluginMetaKeeper.addPipePluginMeta(pipePluginMeta.getPluginName(), pipePluginMeta);
      pipePluginMetaKeeper.addJarNameAndMd5(
          pipePluginMeta.getJarName(), pipePluginMeta.getJarMD5());

      if (physicalPlan.getJarFile() != null) {
        pipePluginExecutableManager.saveToInstallDir(
            ByteBuffer.wrap(physicalPlan.getJarFile().getValues()), pipePluginMeta.getJarName());
      }

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add PipePlugin [%s] in PipePlugin_Table on Config Nodes, because of %s",
              physicalPlan.getPipePluginMeta().getPluginName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public TSStatus dropPipePlugin(DropPipePluginPlan physicalPlan) {
    final String pluginName = physicalPlan.getPluginName();

    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      pipePluginMetaKeeper.removeJarNameAndMd5IfPossible(
          pipePluginMetaKeeper.getPipePluginMeta(pluginName).getJarName());
      pipePluginMetaKeeper.removePipePluginMeta(pluginName);
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public DataSet showPipePlugins() {
    return new PipePluginTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        Arrays.asList(pipePluginMetaKeeper.getAllPipePluginMeta()));
  }

  public JarResp getPipePluginJar(GetPipePluginJarPlan physicalPlan) {
    try {
      List<ByteBuffer> jarList = new ArrayList<>();
      for (String jarName : physicalPlan.getJarNames()) {
        jarList.add(
            ExecutableManager.transferToBytebuffer(
                PipePluginExecutableManager.getInstance()
                    .getFileStringUnderInstallByName(jarName)));
      }
      return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
    } catch (Exception e) {
      LOGGER.error("Get PipePlugin_Jar failed", e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get PipePlugin_Jar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
  }

  /////////////////////////////// Snapshot Processor ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    acquirePipePluginInfoLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      pipePluginMetaKeeper.processTakeSnapshot(fileOutputStream);
    } finally {
      releasePipePluginInfoLock();
    }
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    acquirePipePluginInfoLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      pipePluginMetaKeeper.processLoadSnapshot(fileInputStream);
    } finally {
      releasePipePluginInfoLock();
    }
  }
}
