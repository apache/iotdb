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
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
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
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.DO_NOTHING_PROCESSOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_COLLECTOR;

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
    // both build-in and user defined pipe plugin should be unique
    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      throw new PipeException(
          String.format(
              "Failed to create PipePlugin [%s], the same name PipePlugin has been created",
              pluginName));
    }

    if (pipePluginMetaKeeper.jarNameExistsAndMatchesMd5(jarName, jarMD5)) {
      throw new PipeException(
          String.format(
              "Failed to create PipePlugin [%s], the same name Jar [%s] but different MD5 [%s] has existed",
              pluginName, jarName, jarMD5));
    }
  }

  public void validateBeforeDroppingPipePlugin(String pluginName) {
    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)
        && pipePluginMetaKeeper.getPipePluginMeta(pluginName).isBuiltin()) {
      throw new PipeException(
          String.format(
              "Failed to drop PipePlugin [%s], the PipePlugin is a built-in PipePlugin",
              pluginName));
    }
  }

  public boolean isJarNeededToBeSavedWhenCreatingPipePlugin(String jarName) {
    return !pipePluginMetaKeeper.containsJar(jarName);
  }

  public void checkBeforeCreatePipe(TCreatePipeReq createPipeRequest) {
    final PipeParameters collectorParameters =
        new PipeParameters(createPipeRequest.getCollectorAttributes());
    final String collectorPluginName =
        collectorParameters.getStringOrDefault(
            PipeCollectorConstant.COLLECTOR_KEY, IOTDB_COLLECTOR.getPipePluginName());
    if (!pipePluginMetaKeeper.containsPipePlugin(collectorPluginName)) {
      final String exceptionMessage =
          String.format(
              "Failed to create pipe, the pipe collector plugin %s does not exist",
              collectorPluginName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeParameters processorParameters =
        new PipeParameters(createPipeRequest.getProcessorAttributes());
    final String processorPluginName =
        processorParameters.getStringOrDefault(
            PipeProcessorConstant.PROCESSOR_KEY, DO_NOTHING_PROCESSOR.getPipePluginName());
    if (!pipePluginMetaKeeper.containsPipePlugin(processorPluginName)) {
      final String exceptionMessage =
          String.format(
              "Failed to create pipe, the pipe processor plugin %s does not exist",
              processorPluginName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeParameters connectorParameters =
        new PipeParameters(createPipeRequest.getConnectorAttributes());
    if (!connectorParameters.hasAttribute(PipeConnectorConstant.CONNECTOR_KEY)) {
      final String exceptionMessage =
          "Failed to create pipe, the pipe connector plugin is not specified";
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
    final String connectorPluginName =
        connectorParameters.getString(PipeConnectorConstant.CONNECTOR_KEY);
    if (!pipePluginMetaKeeper.containsPipePlugin(connectorPluginName)) {
      final String exceptionMessage =
          String.format(
              "Failed to create pipe, the pipe connector plugin %s does not exist",
              connectorPluginName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  /////////////////////////////// Pipe Plugin Management ///////////////////////////////

  public TSStatus createPipePlugin(CreatePipePluginPlan createPipePluginPlan) {
    try {
      final PipePluginMeta pipePluginMeta = createPipePluginPlan.getPipePluginMeta();

      // try to drop the old pipe plugin if exists to reduce the effect of the inconsistency
      dropPipePlugin(new DropPipePluginPlan(pipePluginMeta.getPluginName()));

      pipePluginMetaKeeper.addPipePluginMeta(pipePluginMeta.getPluginName(), pipePluginMeta);
      pipePluginMetaKeeper.addJarNameAndMd5(
          pipePluginMeta.getJarName(), pipePluginMeta.getJarMD5());

      if (createPipePluginPlan.getJarFile() != null) {
        pipePluginExecutableManager.saveToInstallDir(
            ByteBuffer.wrap(createPipePluginPlan.getJarFile().getValues()),
            pipePluginMeta.getJarName());
      }

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to execute createPipePlugin(%s) on config nodes, because of %s",
              createPipePluginPlan.getPipePluginMeta().getPluginName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public TSStatus dropPipePlugin(DropPipePluginPlan dropPipePluginPlan) {
    final String pluginName = dropPipePluginPlan.getPluginName();

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

  public JarResp getPipePluginJar(GetPipePluginJarPlan getPipePluginJarPlan) {
    try {
      List<ByteBuffer> jarList = new ArrayList<>();
      for (String jarName : getPipePluginJarPlan.getJarNames()) {
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

    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      pipePluginMetaKeeper.processTakeSnapshot(fileOutputStream);
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

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      pipePluginMetaKeeper.processLoadSnapshot(fileInputStream);
    }
  }

  /////////////////////////////// hashCode & equals ///////////////////////////////

  @Override
  public int hashCode() {
    return Objects.hash(pipePluginMetaKeeper, pipePluginExecutableManager);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PipePluginInfo other = (PipePluginInfo) obj;
    return Objects.equals(pipePluginExecutableManager, other.pipePluginExecutableManager)
        && Objects.equals(pipePluginMetaKeeper, other.pipePluginMetaKeeper);
  }

  @Override
  public String toString() {
    return "PipePluginInfo [pipePluginMetaKeeper="
        + pipePluginMetaKeeper
        + ", pipePluginExecutableManager="
        + pipePluginExecutableManager
        + "]";
  }
}
