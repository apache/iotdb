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
import org.apache.iotdb.commons.pipe.agent.plugin.meta.ConfigNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.agent.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin.DO_NOTHING_PROCESSOR;
import static org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin.IOTDB_EXTRACTOR;
import static org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR;

public class PipePluginInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginInfo.class);
  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();
  private static final String SNAPSHOT_FILE_NAME = "pipe_plugin_info.bin";

  private final ReentrantLock pipePluginInfoLock = new ReentrantLock();

  private final ConfigNodePipePluginMetaKeeper pipePluginMetaKeeper;
  private final PipePluginExecutableManager pipePluginExecutableManager;

  public PipePluginInfo() throws IOException {
    this.pipePluginMetaKeeper = new ConfigNodePipePluginMetaKeeper();
    this.pipePluginExecutableManager =
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

  /**
   * @return true if the pipe plugin is already created and the isSetIfNotExistsCondition is true,
   *     false otherwise
   * @throws PipeException if the pipe plugin is already created and the isSetIfNotExistsCondition
   *     is false
   */
  public boolean validateBeforeCreatingPipePlugin(
      final String pluginName, final boolean isSetIfNotExistsCondition) {
    // both build-in and user defined pipe plugin should be unique
    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      if (isSetIfNotExistsCondition) {
        return true;
      }
      throw new PipeException(
          String.format(
              "Failed to create PipePlugin [%s], the same name PipePlugin has been created",
              pluginName));
    }
    return false;
  }

  /**
   * @return true if the pipe plugin is not created and the isSetIfExistsCondition is true, false
   *     otherwise
   * @throws PipeException if the pipe plugin is not created and the isSetIfExistsCondition is false
   */
  public boolean validateBeforeDroppingPipePlugin(
      final String pluginName, final boolean isSetIfExistsCondition) {
    if (!pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      if (isSetIfExistsCondition) {
        return true;
      }
      throw new PipeException(
          String.format(
              "Failed to drop PipePlugin [%s], this PipePlugin has not been created", pluginName));
    }
    if (pipePluginMetaKeeper.getPipePluginMeta(pluginName).isBuiltin()) {
      throw new PipeException(
          String.format(
              "Failed to drop PipePlugin [%s], the PipePlugin is a built-in PipePlugin",
              pluginName));
    }
    return false;
  }

  public boolean isJarNeededToBeSavedWhenCreatingPipePlugin(final String jarName) {
    return !pipePluginMetaKeeper.containsJar(jarName);
  }

  public void checkPipePluginExistence(
      final Map<String, String> extractorAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> connectorAttributes) {
    final PipeParameters extractorParameters = new PipeParameters(extractorAttributes);
    final String extractorPluginName =
        extractorParameters.getStringOrDefault(
            Arrays.asList(PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY),
            IOTDB_EXTRACTOR.getPipePluginName());
    if (!pipePluginMetaKeeper.containsPipePlugin(extractorPluginName)) {
      final String exceptionMessage =
          String.format(
              "Failed to create or alter pipe, the pipe extractor plugin %s does not exist",
              extractorPluginName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeParameters processorParameters = new PipeParameters(processorAttributes);
    final String processorPluginName =
        processorParameters.getStringOrDefault(
            PipeProcessorConstant.PROCESSOR_KEY, DO_NOTHING_PROCESSOR.getPipePluginName());
    if (!pipePluginMetaKeeper.containsPipePlugin(processorPluginName)) {
      final String exceptionMessage =
          String.format(
              "Failed to create or alter pipe, the pipe processor plugin %s does not exist",
              processorPluginName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }

    final PipeParameters connectorParameters = new PipeParameters(connectorAttributes);
    final String connectorPluginName =
        connectorParameters.getStringOrDefault(
            Arrays.asList(PipeConnectorConstant.CONNECTOR_KEY, PipeConnectorConstant.SINK_KEY),
            IOTDB_THRIFT_CONNECTOR.getPipePluginName());
    if (!pipePluginMetaKeeper.containsPipePlugin(connectorPluginName)) {
      final String exceptionMessage =
          String.format(
              "Failed to create or alter pipe, the pipe connector plugin %s does not exist",
              connectorPluginName);
      LOGGER.warn(exceptionMessage);
      throw new PipeException(exceptionMessage);
    }
  }

  /////////////////////////////// Pipe Plugin Management ///////////////////////////////

  public TSStatus createPipePlugin(final CreatePipePluginPlan createPipePluginPlan) {
    try {
      final PipePluginMeta pipePluginMeta = createPipePluginPlan.getPipePluginMeta();

      // try to drop the old pipe plugin if exists to reduce the effect of the inconsistency
      dropPipePlugin(new DropPipePluginPlan(pipePluginMeta.getPluginName()));

      pipePluginMetaKeeper.addPipePluginMeta(pipePluginMeta.getPluginName(), pipePluginMeta);
      pipePluginMetaKeeper.addJarNameAndMd5(
          pipePluginMeta.getJarName(), pipePluginMeta.getJarMD5());

      if (createPipePluginPlan.getJarFile() != null) {
        pipePluginExecutableManager.savePluginToInstallDir(
            ByteBuffer.wrap(createPipePluginPlan.getJarFile().getValues()),
            createPipePluginPlan.getPipePluginMeta().getPluginName(),
            pipePluginMeta.getJarName());
      }

      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (final Exception e) {
      final String errorMessage =
          String.format(
              "Failed to execute createPipePlugin(%s) on config nodes, because of %s",
              createPipePluginPlan.getPipePluginMeta().getPluginName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public TSStatus dropPipePlugin(final DropPipePluginPlan dropPipePluginPlan) {
    final String pluginName = dropPipePluginPlan.getPluginName();

    if (pipePluginMetaKeeper.containsPipePlugin(pluginName)) {
      String jarName = pipePluginMetaKeeper.getPipePluginMeta(pluginName).getJarName();
      pipePluginMetaKeeper.removeJarNameAndMd5IfPossible(jarName);
      pipePluginMetaKeeper.removePipePluginMeta(pluginName);

      try {
        pipePluginExecutableManager.removePluginFileUnderLibRoot(pluginName, jarName);
      } catch (IOException e) {
        final String errorMessage =
            String.format(
                "Failed to execute dropPipePlugin(%s) on config nodes, because of %s",
                pluginName, e);
        LOGGER.warn(errorMessage, e);
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(errorMessage);
      }
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public DataSet showPipePlugins() {
    return new PipePluginTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        Arrays.asList(pipePluginMetaKeeper.getAllPipePluginMeta()));
  }

  public JarResp getPipePluginJar(final GetPipePluginJarPlan getPipePluginJarPlan) {
    try {
      final List<ByteBuffer> jarList = new ArrayList<>();
      final PipePluginExecutableManager manager = PipePluginExecutableManager.getInstance();

      for (final String jarName : getPipePluginJarPlan.getJarNames()) {
        String pluginName = pipePluginMetaKeeper.getPluginNameByJarName(jarName);
        if (pluginName == null) {
          throw new PipeException(String.format("%s does not exist", jarName));
        }

        String jarPath = manager.getPluginInstallPathV2(pluginName, jarName);

        boolean isJarExistedInV2Dir = Files.exists(Paths.get(jarPath));
        if (!isJarExistedInV2Dir) {
          jarPath = manager.getPluginInstallPathV1(jarName);
        }

        if (!Files.exists(Paths.get(jarPath))) {
          throw new PipeException(String.format("%s does not exist", jarName));
        }

        ByteBuffer byteBuffer = ExecutableManager.transferToBytebuffer(jarPath);
        if (!isJarExistedInV2Dir) {
          pipePluginExecutableManager.savePluginToInstallDir(
              byteBuffer.duplicate(), pluginName, jarName);
        }

        jarList.add(byteBuffer);
      }
      return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
    } catch (final Exception e) {
      LOGGER.error("Get PipePlugin_Jar failed", e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get PipePlugin_Jar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
  }

  /////////////////////////////// Snapshot Processor ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(final File snapshotDir) throws IOException {
    acquirePipePluginInfoLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (snapshotFile.exists() && snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to take snapshot, because snapshot file [{}] is already exist.",
            snapshotFile.getAbsolutePath());
        return false;
      }

      try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
        pipePluginMetaKeeper.processTakeSnapshot(fileOutputStream);
        fileOutputStream.getFD().sync();
      }
      return true;
    } finally {
      releasePipePluginInfoLock();
    }
  }

  @Override
  public void processLoadSnapshot(final File snapshotDir) throws IOException {
    acquirePipePluginInfoLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (!snapshotFile.exists() || !snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to load snapshot,snapshot file [{}] is not exist.",
            snapshotFile.getAbsolutePath());
        return;
      }

      try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
        pipePluginMetaKeeper.processLoadSnapshot(fileInputStream);
      }
    } finally {
      releasePipePluginInfoLock();
    }
  }

  /////////////////////////////// hashCode & equals ///////////////////////////////

  @Override
  public int hashCode() {
    return Objects.hash(pipePluginMetaKeeper, pipePluginExecutableManager);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final PipePluginInfo other = (PipePluginInfo) obj;
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
