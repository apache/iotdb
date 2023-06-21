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

package org.apache.iotdb.db.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoader;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginAgent.class);

  private final ReentrantLock lock = new ReentrantLock();

  private final DataNodePipePluginMetaKeeper pipePluginMetaKeeper;

  public PipePluginAgent() {
    this.pipePluginMetaKeeper = new DataNodePipePluginMetaKeeper();
  }

  /////////////////////////////// Lock ///////////////////////////////

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  /////////////////////////////// Pipe Plugin Management ///////////////////////////////

  public void register(PipePluginMeta pipePluginMeta, ByteBuffer jarFile) throws Exception {
    acquireLock();
    try {
      // try to deregister first to avoid inconsistent state
      deregister(pipePluginMeta.getPluginName(), false);

      // register process from here
      checkIfRegistered(pipePluginMeta);
      saveJarFileIfNeeded(pipePluginMeta.getJarName(), jarFile);
      doRegister(pipePluginMeta);
    } finally {
      releaseLock();
    }
  }

  private void checkIfRegistered(PipePluginMeta pipePluginMeta) throws PipeException {
    final String pluginName = pipePluginMeta.getPluginName();
    final PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);
    if (information == null) {
      return;
    }

    if (information.isBuiltin()) {
      String errorMessage =
          String.format(
              "Failed to register PipePlugin %s, because the given PipePlugin name is the same as a built-in PipePlugin name.",
              pluginName);
      LOGGER.warn(errorMessage);
      throw new PipeException(errorMessage);
    }

    if (PipePluginExecutableManager.getInstance()
            .hasFileUnderInstallDir(pipePluginMeta.getJarName())
        && !PipePluginExecutableManager.getInstance().isLocalJarMatched(pipePluginMeta)) {
      String errMsg =
          String.format(
              "Failed to register PipePlugin %s, "
                  + "because existed md5 of jar file for pipe plugin %s is different from the new jar file.",
              pluginName, pluginName);
      LOGGER.warn(errMsg);
      throw new PipeException(errMsg);
    }

    // if the pipe plugin is already registered and the jar file is the same, do nothing
    // we allow users to register the same pipe plugin multiple times without any error
  }

  private void saveJarFileIfNeeded(String jarName, ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer != null) {
      PipePluginExecutableManager.getInstance().saveToInstallDir(byteBuffer, jarName);
    }
  }

  /**
   * Register a PipePlugin to the system without any meta checks. The PipePlugin will be loaded by
   * the PipePluginClassLoader and its instance will be created to ensure that it can be loaded.
   *
   * @param pipePluginMeta the meta information of the PipePlugin
   * @throws PipeException if the PipePlugin can not be loaded or its instance can not be created
   */
  public void doRegister(PipePluginMeta pipePluginMeta) throws PipeException {
    final String pluginName = pipePluginMeta.getPluginName();
    final String className = pipePluginMeta.getClassName();

    try {
      final PipePluginClassLoader currentActiveClassLoader =
          PipePluginClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      final Class<?> pluginClass = Class.forName(className, true, currentActiveClassLoader);

      @SuppressWarnings("unused") // ensure that it is a PipePlugin class
      final PipePlugin ignored = (PipePlugin) pluginClass.getDeclaredConstructor().newInstance();

      pipePluginMetaKeeper.addPipePluginMeta(pluginName, pipePluginMeta);
      pipePluginMetaKeeper.addPluginAndClass(pluginName, pluginClass);
    } catch (IOException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException
        | ClassCastException e) {
      String errorMessage =
          String.format(
              "Failed to register PipePlugin %s(%s), because its instance can not be constructed successfully. Exception: %s",
              pluginName.toUpperCase(), className, e);
      LOGGER.warn(errorMessage, e);
      throw new PipeException(errorMessage);
    }
  }

  private void updateAllRegisteredClasses(PipePluginClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (PipePluginMeta information : pipePluginMetaKeeper.getAllPipePluginMeta()) {
      pipePluginMetaKeeper.updatePluginClass(information, activeClassLoader);
    }
  }

  public void deregister(String pluginName, boolean needToDeleteJar) throws PipeException {
    acquireLock();
    try {
      final PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);

      if (information != null && information.isBuiltin()) {
        String errorMessage =
            String.format("Failed to deregister builtin PipePlugin %s.", pluginName);
        LOGGER.warn(errorMessage);
        throw new PipeException(errorMessage);
      }

      // remove anyway
      pipePluginMetaKeeper.removePipePluginMeta(pluginName);
      pipePluginMetaKeeper.removePluginClass(pluginName);

      // if it is needed to delete jar file of the pipe plugin, delete both jar file and md5
      if (information != null && needToDeleteJar) {
        PipePluginExecutableManager.getInstance().removeFileUnderLibRoot(information.getJarName());
        PipePluginExecutableManager.getInstance()
            .removeFileUnderTemporaryRoot(pluginName.toUpperCase() + ".txt");
      }
    } finally {
      releaseLock();
    }
  }

  public PipeCollector reflectCollector(PipeParameters collectorParameters) {
    return (PipeCollector)
        reflect(
            collectorParameters.getStringOrDefault(
                PipeCollectorConstant.COLLECTOR_KEY,
                BuiltinPipePlugin.IOTDB_COLLECTOR.getPipePluginName()));
  }

  public PipeProcessor reflectProcessor(PipeParameters processorParameters) {
    return (PipeProcessor)
        reflect(
            processorParameters.getStringOrDefault(
                PipeProcessorConstant.PROCESSOR_KEY,
                BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName()));
  }

  public PipeConnector reflectConnector(PipeParameters connectorParameters) {
    if (!connectorParameters.hasAttribute(PipeConnectorConstant.CONNECTOR_KEY)) {
      throw new PipeException(
          "Failed to reflect PipeConnector instance because 'connector' is not specified in the parameters.");
    }
    return (PipeConnector)
        reflect(connectorParameters.getString(PipeConnectorConstant.CONNECTOR_KEY));
  }

  private PipePlugin reflect(String pluginName) {
    PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect PipePlugin instance, because PipePlugin %s has not been registered.",
              pluginName.toUpperCase());
      LOGGER.warn(errorMessage);
      throw new PipeException(errorMessage);
    }

    try {
      return (PipePlugin)
          pipePluginMetaKeeper.getPluginClass(pluginName).getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException e) {
      String errorMessage =
          String.format(
              "Failed to reflect PipePlugin %s(%s) instance, because %s",
              pluginName, information.getClassName(), e);
      LOGGER.warn(errorMessage, e);
      throw new PipeException(errorMessage);
    }
  }
}
