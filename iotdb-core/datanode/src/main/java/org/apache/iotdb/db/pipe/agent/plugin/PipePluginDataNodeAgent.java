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

import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoader;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.db.pipe.agent.plugin.dataregion.PipeDataRegionPluginAgent;
import org.apache.iotdb.db.pipe.agent.plugin.schemaregion.PipeSchemaRegionPluginAgent;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginDataNodeAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginDataNodeAgent.class);

  private final DataNodePipePluginMetaKeeper pipePluginMetaKeeper;

  private final PipeDataRegionPluginAgent dataRegionPluginAgent;
  private final PipeSchemaRegionPluginAgent schemaRegionPluginAgent;

  private final ReentrantLock lock;

  public PipePluginDataNodeAgent() {
    pipePluginMetaKeeper = new DataNodePipePluginMetaKeeper();

    dataRegionPluginAgent = new PipeDataRegionPluginAgent(pipePluginMetaKeeper);
    schemaRegionPluginAgent = new PipeSchemaRegionPluginAgent(null);

    lock = new ReentrantLock();
  }

  public PipeDataRegionPluginAgent dataRegion() {
    return dataRegionPluginAgent;
  }

  public PipeSchemaRegionPluginAgent schemaRegion() {
    return schemaRegionPluginAgent;
  }

  /////////////////////////////// Pipe Plugin Management ///////////////////////////////

  public void register(PipePluginMeta pipePluginMeta, ByteBuffer jarFile)
      throws IOException, PipeException {
    lock.lock();
    try {
      // try to deregister first to avoid inconsistent state
      deregister(pipePluginMeta.getPluginName(), false);

      // register process from here
      checkIfRegistered(pipePluginMeta);
      saveJarFileIfNeeded(pipePluginMeta.getJarName(), jarFile);
      doRegister(pipePluginMeta);
    } finally {
      lock.unlock();
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
              "Failed to register PipePlugin %s, because "
                  + "the given PipePlugin name is the same as a built-in PipePlugin name.",
              pluginName);
      LOGGER.warn(errorMessage);
      throw new PipeException(errorMessage);
    }

    if (PipePluginExecutableManager.getInstance()
            .hasFileUnderInstallDir(pipePluginMeta.getJarName())
        && !PipePluginExecutableManager.getInstance().isLocalJarMatched(pipePluginMeta)) {
      String errMsg =
          String.format(
              "Failed to register PipePlugin %s, because "
                  + "existed md5 of jar file for pipe plugin %s "
                  + "is different from the new jar file.",
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
              "Failed to register PipePlugin %s(%s), because "
                  + "its instance can not be constructed successfully. Exception: %s",
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
    lock.lock();
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
    } catch (IOException e) {
      throw new PipeException(e.getMessage(), e);
    } finally {
      lock.unlock();
    }
  }

  // TODO: validate pipe plugin attributes for config node
  public void validate(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    dataRegionPluginAgent.validate(
        pipeName, extractorAttributes, processorAttributes, connectorAttributes);
    // FIXME: Currently we comment out the following code to avoid instantiating
    // `IoTDBMetaConnector` and `IoTDBMetaExtractor`.
    // schemaRegionPluginAgent.validate(pipeName, extractorAttributes, processorAttributes,
    // connectorAttributes);
  }
}
