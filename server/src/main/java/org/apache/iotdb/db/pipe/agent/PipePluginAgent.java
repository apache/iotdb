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

package org.apache.iotdb.db.pipe.agent;

import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoader;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoaderManager;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginAgent.class);

  private final ReentrantLock lock = new ReentrantLock();

  private final DataNodePipePluginMetaKeeper pipePluginMetaKeeper =
      new DataNodePipePluginMetaKeeper();

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
      checkIfRegistered(pipePluginMeta);
      saveJarFileIfNeeded(pipePluginMeta.getJarName(), jarFile);
      doRegister(pipePluginMeta);
    } finally {
      releaseLock();
    }
  }

  private void checkIfRegistered(PipePluginMeta pipePluginMeta) throws PipeManagementException {
    final String pluginName = pipePluginMeta.getPluginName();
    final PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);
    if (information == null) {
      return;
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
      throw new PipeManagementException(errMsg);
    }
  }

  private void saveJarFileIfNeeded(String jarName, ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer != null) {
      PipePluginExecutableManager.getInstance().saveToInstallDir(byteBuffer, jarName);
    }
  }

  private void doRegister(PipePluginMeta pipePluginMeta) throws PipeManagementException {
    final String pluginName = pipePluginMeta.getPluginName();
    final String className = pipePluginMeta.getClassName();

    try {
      final PipePluginClassLoader currentActiveClassLoader =
          PipePluginClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      final Class<?> pluginClass = Class.forName(className, true, currentActiveClassLoader);
      // ensure that it is a PipePlugin class
      PipePlugin ignored = (PipePlugin) pluginClass.getDeclaredConstructor().newInstance();

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
      throw new PipeManagementException(errorMessage);
    }
  }

  private void updateAllRegisteredClasses(PipePluginClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (PipePluginMeta information : pipePluginMetaKeeper.getAllPipePluginMeta()) {
      pipePluginMetaKeeper.updatePluginClass(information, activeClassLoader);
    }
  }

  public void deregister(String pluginName, boolean needToDeleteJar) throws Exception {
    acquireLock();
    try {
      PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);
      if (information == null) {
        return;
      }

      pipePluginMetaKeeper.removePipePluginMeta(pluginName);
      pipePluginMetaKeeper.removePluginClass(pluginName);

      if (needToDeleteJar) {
        PipePluginExecutableManager.getInstance().removeFileUnderLibRoot(information.getJarName());
        PipePluginExecutableManager.getInstance()
            .removeFileUnderTemporaryRoot(pluginName.toUpperCase() + ".txt");
      }
    } finally {
      releaseLock();
    }
  }

  public PipePlugin reflect(String pluginName) {
    PipePluginMeta information = pipePluginMetaKeeper.getPipePluginMeta(pluginName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect PipePlugin instance, because PipePlugin %s has not been registered.",
              pluginName.toUpperCase());
      LOGGER.warn(errorMessage);
      throw new RuntimeException(errorMessage);
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
      throw new RuntimeException(errorMessage);
    }
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private static class PipePluginAgentServiceHolder {
    private static final PipePluginAgent INSTANCE = new PipePluginAgent();
  }

  public static PipePluginAgent getInstance() {
    return PipePluginAgentServiceHolder.INSTANCE;
  }
}
