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

package org.apache.iotdb.commons.pipe.plugin.service;

import org.apache.iotdb.commons.pipe.plugin.PipePluginInformation;
import org.apache.iotdb.commons.pipe.plugin.PipePluginTable;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginManagementService.class);

  private final ReentrantLock lock;

  private final PipePluginTable pipePluginTable;

  private PipePluginManagementService() {
    lock = new ReentrantLock();
    pipePluginTable = new PipePluginTable();
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public void validate(PipePluginInformation pipePluginInformation) {
    try {
      acquireLock();
      checkIfRegistered(pipePluginInformation);
    } finally {
      releaseLock();
    }
  }

  public void register(PipePluginInformation pipePluginInformation, ByteBuffer jarFile)
      throws Exception {
    try {
      acquireLock();
      checkIfRegistered(pipePluginInformation);
      saveJarFile(pipePluginInformation.getJarName(), jarFile);
      doRegister(pipePluginInformation);
    } finally {
      releaseLock();
    }
  }

  private void checkIfRegistered(PipePluginInformation pipePluginInformation)
      throws PipeManagementException {
    String pluginName = pipePluginInformation.getPluginName();
    String className = pipePluginInformation.getClassName();
    PipePluginInformation information = pipePluginTable.getPipePluginInformation(pluginName);
    if (information == null) {
      return;
    }

    if (PipePluginExecutableManager.getInstance()
            .hasFileUnderInstallDir(pipePluginInformation.getJarName())
        && !isLocalJarConflicted(pipePluginInformation)) {
      String errMsg =
          String.format(
              "Failed to register PipePlugin %s, "
                  + "because existed md5 of jar file for pipe plugin %s is different from the new jar file.",
              pluginName, pluginName);
      LOGGER.error(errMsg);
      throw new PipeManagementException(errMsg);
    }
  }

  public boolean isLocalJarConflicted(PipePluginInformation pipePluginInformation)
      throws PipeManagementException {
    String pluginName = pipePluginInformation.getPluginName();
    String existedMd5 = "";
    String md5FilePath = pluginName + ".txt";

    // if meet error when reading md5 from txt, we need to compute it again
    boolean hasComputed = false;
    if (PipePluginExecutableManager.getInstance().hasFileUnderInstallDir(md5FilePath)) {
      try {
        existedMd5 =
            PipePluginExecutableManager.getInstance()
                .readTextFromFileUnderTemporaryRoot(md5FilePath);
        hasComputed = true;
      } catch (IOException e) {
        LOGGER.error("Failed to read md5 from txt file for pipe plugin {}", pluginName, e);
      }
    }
    if (!hasComputed) {
      try {
        existedMd5 =
            DigestUtils.md5Hex(
                Files.newInputStream(
                    Paths.get(
                        PipePluginExecutableManager.getInstance().getInstallDir()
                            + File.separator
                            + pipePluginInformation.getJarName())));
        // save the md5 in a txt under trigger temporary lib
        PipePluginExecutableManager.getInstance()
            .saveTextAsFileUnderTemporaryRoot(existedMd5, md5FilePath);
      } catch (IOException e) {
        String errorMessage =
            String.format(
                "Failed to registered function %s, "
                    + "because error occurred when trying to compute md5 of jar file for function %s ",
                pluginName, pluginName);
        LOGGER.warn(errorMessage, e);
        throw new PipeManagementException(errorMessage);
      }
    }
    return !existedMd5.equals(pipePluginInformation.getJarMD5());
  }

  private void saveJarFile(String jarName, ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer != null) {
      PipePluginExecutableManager.getInstance().saveToInstallDir(byteBuffer, jarName);
    }
  }

  public void doRegister(PipePluginInformation pipePluginInformation)
      throws PipeManagementException {
    String pluginName = pipePluginInformation.getPluginName();
    String className = pipePluginInformation.getClassName();
    try {
      PipePluginClassLoader currentActiveClassLoader =
          PipePluginClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      Class<?> pluginClass = Class.forName(className, true, currentActiveClassLoader);

      // ensure that it is a PipePlugin class
      PipePlugin pipePlugin = (PipePlugin) pluginClass.getDeclaredConstructor().newInstance();
      pipePluginTable.addPipePluginInformation(pluginName, pipePluginInformation);
      pipePluginTable.addPluginAndClass(pluginName, pluginClass);
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
    for (PipePluginInformation information : getAllPipePluginInformation()) {
      pipePluginTable.updatePluginClass(information, activeClassLoader);
    }
  }

  public void deregister(String pluginName, boolean needToDeleteJar) throws Exception {
    try {
      acquireLock();
      PipePluginInformation information = pipePluginTable.getPipePluginInformation(pluginName);
      if (information == null) {
        return;
      }
      pipePluginTable.removePipePluginInformation(pluginName);
      pipePluginTable.removePluginClass(pluginName);
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
    PipePluginInformation information = pipePluginTable.getPipePluginInformation(pluginName);
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
          pipePluginTable.getPluginClass(pluginName).getDeclaredConstructor().newInstance();
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

  public boolean isPipePlugin(String pluginName)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
    return pipePluginTable.getPluginClass(pluginName).getDeclaredConstructor().newInstance()
        instanceof PipePlugin;
  }

  public PipePluginInformation[] getAllPipePluginInformation() {
    return pipePluginTable.getAllPipePluginInformation();
  }

  private static class PipePluginManagementServiceHolder {
    private static final PipePluginManagementService INSTANCE = new PipePluginManagementService();
  }

  public static PipePluginManagementService getInstance() {
    return PipePluginManagementServiceHolder.INSTANCE;
  }
}
