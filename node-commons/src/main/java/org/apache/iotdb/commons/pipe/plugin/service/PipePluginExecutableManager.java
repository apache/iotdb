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

import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PipePluginExecutableManager extends ExecutableManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginExecutableManager.class);

  public PipePluginExecutableManager(String temporaryLibRoot, String libRoot) {
    super(temporaryLibRoot, libRoot);
  }

  public boolean isLocalJarMatched(PipePluginMeta pipePluginMeta) throws PipeManagementException {
    final String pluginName = pipePluginMeta.getPluginName();
    final String md5FilePath = pluginName + ".txt";

    if (hasFileUnderInstallDir(md5FilePath)) {
      try {
        return readTextFromFileUnderTemporaryRoot(md5FilePath).equals(pipePluginMeta.getJarMD5());
      } catch (IOException e) {
        // if meet error when reading md5 from txt, we need to compute it again
        LOGGER.error("Failed to read md5 from txt file for pipe plugin {}", pluginName, e);
      }
    }

    try {
      final String md5 =
          DigestUtils.md5Hex(
              Files.newInputStream(
                  Paths.get(getInstallDir() + File.separator + pipePluginMeta.getJarName())));
      // save the md5 in a txt under trigger temporary lib
      saveTextAsFileUnderTemporaryRoot(md5, md5FilePath);
      return md5.equals(pipePluginMeta.getJarMD5());
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

  /////////////////////////////// Singleton Instance ///////////////////////////////

  private static PipePluginExecutableManager INSTANCE = null;

  public static synchronized PipePluginExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String libRoot) throws IOException {
    if (INSTANCE == null) {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot + File.separator + INSTALL_DIR);
      INSTANCE = new PipePluginExecutableManager(temporaryLibRoot, libRoot);
    }
    return INSTANCE;
  }

  public static PipePluginExecutableManager getInstance() {
    return INSTANCE;
  }
}
