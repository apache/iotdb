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

package org.apache.iotdb.commons.pipe.agent.plugin.service;

import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PipePluginExecutableManager extends ExecutableManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginExecutableManager.class);

  public PipePluginExecutableManager(String temporaryLibRoot, String libRoot) {
    super(temporaryLibRoot, libRoot);
  }

  public boolean isLocalJarMatched(PipePluginMeta pipePluginMeta) throws PipeException {
    final String pluginName = pipePluginMeta.getPluginName();
    final String md5FilePath = pluginName + ".txt";

    if (hasFileUnderTemporaryRoot(md5FilePath)) {
      try {
        return readTextFromFileUnderTemporaryRoot(md5FilePath).equals(pipePluginMeta.getJarMD5());
      } catch (IOException e) {
        // If meet error when reading md5 from txt, we need to compute it again
        LOGGER.error("Failed to read md5 from txt file for pipe plugin {}", pluginName, e);
      }
    }

    try {
      final String md5 =
          DigestUtils.md5Hex(
              Files.newInputStream(
                  Paths.get(getPluginInstallPathV2(pluginName, pipePluginMeta.getJarName()))));
      // Save the md5 in a txt under trigger temporary lib
      saveTextAsFileUnderTemporaryRoot(md5, md5FilePath);
      return md5.equals(pipePluginMeta.getJarMD5());
    } catch (IOException e) {
      String errorMessage =
          String.format(
              "Failed to registered function %s, because "
                  + "error occurred when trying to compute md5 of jar file for function %s ",
              pluginName, pluginName);
      LOGGER.warn(errorMessage, e);
      throw new PipeException(errorMessage);
    }
  }

  /////////////////////////////// Singleton Instance ///////////////////////////////

  private static PipePluginExecutableManager instance = null;

  public static synchronized PipePluginExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String libRoot) throws IOException {
    if (instance == null) {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot + File.separator + INSTALL_DIR);
      instance = new PipePluginExecutableManager(temporaryLibRoot, libRoot);
    }
    return instance;
  }

  public static PipePluginExecutableManager getInstance() {
    return instance;
  }

  public boolean hasPluginFileUnderInstallDir(String pluginName, String fileName) {
    return Files.exists(Paths.get(getPluginInstallPathV2(pluginName, fileName)));
  }

  public String getPluginsDirPath(String pluginName) {
    return this.libRoot + File.separator + INSTALL_DIR + File.separator + pluginName.toUpperCase();
  }

  public void removePluginFileUnderLibRoot(String pluginName, String fileName) throws IOException {
    String pluginPath = getPluginInstallPathV2(pluginName, fileName);
    Path path = Paths.get(pluginPath);
    Files.deleteIfExists(path);
    Files.deleteIfExists(path.getParent());
  }

  public String getPluginInstallPathV2(String pluginName, String fileName) {
    return this.libRoot
        + File.separator
        + INSTALL_DIR
        + File.separator
        + pluginName.toUpperCase()
        + File.separator
        + fileName;
  }

  public String getPluginInstallPathV1(String fileName) {
    return this.libRoot + File.separator + INSTALL_DIR + File.separator + fileName;
  }

  /**
   * @param byteBuffer file
   * @param pluginName
   * @param fileName Absolute Path will be libRoot + File_Separator + INSTALL_DIR + File_Separator +
   *     pluginName + File_Separator + fileName
   */
  public void savePluginToInstallDir(ByteBuffer byteBuffer, String pluginName, String fileName)
      throws IOException {
    String destination = getPluginInstallPathV2(pluginName, fileName);
    saveToDir(byteBuffer, destination);
  }
}
