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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class UDFExecutableManager extends ExecutableManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFExecutableManager.class);

  private UDFExecutableManager(String temporaryLibRoot, String udfLibRoot) {
    super(temporaryLibRoot, udfLibRoot);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static UDFExecutableManager INSTANCE = null;

  public static synchronized UDFExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String udfLibRoot) throws IOException {
    if (INSTANCE == null) {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(udfLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(udfLibRoot + File.separator + INSTALL_DIR);
      INSTANCE = new UDFExecutableManager(temporaryLibRoot, udfLibRoot);
    }
    return INSTANCE;
  }

  /** check whether local jar is correct according to md5 */
  public boolean isLocalJarConflicted(UDFInformation udfInformation) throws UDFManagementException {
    String functionName = udfInformation.getFunctionName();
    // A jar with the same name exists, we need to check md5
    String existedMd5 = "";
    String md5FilePath = functionName + ".txt";

    // if meet error when reading md5 from txt, we need to compute it again
    boolean hasComputed = false;
    if (hasFileUnderTemporaryRoot(md5FilePath)) {
      try {
        existedMd5 = readTextFromFileUnderTemporaryRoot(md5FilePath);
        hasComputed = true;
      } catch (IOException e) {
        LOGGER.warn("Error occurred when trying to read md5 of {}", md5FilePath);
      }
    }
    if (!hasComputed) {
      try {
        existedMd5 =
            DigestUtils.md5Hex(
                Files.newInputStream(
                    Paths.get(
                        UDFExecutableManager.getInstance().getInstallDir()
                            + File.separator
                            + udfInformation.getJarName())));
        // save the md5 in a txt under UDF temporary lib
        saveTextAsFileUnderTemporaryRoot(existedMd5, md5FilePath);
      } catch (IOException e) {
        String errorMessage =
            String.format(
                "Failed to registered function %s, "
                    + "because error occurred when trying to compute md5 of jar file for function %s ",
                functionName, functionName);
        LOGGER.warn(errorMessage, e);
        throw new UDFManagementException(errorMessage);
      }
    }
    return !existedMd5.equals(udfInformation.getJarMD5());
  }

  public static UDFExecutableManager getInstance() {
    return INSTANCE;
  }
}
