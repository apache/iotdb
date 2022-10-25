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
import org.apache.iotdb.commons.executable.ExecutableResource;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class UDFExecutableManager extends ExecutableManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFExecutableManager.class);

  private UDFExecutableManager(String temporaryLibRoot, String udfLibRoot) {
    super(temporaryLibRoot, udfLibRoot);
  }

  public void removeUDFJarFromExtLibDir(String functionName) {
    FileUtils.deleteQuietly(getDirUnderLibRootByName(functionName));
  }

  private void downloadExecutableResources(String functionName, String className, List<String> uris)
      throws UDFManagementException {
    if (uris.isEmpty()) {
      return;
    }

    try {
      final ExecutableResource resource = request(uris);
      try {
        removeUDFJarFromExtLibDir(functionName);
        moveTempDirToExtLibDir(resource, functionName);
      } catch (Exception innerException) {
        removeUDFJarFromExtLibDir(functionName);
        removeFromTemporaryLibRoot(resource);
        throw innerException;
      }
    } catch (Exception outerException) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s) because failed to fetch UDF executables(%s)",
              functionName.toUpperCase(), className, uris);
      LOGGER.warn(errorMessage, outerException);
      throw new UDFManagementException(errorMessage, outerException);
    }
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
      INSTANCE = new UDFExecutableManager(temporaryLibRoot, udfLibRoot);
    }
    return INSTANCE;
  }

  public static UDFExecutableManager getInstance() {
    return INSTANCE;
  }
}
