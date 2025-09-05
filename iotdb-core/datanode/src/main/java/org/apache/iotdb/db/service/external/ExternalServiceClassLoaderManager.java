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

package org.apache.iotdb.db.service.external;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ExternalServiceClassLoaderManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExternalServiceClassLoaderManager.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static ExternalServiceClassLoaderManager INSTANCE;

  /** The dir that stores jar files. */
  private final String libRoot;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE SERVICE or after the user executes DROP SERVICE. Therefore, we need to
   * continuously maintain the activeClassLoader so that the classes it loads are always up-to-date.
   */
  @SuppressWarnings("squid:S3077")
  private volatile ExternalServiceClassLoader activeClassLoader;

  private ExternalServiceClassLoaderManager(String libRoot) throws IOException {
    this.libRoot = libRoot;
    LOGGER.info("Service lib root: {}", libRoot);
    activeClassLoader = new ExternalServiceClassLoader(libRoot);
  }

  public ExternalServiceClassLoader updateAndGetActiveClassLoader() throws IOException {
    ExternalServiceClassLoader deprecatedClassLoader = activeClassLoader;
    activeClassLoader = new ExternalServiceClassLoader(libRoot);
    deprecatedClassLoader.close();
    return activeClassLoader;
  }

  private static class ExternalServiceClassLoaderManagerHolder {
    private static final ExternalServiceClassLoaderManager INSTANCE;

    static {
      try {
        SystemFileFactory.INSTANCE.makeDirIfNecessary(CONFIG.getServiceDir());
        INSTANCE = new ExternalServiceClassLoaderManager(CONFIG.getServiceDir());
      } catch (IOException e) {
        LOGGER.error("Failed to initialize ExternalServiceClassLoaderManager.", e);
        throw new RuntimeException(e);
      }
    }
  }

  public static ExternalServiceClassLoaderManager getInstance() {
    return ExternalServiceClassLoaderManagerHolder.INSTANCE;
  }
}
