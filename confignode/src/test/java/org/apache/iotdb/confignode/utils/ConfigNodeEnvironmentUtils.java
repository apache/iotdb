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
package org.apache.iotdb.confignode.utils;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.service.ConfigNode;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

/** Test environment for ConfigNode UT and IT */
public class ConfigNodeEnvironmentUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeEnvironmentUtils.class);

  private static ConfigNode daemon;

  @TestOnly
  public static void envSetUp() {
    LOGGER.debug("ConfigNodeEnvironmentUtils setup...");

    if (daemon == null) {
      daemon = new ConfigNode();
    }

    try {
      daemon.active();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    createAllDir();
  }

  @TestOnly
  public static void cleanEnv() {
    LOGGER.debug("ConfigNodeEnvironmentUtils cleanEnv...");

    if (daemon != null) {
      daemon.stop();
      daemon = null;
    }

    // delete all directory
    cleanAllDir();
  }

  @TestOnly
  public static void stopDaemon() {
    if (daemon != null) {
      daemon.stop();
    }
  }

  @TestOnly
  public static void shutdownDaemon() {
    if (daemon != null) {
      daemon.shutdown();
    }
  }

  @TestOnly
  public static void activeDaemon() {
    if (daemon != null) {
      daemon.active();
    }
  }

  @TestOnly
  public static void reactiveDaemon() {
    if (daemon == null) {
      daemon = new ConfigNode();
      daemon.active();
    } else {
      activeDaemon();
    }
  }

  @TestOnly
  public static void restartDaemon() {
    shutdownDaemon();
    stopDaemon();
    reactiveDaemon();
  }

  private static void createAllDir() {
    createDir(ConfigNodeConstant.CONF_DIR);
    createDir(ConfigNodeConstant.DATA_DIR);
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    if (!file.mkdirs()) {
      LOGGER.error("ConfigNodeEnvironmentUtils can't mkdir {}.", dir);
    }
  }

  private static void cleanAllDir() {
    cleanDir(ConfigNodeConstant.CONF_DIR);
    cleanDir(ConfigNodeConstant.DATA_DIR);
  }

  public static void cleanDir(String dir) {
    try {
      FileUtils.deleteDirectory(new File(dir));
    } catch (IOException e) {
      LOGGER.error("ConfigNodeEnvironmentUtils can't remove dir {}.", dir, e);
    }
  }
}
