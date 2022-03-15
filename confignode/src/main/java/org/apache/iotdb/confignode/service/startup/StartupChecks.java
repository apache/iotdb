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
package org.apache.iotdb.confignode.service.startup;

import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.exception.startup.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** StartupChecks checks the startup environment for running ConfigNode */
public class StartupChecks {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartupChecks.class);
  public static final StartupCheck checkJMXPort =
      () -> {
        String jmxPort = System.getProperty(ConfigNodeConstant.CONFIGNODE_JMX_PORT);
        if (jmxPort == null) {
          LOGGER.warn(
              "{} missing from {}.sh(Unix or OS X, if you use Windows," + " check conf/{}.bat)",
              ConfigNodeConstant.CONFIGNODE_JMX_PORT,
              ConfigNodeConstant.ENV_FILE_NAME,
              ConfigNodeConstant.ENV_FILE_NAME);
        } else {
          LOGGER.info("JMX is enabled to receive remote connection on port {}", jmxPort);
        }
      };
  public static final StartupCheck checkJDK =
      () -> {
        int version = getJdkVersion();
        if (version < ConfigNodeConstant.MIN_SUPPORTED_JDK_VERSION) {
          throw new StartupException(
              String.format(
                  "Requires JDK version >= %d, current version is %d",
                  ConfigNodeConstant.MIN_SUPPORTED_JDK_VERSION, version));
        } else {
          LOGGER.info("JDK version is {}.", version);
        }
      };
  private final List<StartupCheck> preChecks = new ArrayList<>();
  private final List<StartupCheck> defaultChecks = new ArrayList<>();

  public StartupChecks() {
    defaultChecks.add(checkJMXPort);
    defaultChecks.add(checkJDK);
  }

  public StartupChecks withDefaultTest() {
    preChecks.addAll(defaultChecks);
    return this;
  }

  /** execute every pretests. */
  public void verify() throws StartupException {
    for (StartupCheck check : preChecks) {
      check.execute();
    }
  }

  private static int getJdkVersion() {
    String[] javaVersionElements = System.getProperty("java.version").split("\\.");
    if (Integer.parseInt(javaVersionElements[0]) == 1) {
      return Integer.parseInt(javaVersionElements[1]);
    } else {
      return Integer.parseInt(javaVersionElements[0]);
    }
  }
}
