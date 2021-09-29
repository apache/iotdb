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
package org.apache.iotdb.db.service;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StartupChecks {

  private static final Logger logger = LoggerFactory.getLogger(StartupChecks.class);
  public static final StartupCheck checkJMXPort =
      () -> {
        String jmxPort = System.getProperty(IoTDBConstant.IOTDB_JMX_PORT);
        if (jmxPort == null) {
          logger.warn(
              "{} missing from {}.sh(Unix or OS X, if you use Windows," + " check conf/{}.bat)",
              IoTDBConstant.IOTDB_JMX_PORT,
              IoTDBConstant.ENV_FILE_NAME,
              IoTDBConstant.ENV_FILE_NAME);
        } else {
          logger.info("JMX is enabled to receive remote connection on port {}", jmxPort);
        }
      };
  public static final StartupCheck checkJDK =
      () -> {
        int version = CommonUtils.getJdkVersion();
        if (version < IoTDBConstant.MIN_SUPPORTED_JDK_VERSION) {
          throw new StartupException(
              String.format(
                  "Requires JDK version >= %d, current version is %d",
                  IoTDBConstant.MIN_SUPPORTED_JDK_VERSION, version));
        } else {
          logger.info("JDK version is {}.", version);
        }
      };
  private final List<StartupCheck> preChecks = new ArrayList<>();
  private final List<StartupCheck> defaultTests = new ArrayList<>();

  public StartupChecks() {
    defaultTests.add(checkJMXPort);
    defaultTests.add(checkJDK);
  }

  public StartupChecks withDefaultTest() {
    preChecks.addAll(defaultTests);
    return this;
  }

  /** execute every pretests. */
  public void verify() throws StartupException {
    for (StartupCheck check : preChecks) {
      check.execute();
    }
  }
}
