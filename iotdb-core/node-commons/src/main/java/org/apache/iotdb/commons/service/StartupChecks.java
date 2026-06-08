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
package org.apache.iotdb.commons.service;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.i18n.ServiceMessages;
import org.apache.iotdb.commons.utils.JVMCommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;

public abstract class StartupChecks {

  private static final Logger logger = LoggerFactory.getLogger(StartupChecks.class);

  private final String nodeRole;

  protected final List<StartupCheck> preChecks = new ArrayList<>();

  protected StartupChecks(String nodeRole) {
    this.nodeRole = nodeRole;
  }

  private void checkJMXPort(String nodeRole) {
    boolean jmxLocal = Boolean.parseBoolean(System.getProperty(IoTDBConstant.IOTDB_JMX_LOCAL));
    String jmxPort = System.getProperty(IoTDBConstant.IOTDB_JMX_PORT);

    if (jmxLocal) {
      logger.info(ServiceMessages.START_JMX_LOCALLY);
      return;
    }

    if (jmxPort == null) {
      String filename =
          nodeRole.equals(IoTDBConstant.DN_ROLE)
              ? IoTDBConstant.DN_ENV_FILE_NAME
              : IoTDBConstant.CN_ENV_FILE_NAME;
      logger.warn(
          ServiceMessages.JMX_PORT_MISSING_FROM_ENV,
          IoTDBConstant.IOTDB_JMX_PORT,
          filename,
          filename);
    } else {
      logger.info(ServiceMessages.START_JMX_REMOTELY, jmxPort);
    }
  }

  private void checkJDK() throws StartupException {
    int version = JVMCommonUtils.getJdkVersion();
    if (version < IoTDBConstant.MIN_SUPPORTED_JDK_VERSION) {
      throw new StartupException(
          String.format(
              ServiceMessages.JDK_VERSION_TOO_LOW,
              IoTDBConstant.MIN_SUPPORTED_JDK_VERSION,
              version));
    } else {
      logger.info(ServiceMessages.JDK_VERSION_IS, version);
    }
  }

  private void checkJVM() {
    RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
    logger.info(ServiceMessages.JVM_VERSION_IS, bean.getVmName(), bean.getVmVersion());
    try {
      // For more information, visit https://github.com/oracle/graal/issues/8638
      Class.forName("org.graalvm.home.Version");
    } catch (ClassNotFoundException e) {
      return;
    }
    logger.warn(ServiceMessages.GRAALVM_NOT_RECOMMENDED);
  }

  protected void envCheck() {
    preChecks.add(() -> checkJMXPort(nodeRole));
    preChecks.add(this::checkJDK);
    preChecks.add(this::checkJVM);
  }

  /** execute every pretest. */
  protected void verify() throws StartupException {
    for (StartupCheck check : preChecks) {
      check.execute();
    }
  }

  protected abstract void portCheck() throws StartupException;

  protected abstract void startUpCheck() throws Exception;
}
