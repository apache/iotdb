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

package org.apache.iotdb.procedure.service;

import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.procedure.conf.ProcedureNodeConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProcedureNode implements ProcedureNodeMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureNode.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ProcedureNodeConstant.PROCEDURENODE_PACKAGE,
          ProcedureNodeConstant.JMX_TYPE,
          "ProcedureNode");
  private final RegisterManager registerManager = new RegisterManager();

  private ProcedureNode() {}

  public static ProcedureNode getInstance() {
    return ProcedureNodeHolder.INSTANCE;
  }

  public static void main(String[] args) {
    new ProcedureServerCommandLine().doMain(args);
  }

  public void setUp() throws StartupException, IOException {
    LOGGER.info("Setting up {}...", ProcedureNodeConstant.GLOBAL_NAME);
    registerManager.register(new JMXService());
    JMXService.registerMBean(getInstance(), mbeanName);
    ProcedureServer.getInstance().initSyncedServiceImpl(new ProcedureServerProcessor());
    registerManager.register(ProcedureServer.getInstance());
    LOGGER.info("Init rpc server  success");
  }

  public void active() {
    try {
      setUp();
    } catch (StartupException | IOException e) {
      LOGGER.error("Meet  error  while starting  up.", e);
      ProcedureServer.getInstance().stop();
      LOGGER.warn("Procedure executor has stopped.");
      deactivate();
      return;
    }
    LOGGER.info("{}  has  started.", ProcedureNodeConstant.GLOBAL_NAME);
  }

  public void deactivate() {
    LOGGER.info("Deactivating  {}...", ProcedureNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("{} is  deactivated.", ProcedureNodeConstant.GLOBAL_NAME);
  }

  public void shutdown() throws ShutdownException {
    LOGGER.info("Deactivating  {}...", ProcedureNodeConstant.GLOBAL_NAME);
    registerManager.shutdownAll();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("{} is  deactivated.", ProcedureNodeConstant.GLOBAL_NAME);
  }

  public void stop() {
    deactivate();
  }

  private static class ProcedureNodeHolder {
    private static final ProcedureNode INSTANCE = new ProcedureNode();

    private ProcedureNodeHolder() {}
  }
}
