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
package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.confignode.conf.ConfigNodeRemoveCheck;
import org.apache.iotdb.confignode.conf.ConfigNodeStartupCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_CONFIGNODE_USAGE;

public class ConfigNodeCommandLine extends ServerCommandLine {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeCommandLine.class);

  // Start ConfigNode
  private static final String MODE_START = "-s";
  // Remove ConfigNode
  private static final String MODE_REMOVE = "-r";

  private static final String USAGE =
      "Usage: <-s|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: Start the ConfigNode and join to the cluster\n"
          + "-r: Remove the ConfigNode out of the cluster\n";

  @Override
  protected String getUsage() {
    return USAGE;
  }

  @Override
  protected int run(String[] args) {
    String mode;
    if (args.length < 1) {
      mode = MODE_START;
      LOGGER.warn(
          "ConfigNode does not specify a startup mode. The default startup mode {} will be used",
          MODE_START);
    } else {
      mode = args[0];
    }

    LOGGER.info("Running mode {}", mode);
    if (MODE_START.equals(mode)) {
      try {
        // Startup environment check
        StartupChecks checks = new StartupChecks(IoTDBConstant.CN_ROLE).withDefaultTest();
        checks.verify();
        // Do ConfigNode startup checks
        ConfigNodeStartupCheck.getInstance().startUpCheck();
      } catch (StartupException | ConfigurationException | IOException e) {
        LOGGER.error("Meet error when doing start checking", e);
        return -1;
      }
      ConfigNode.getInstance().active();
    } else if (MODE_REMOVE.equals(mode)) {
      // remove ConfigNode
      try {
        doRemoveConfigNode(args);
      } catch (IOException e) {
        LOGGER.error("Meet error when doing remove ConfigNode", e);
        return -1;
      }
    } else {
      LOGGER.error("Unsupported startup mode: {}", mode);
      return -1;
    }

    return 0;
  }

  private void doRemoveConfigNode(String[] args) throws IOException {

    if (args.length != 2) {
      LOGGER.info(REMOVE_CONFIGNODE_USAGE);
      return;
    }

    LOGGER.info("Starting to remove ConfigNode, parameter: {}, {}", args[0], args[1]);

    try {
      TConfigNodeLocation removeConfigNodeLocation =
          ConfigNodeRemoveCheck.getInstance().removeCheck(args[1]);
      if (removeConfigNodeLocation == null) {
        LOGGER.error(
            "The ConfigNode to be removed is not in the cluster, or the input format is incorrect.");
        return;
      }

      ConfigNodeRemoveCheck.getInstance().removeConfigNode(removeConfigNodeLocation);
    } catch (BadNodeUrlException e) {
      LOGGER.warn("No ConfigNodes need to be removed.", e);
      return;
    }

    LOGGER.info(
        "ConfigNode: {} is removed. If the confignode data directory is no longer needed, you can delete it manually.",
        args[1]);
  }
}
