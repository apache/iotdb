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

import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataNodeServerCommandLine extends ServerCommandLine {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeServerCommandLine.class);

  // join an established cluster
  private static final String MODE_START = "-s";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  private static final String MODE_REMOVE = "-r";

  private static final String USAGE =
      "Usage: <-s|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: start the node to the cluster\n"
          + "-r: remove the node out of the cluster\n";

  @Override
  protected String getUsage() {
    return USAGE;
  }

  @Override
  protected int run(String[] args) {
    if (args.length < 1) {
      usage(null);
      return -1;
    }

    DataNode dataNode = DataNode.getInstance();
    // check config of iotdb,and set some configs in cluster mode
    try {
      dataNode.serverCheckAndInit();
    } catch (ConfigurationException | IOException e) {
      logger.error("meet error when doing start checking", e);
      return -1;
    }
    String mode = args[0];
    logger.info("Running mode {}", mode);

    // initialize the current node and its services
    if (!dataNode.initLocalEngines()) {
      logger.error("initLocalEngines error, stop process!");
      return -1;
    }

    // we start IoTDB kernel first. then we start the cluster module.
    if (MODE_START.equals(mode)) {
      dataNode.doAddNode(args);
    } else if (MODE_REMOVE.equals(mode)) {
      dataNode.doRemoveNode(args);
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
    return 0;
  }
}
