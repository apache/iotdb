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

import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.confignode.conf.ConfigNodeConfCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConfigNodeCommandLine extends ServerCommandLine {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeCommandLine.class);

  // establish the cluster as a seed
  private static final String MODE_START = "-s";
  // join an established cluster
  private static final String MODE_ADD = "-a";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  private static final String MODE_REMOVE = "-r";

  private static final String USAGE =
      "Usage: <-s|-a|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: start the node as a seed\n"
          + "-a: start the node as a new node\n"
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

    String mode = args[0];
    LOGGER.info("Running mode {}", mode);
    if (MODE_START.equals(mode)) {
      try {
        // Check parameters
        ConfigNodeConfCheck.getInstance().checkConfig();
      } catch (IOException | ConfigurationException | StartupException e) {
        LOGGER.error("Meet error when doing start checking", e);
        return -1;
      }
      ConfigNode configNode = ConfigNode.getInstance();
      configNode.active();
    } else if (MODE_ADD.equals(mode)) {
      // TODO: add node
    } else if (MODE_REMOVE.equals(mode)) {
      // TODO: remove node
    }

    return 0;
  }
}
