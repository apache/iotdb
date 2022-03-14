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

import org.apache.iotdb.confignode.conf.ConfigNodeConfCheck;
import org.apache.iotdb.confignode.exception.ConfigNodeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConfigNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  public ConfigNode() {
    // empty constructor
  }

  public static void main(String[] args) {
    try {
      ConfigNodeConfCheck.getInstance().checkConfig();
    } catch (ConfigNodeException | IOException e) {
      LOGGER.error("Meet error when doing start checking", e);
      System.exit(1);
    }

    ConfigNode daemon = ConfigNode.getInstance();
    daemon.active();
  }

  public void active() {
    LOGGER.info("IoTDB ConfigNode has started.");
  }

  public void deactivate() {}

  public void shutdown() {}

  public void stop() {}

  private static class ConfigNodeHolder {

    private static final ConfigNode INSTANCE = new ConfigNode();

    private ConfigNodeHolder() {
      // empty constructor
    }
  }

  public static ConfigNode getInstance() {
    return ConfigNodeHolder.INSTANCE;
  }
}
