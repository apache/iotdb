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

package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.StartupChecks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * {@link DataNodeStartupCheck} checks the parameters in iotdb-datanode.properties when start and
 * restart.
 */
public class DataNodeStartupCheck extends StartupChecks {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeStartupCheck.class);
  private final IoTDBConfig config;
  private static final int DATANODE_PORTS = 6;

  public DataNodeStartupCheck(String nodeRole, IoTDBConfig config) {
    super(nodeRole);
    this.config = config;
  }

  private void checkDataNodePortUnique() throws StartupException {
    Set<Integer> portSet = new HashSet<>();
    portSet.add(config.getInternalPort());
    portSet.add(config.getMqttPort());
    portSet.add(config.getRpcPort());
    portSet.add(config.getMppDataExchangePort());
    portSet.add(config.getDataRegionConsensusPort());
    portSet.add(config.getSchemaRegionConsensusPort());
    if (portSet.size() != DATANODE_PORTS) {
      throw new StartupException("ports used in datanode have repeat.");
    } else {
      LOGGER.info("DataNode port check successful.");
    }
  }

  @Override
  protected void portCheck() {
    preChecks.add(this::checkDataNodePortUnique);
  }

  @Override
  public void startUpCheck() throws StartupException {
    envCheck();
    portCheck();
    verify();
  }
}
