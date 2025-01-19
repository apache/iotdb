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

package org.apache.iotdb.collector;

import org.apache.iotdb.collector.config.CollectorConfig;
import org.apache.iotdb.collector.config.CollectorDescriptor;
import org.apache.iotdb.collector.service.CollectorMBean;
import org.apache.iotdb.collector.service.CollectorRestService;
import org.apache.iotdb.collector.service.RegisterManager;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Set;

public class Collector extends ServerCommandLine implements CollectorMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(Collector.class);

  private static final RegisterManager REGISTER_MANAGER = RegisterManager.getInstance();
  private static final CollectorConfig COLLECTOR_CONFIG =
      CollectorDescriptor.getInstance().getConfig();

  private Collector() {
    super("Collector");
  }

  public static void main(String[] args) {
    LOGGER.info(
        "IoTDB-CollectorNode environment variables: {}", CollectorConfig.getEnvironmentVariables());
    LOGGER.info(
        "IoTDB-CollectorNode default charset is: {}", Charset.defaultCharset().displayName());

    final Collector collector = new Collector();
    final int returnCode = collector.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  @Override
  protected void start() {
    try {
      initProtocols();

      LOGGER.info("IoTDB-CollectorNode configuration: {}", COLLECTOR_CONFIG.getConfigMessage());
      LOGGER.info(
          "Congratulations, IoTDB CollectorNode is set up successfully. Now, enjoy yourself!");
    } catch (final StartupException e) {
      LOGGER.error("CollectorNode start failed", e);
      System.exit(-1);
    }
  }

  private void initProtocols() throws StartupException {
    REGISTER_MANAGER.register(CollectorRestService.getInstance());
  }

  @Override
  protected void remove(final Set<Integer> nodeIds) {
    // empty method
  }
}
