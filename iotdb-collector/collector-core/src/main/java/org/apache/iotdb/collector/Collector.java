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
import org.apache.iotdb.collector.config.CollectorSystemPropertiesHandler;
import org.apache.iotdb.collector.service.CollectorMBean;
import org.apache.iotdb.collector.service.CollectorRestService;
import org.apache.iotdb.collector.service.RegisterManager;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Set;

public class Collector extends ServerCommandLine implements CollectorMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(Collector.class);

  private static final RegisterManager REGISTER_MANAGER = RegisterManager.getInstance();

  public Collector() {
    super("Collector");
    CollectorHolder.INSTANCE = this;
  }

  public static void main(String[] args) {
    LOGGER.info(
        "IoTDB-Collector environment variables: {}", CollectorConfig.getEnvironmentVariables());
    LOGGER.info("IoTDB-Collector default charset is: {}", Charset.defaultCharset().displayName());

    final Collector collector = new Collector();
    final int returnCode = collector.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  @Override
  protected void start() {
    boolean isFirstStart;
    try {
      isFirstStart = prepareCollector();
      if (isFirstStart) {
        LOGGER.info("Collector is starting for the first time...");
      } else {
        LOGGER.info("Collector is restarting...");
      }

      pullAndCheckSystemConfigurations();

      initProtocols();
    } catch (final StartupException e) {
      LOGGER.error("Collector start failed", e);
      stop();
      System.exit(-1);
    }
  }

  private boolean prepareCollector() {
    return CollectorSystemPropertiesHandler.getInstance().fileExist();
  }

  private void pullAndCheckSystemConfigurations() {
    LOGGER.info("Pulling system configurations from the ConfigNode-leader...");
  }

  private void initProtocols() throws StartupException {
    REGISTER_MANAGER.register(CollectorRestService.getInstance());
  }

  private void stop() {}

  @Override
  protected void remove(final Set<Integer> nodeIds) throws IoTDBException {
    // empty method
  }

  private static class CollectorHolder {
    private static Collector INSTANCE;

    private CollectorHolder() {
      // empty constructor
    }
  }
}
