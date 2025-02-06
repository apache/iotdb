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

import org.apache.iotdb.collector.config.Configuration;
import org.apache.iotdb.collector.service.ApiService;
import org.apache.iotdb.collector.service.IService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class Application {

  private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

  private final Configuration configuration = new Configuration();
  private final LinkedList<IService> services = new LinkedList<>();

  private Application() {
    services.add(new ApiService());
  }

  public static void main(String[] args) {
    LOGGER.info("[Application] Starting ...");
    final long startTime = System.currentTimeMillis();

    final Application application = new Application();

    application.logAllOptions();
    application.registerShutdownHook();
    application.startServices();

    LOGGER.info(
        "[Application] Successfully started in {}ms", System.currentTimeMillis() - startTime);
  }

  private void logAllOptions() {
    configuration.logAllOptions();
  }

  private void registerShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOGGER.warn("[Application] Exiting ...");

                  for (final IService service : services) {
                    try {
                      service.stop();
                    } catch (final Exception e) {
                      LOGGER.warn(
                          "[{}] Unexpected exception occurred when stopping: {}",
                          service.name(),
                          e.getMessage(),
                          e);
                    }
                  }

                  LOGGER.warn(
                      "[Application] JVM report: total memory {}, free memory {}, used memory {}",
                      Runtime.getRuntime().totalMemory(),
                      Runtime.getRuntime().freeMemory(),
                      Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
                  LOGGER.warn("[Application] Exited.");
                }));
  }

  private void startServices() {
    for (final IService service : services) {
      service.start();
    }
  }
}
