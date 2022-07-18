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
package org.apache.iotdb.db.conf.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IoTDBRestServiceCheck {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBRestServiceCheck.class);
  private static final IoTDBRestServiceConfig CONFIG =
      IoTDBRestServiceDescriptor.getInstance().getConfig();

  public static IoTDBRestServiceCheck getInstance() {
    return IoTDBRestServiceConfigCheckHolder.INSTANCE;
  }

  private static class IoTDBRestServiceConfigCheckHolder {

    private static final IoTDBRestServiceCheck INSTANCE = new IoTDBRestServiceCheck();
  }

  public void checkConfig() throws IOException {
    if (CONFIG.getRestServicePort() > 65535 || CONFIG.getRestServicePort() < 1024) {
      printErrorLogAndExit("rest_service_port");
    }
    if (CONFIG.getIdleTimeoutInSeconds() <= 0) {
      printErrorLogAndExit("idle_timeout_in_seconds");
    }
    if (CONFIG.getCacheExpireInSeconds() <= 0) {
      printErrorLogAndExit("cache_expire_in_seconds");
    }
    if (CONFIG.getCacheMaxNum() <= 0) {
      printErrorLogAndExit("cache_max_num");
    }
    if (CONFIG.getCacheInitNum() <= 0) {
      printErrorLogAndExit("cache_init_num");
    }
    if (CONFIG.getCacheInitNum() > CONFIG.getCacheMaxNum()) {
      printErrorLogAndExit("cache_init_num");
    }
  }

  private void printErrorLogAndExit(String property) {
    LOGGER.error("Wrong config {}, please check!", property);
    System.exit(-1);
  }
}
