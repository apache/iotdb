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
import java.util.Properties;

public class IoTDBRestServiceCheck {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBRestServiceCheck.class);
  private final Properties properties = new Properties();
  private static final IoTDBRestServiceConfig config =
      IoTDBRestServiceDescriptor.getInstance().getConfig();

  private static final String ENABLE_REST_SERVICE_VALUE =
      String.valueOf(config.isEnableRestService());

  private static final String REST_SERVICE_VALUE = String.valueOf(config.getRestServicePort());

  public static IoTDBRestServiceCheck getInstance() {
    return IoTDBRestServiceConfigCheckHolder.INSTANCE;
  }

  private static class IoTDBRestServiceConfigCheckHolder {

    private static final IoTDBRestServiceCheck INSTANCE = new IoTDBRestServiceCheck();
  }

  public void checkConfig() throws IOException {
    try {
      Integer.parseInt(REST_SERVICE_VALUE);
    } catch (NumberFormatException e) {
      printErrorLogAndExit(REST_SERVICE_VALUE);
    }
    try {
      Boolean.parseBoolean(ENABLE_REST_SERVICE_VALUE);
    } catch (NumberFormatException e) {
      printErrorLogAndExit(ENABLE_REST_SERVICE_VALUE);
    }
  }

  private void printErrorLogAndExit(String property) {
    logger.error("Wrong {}, please set as: {} !", property, properties.getProperty(property));
    System.exit(-1);
  }
}
