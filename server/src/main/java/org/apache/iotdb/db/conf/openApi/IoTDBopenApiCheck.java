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
package org.apache.iotdb.db.conf.openApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class IoTDBopenApiCheck {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBopenApiCheck.class);
  private Properties properties = new Properties();
  private static final IoTDBopenApiConfig config = IoTDBopenApiDescriptor.getInstance().getConfig();

  private static String ENABLE_OPENAPI_VALUE = String.valueOf(config.isStartOpenApi());

  private static String OPENAPI_PORT_VALUE = String.valueOf(config.getOpenApiPort());

  public static IoTDBopenApiCheck getInstance() {
    return IoTDBopenApiConfigCheckHolder.INSTANCE;
  }

  private static class IoTDBopenApiConfigCheckHolder {

    private static final IoTDBopenApiCheck INSTANCE = new IoTDBopenApiCheck();
  }

  public void checkConfig() throws IOException {
    try {
      Integer.parseInt(OPENAPI_PORT_VALUE);
    } catch (NumberFormatException e) {
      printErrorLogAndExit(OPENAPI_PORT_VALUE);
    }
    try {
      Boolean.parseBoolean(ENABLE_OPENAPI_VALUE);
    } catch (NumberFormatException e) {
      printErrorLogAndExit(ENABLE_OPENAPI_VALUE);
    }
  }

  private void printErrorLogAndExit(String property) {
    logger.error("Wrong {}, please set as: {} !", property, properties.getProperty(property));
    System.exit(-1);
  }
}
