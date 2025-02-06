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

package org.apache.iotdb.collector.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;

public class CollectorConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorConfig.class);

  private int restServicePort = 17070;

  public int getRestServicePort() {
    return restServicePort;
  }

  public void setRestServicePort(int restServicePort) {
    this.restServicePort = restServicePort;
  }

  public String getAllFormattedConfigFields() {
    final StringBuilder configMessage = new StringBuilder();
    for (final Field configField : CollectorConfig.class.getDeclaredFields()) {
      try {
        final String configType = configField.getGenericType().getTypeName();
        final String configContent;
        if (configType.contains("java.lang.String[][]")) {
          final String[][] configList = (String[][]) configField.get(this);
          final StringBuilder builder = new StringBuilder();
          for (final String[] strings : configList) {
            builder.append(Arrays.asList(strings)).append(";");
          }
          configContent = builder.toString();
        } else if (configType.contains("java.lang.String[]")) {
          final String[] configList = (String[]) configField.get(this);
          configContent = Arrays.asList(configList).toString();
        } else {
          configContent = configField.get(this).toString();
        }
        configMessage
            .append("\n\t")
            .append(configField.getName())
            .append("=")
            .append(configContent)
            .append(";");
      } catch (final IllegalAccessException e) {
        LOGGER.warn("Failed to get config message for field {}: {}",
            configField.getName(), e.getMessage(), e);
      }
    }
    return configMessage.toString();
  }
}
