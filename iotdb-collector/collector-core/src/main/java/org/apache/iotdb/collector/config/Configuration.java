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

import org.apache.iotdb.commons.conf.TrimProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;

public class Configuration {

  private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

  private static final String CONFIG_FILE_NAME = "application.properties";

  private final Options options = new Options();

  public Configuration() {
    loadProps();
  }

  private void loadProps() {
    final Optional<URL> url = getPropsUrl();
    if (url.isPresent()) {
      try (final InputStream inputStream = url.get().openStream()) {
        LOGGER.info("Start to read config file {}", url.get());
        final Properties properties = new Properties();
        properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        final TrimProperties trimProperties = new TrimProperties();
        trimProperties.putAll(properties);
        options.loadProperties(trimProperties);
      } catch (final FileNotFoundException e) {
        LOGGER.error("Fail to find config file, reject startup.", e);
        System.exit(-1);
      } catch (final IOException e) {
        LOGGER.error("IO exception when reading config file, reject startup.", e);
        System.exit(-1);
      } catch (final Exception e) {
        LOGGER.error("Unexpected exception when reading config file, reject startup.", e);
        System.exit(-1);
      }
    } else {
      LOGGER.warn("{} is not found, use default configuration", CONFIG_FILE_NAME);
    }
  }

  private Optional<URL> getPropsUrl() {
    final URL url = Options.class.getResource("/" + CONFIG_FILE_NAME);

    if (url != null) {
      return Optional.of(url);
    } else {
      LOGGER.warn(
          "Cannot find IOTDB_COLLECTOR_HOME or IOTDB_COLLECTOR_CONF environment variable when loading "
              + "config file {}, use default configuration",
          CONFIG_FILE_NAME);
      return Optional.empty();
    }
  }

  public void logAllOptions() {
    options.logAllOptions();
  }
}
