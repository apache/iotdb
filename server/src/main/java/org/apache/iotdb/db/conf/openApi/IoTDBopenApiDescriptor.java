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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class IoTDBopenApiDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBopenApiDescriptor.class);

  private final IoTDBopenApiConfig conf = new IoTDBopenApiConfig();

  protected IoTDBopenApiDescriptor() {
    loadProps();
  }

  public static IoTDBopenApiDescriptor getInstance() {
    return IoTDBOpenApiDescriptorHolder.INSTANCE;
  }

  /** load an property file and set TsfileDBConfig variables. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void loadProps() {
    URL url = getPropsUrl();
    if (url == null) {
      logger.warn("Couldn't load the OpenAPI configuration from any of the known sources.");
      return;
    }
    try (InputStream inputStream = url.openStream()) {
      logger.info("Start to read config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);
      conf.setStartOpenApi(
          Boolean.parseBoolean(
              properties.getProperty("enable_openApi", Boolean.toString(conf.isStartOpenApi()))));
      conf.setOpenApiPort(
          Integer.parseInt(
              properties.getProperty("openApi_port", Integer.toString(conf.getOpenApiPort()))));

      conf.setEnable_https(
          Boolean.parseBoolean(
              properties.getProperty("enable_https", Boolean.toString(conf.isEnable_https()))));
      conf.setKeyStorePath(properties.getProperty("key_store_path", conf.getKeyStorePath()));
      conf.setKeyStorePwd(properties.getProperty("key_store_pwd", conf.getKeyStorePwd()));
      conf.setTrustStorePath(properties.getProperty("trust_store_path", conf.getTrustStorePath()));
      conf.setTrustStorePwd(properties.getProperty("trust_store_pwd", conf.getTrustStorePwd()));
      conf.setIdleTimeout(
          Integer.parseInt(
              properties.getProperty("idle_timeout", Integer.toString(conf.getIdleTimeout()))));

    } catch (FileNotFoundException e) {
      logger.warn("Fail to find config file {}", url, e);
    } catch (IOException e) {
      logger.warn("Cannot load config file, use default configuration", e);
    } catch (Exception e) {
      logger.warn("Incorrect format in config file, use default configuration", e);
    }
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public URL getPropsUrl() {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (urlString != null) {
        urlString =
            urlString
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + "openApi"
                + File.separatorChar
                + IoTDBopenApiConfig.CONFIG_NAME;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = IoTDBConfig.class.getResource("/" + IoTDBopenApiConfig.CONFIG_NAME);
        if (uri != null) {
          return uri;
        }
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            IoTDBopenApiConfig.CONFIG_NAME);
        // update all data seriesPath
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + IoTDBopenApiConfig.CONFIG_NAME);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      return null;
    }
  }

  public IoTDBopenApiConfig getConfig() {
    return conf;
  }

  private static class IoTDBOpenApiDescriptorHolder {

    private static final IoTDBopenApiDescriptor INSTANCE = new IoTDBopenApiDescriptor();

    private IoTDBOpenApiDescriptorHolder() {}
  }
}
