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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.db.conf.IoTDBConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class IoTDBRestServiceDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBRestServiceDescriptor.class);

  private final IoTDBRestServiceConfig conf = new IoTDBRestServiceConfig();

  protected IoTDBRestServiceDescriptor() {
    URL systemConfig = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (systemConfig != null) {
      TrimProperties trimProperties = loadProps(CommonConfig.SYSTEM_CONFIG_NAME);
      if (trimProperties != null) {
        loadProps(trimProperties);
      }
    }
  }

  public static IoTDBRestServiceDescriptor getInstance() {
    return IoTDBRestServiceDescriptorHolder.INSTANCE;
  }

  /** load an property file. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private TrimProperties loadProps(String configName) {
    URL url = getPropsUrl(configName);
    if (url == null) {
      logger.warn("Couldn't load the REST Service configuration from any of the known sources.");
      return null;
    }
    try (InputStream inputStream = url.openStream()) {
      logger.info("Start to read config file {}", url);
      TrimProperties trimProperties = new TrimProperties();
      trimProperties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      return trimProperties;
    } catch (FileNotFoundException e) {
      logger.warn("REST service fail to find config file {}", url, e);
    } catch (IOException e) {
      logger.warn("REST service cannot load config file, use default configuration", e);
    } catch (Exception e) {
      logger.warn("REST service Incorrect format in config file, use default configuration", e);
    }
    return null;
  }

  private void loadProps(TrimProperties trimProperties) {
    conf.setEnableRestService(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "enable_rest_service", Boolean.toString(conf.isEnableRestService())))
                .map(String::trim)
                .orElse(Boolean.toString(conf.isEnableRestService()))));
    conf.setRestServicePort(
        Integer.parseInt(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "rest_service_port", Integer.toString(conf.getRestServicePort())))
                .map(String::trim)
                .orElse(Integer.toString(conf.getRestServicePort()))));
    conf.setRestQueryDefaultRowSizeLimit(
        Integer.parseInt(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "rest_query_default_row_size_limit",
                        Integer.toString(conf.getRestQueryDefaultRowSizeLimit())))
                .map(String::trim)
                .orElse(Integer.toString(conf.getRestQueryDefaultRowSizeLimit()))));
    conf.setEnableSwagger(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "enable_swagger", Boolean.toString(conf.isEnableSwagger())))
                .map(String::trim)
                .orElse(Boolean.toString(conf.isEnableSwagger()))));

    conf.setEnableHttps(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "enable_https", Boolean.toString(conf.isEnableHttps())))
                .map(String::trim)
                .orElse(Boolean.toString(conf.isEnableHttps()))));
    conf.setClientAuth(
        Boolean.parseBoolean(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "client_auth", Boolean.toString(conf.isClientAuth())))
                .map(String::trim)
                .orElse(Boolean.toString(conf.isClientAuth()))));
    conf.setKeyStorePath(
        Optional.ofNullable(trimProperties.getProperty("key_store_path", conf.getKeyStorePath()))
            .map(String::trim)
            .orElse(conf.getKeyStorePath()));
    conf.setKeyStorePwd(
        Optional.ofNullable(trimProperties.getProperty("key_store_pwd", conf.getKeyStorePwd()))
            .map(String::trim)
            .orElse(conf.getKeyStorePwd()));
    conf.setTrustStorePath(
        Optional.ofNullable(
                trimProperties.getProperty("trust_store_path", conf.getTrustStorePath()))
            .map(String::trim)
            .orElse(conf.getTrustStorePath()));
    conf.setTrustStorePwd(
        Optional.ofNullable(trimProperties.getProperty("trust_store_pwd", conf.getTrustStorePwd()))
            .map(String::trim)
            .orElse(conf.getTrustStorePwd()));
    conf.setIdleTimeoutInSeconds(
        Integer.parseInt(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "idle_timeout_in_seconds",
                        Integer.toString(conf.getIdleTimeoutInSeconds())))
                .map(String::trim)
                .orElse(Integer.toString(conf.getIdleTimeoutInSeconds()))));
    conf.setCacheExpireInSeconds(
        Integer.parseInt(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "cache_expire_in_seconds",
                        Integer.toString(conf.getCacheExpireInSeconds())))
                .map(String::trim)
                .orElse(Integer.toString(conf.getCacheExpireInSeconds()))));
    conf.setCacheInitNum(
        Integer.parseInt(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "cache_init_num", Integer.toString(conf.getCacheInitNum())))
                .map(String::trim)
                .orElse(Integer.toString(conf.getCacheInitNum()))));
    conf.setCacheMaxNum(
        Integer.parseInt(
            Optional.ofNullable(
                    trimProperties.getProperty(
                        "cache_max_num", Integer.toString(conf.getCacheMaxNum())))
                .map(String::trim)
                .orElse(Integer.toString(conf.getCacheMaxNum()))));
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public URL getPropsUrl(String configName) {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (urlString != null) {
        urlString = urlString + File.separatorChar + "conf" + File.separatorChar + configName;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = IoTDBConfig.class.getResource("/" + configName);
        if (uri != null) {
          return uri;
        }
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            configName);
        // update all data seriesPath
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + configName);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      logger.warn("get url failed", e);
      return null;
    }
  }

  public IoTDBRestServiceConfig getConfig() {
    return conf;
  }

  private static class IoTDBRestServiceDescriptorHolder {

    private static final IoTDBRestServiceDescriptor INSTANCE = new IoTDBRestServiceDescriptor();

    private IoTDBRestServiceDescriptorHolder() {}
  }
}
