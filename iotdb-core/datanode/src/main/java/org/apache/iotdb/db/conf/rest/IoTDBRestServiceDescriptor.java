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

public class IoTDBRestServiceDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBRestServiceDescriptor.class);

  private static final String REST_QUERY_DEFAULT_ROW_SIZE_LIMIT =
      "rest_query_default_row_size_limit";
  private static final String REST_MAX_REQUEST_BODY_SIZE_IN_BYTES =
      "rest_max_request_body_size_in_bytes";
  private static final String REST_MAX_TOTAL_CONCURRENT_REQUEST_BODY_SIZE_IN_BYTES =
      "rest_max_total_concurrent_request_body_size_in_bytes";
  private static final String REST_MAX_INSERT_ROWS = "rest_max_insert_rows";
  private static final String REST_MAX_INSERT_COLUMNS = "rest_max_insert_columns";
  private static final String REST_MAX_INSERT_VALUES = "rest_max_insert_values";

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

  private void loadProps(TrimProperties properties) {
    conf.setEnableRestService(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_rest_service", Boolean.toString(conf.isEnableRestService()))));
    conf.setRestServicePort(
        Integer.parseInt(
            properties.getProperty(
                "rest_service_port", Integer.toString(conf.getRestServicePort()))));
    loadRuntimeLimitProps(properties);
    conf.setEnableSwagger(
        Boolean.parseBoolean(
            properties.getProperty("enable_swagger", Boolean.toString(conf.isEnableSwagger()))));

    conf.setEnableHttps(
        Boolean.parseBoolean(
            properties.getProperty("enable_https", Boolean.toString(conf.isEnableHttps()))));
    conf.setClientAuth(
        Boolean.parseBoolean(
            properties.getProperty("client_auth", Boolean.toString(conf.isClientAuth()))));
    conf.setKeyStorePath(properties.getProperty("key_store_path", conf.getKeyStorePath()));
    conf.setKeyStorePwd(properties.getProperty("key_store_pwd", conf.getKeyStorePwd()));
    conf.setTrustStorePath(properties.getProperty("trust_store_path", conf.getTrustStorePath()));
    conf.setTrustStorePwd(properties.getProperty("trust_store_pwd", conf.getTrustStorePwd()));
    conf.setIdleTimeoutInSeconds(
        Integer.parseInt(
            properties.getProperty(
                "idle_timeout_in_seconds", Integer.toString(conf.getIdleTimeoutInSeconds()))));
  }

  public synchronized void loadHotModifiedProps(TrimProperties properties) {
    loadRuntimeLimitProps(properties);
  }

  private void loadRuntimeLimitProps(TrimProperties properties) {
    conf.setRestQueryDefaultRowSizeLimit(
        Integer.parseInt(
            properties.getProperty(
                REST_QUERY_DEFAULT_ROW_SIZE_LIMIT,
                Integer.toString(conf.getRestQueryDefaultRowSizeLimit()))));
    conf.setRestMaxRequestBodySizeInBytes(
        Long.parseLong(
            properties.getProperty(
                REST_MAX_REQUEST_BODY_SIZE_IN_BYTES,
                Long.toString(conf.getRestMaxRequestBodySizeInBytes()))));
    conf.setRestMaxTotalConcurrentRequestBodySizeInBytes(
        parseMaxTotalConcurrentRequestBodySizeInBytes(properties));
    conf.setRestMaxInsertRows(
        Integer.parseInt(
            properties.getProperty(
                REST_MAX_INSERT_ROWS, Integer.toString(conf.getRestMaxInsertRows()))));
    conf.setRestMaxInsertColumns(
        Integer.parseInt(
            properties.getProperty(
                REST_MAX_INSERT_COLUMNS, Integer.toString(conf.getRestMaxInsertColumns()))));
    conf.setRestMaxInsertValues(
        Long.parseLong(
            properties.getProperty(
                REST_MAX_INSERT_VALUES, Long.toString(conf.getRestMaxInsertValues()))));
  }

  private long parseMaxTotalConcurrentRequestBodySizeInBytes(TrimProperties properties) {
    long configuredLimit =
        Long.parseLong(
            properties.getProperty(
                REST_MAX_TOTAL_CONCURRENT_REQUEST_BODY_SIZE_IN_BYTES,
                Long.toString(conf.getRestMaxTotalConcurrentRequestBodySizeInBytes())));
    return configuredLimit == 0
        ? calculateRequestBodyMemoryLimitInBytes(properties)
        : configuredLimit;
  }

  static long calculateRequestBodyMemoryLimitInBytes(TrimProperties properties) {
    String memoryAllocateProportion = properties.getProperty("datanode_memory_proportion", null);
    if (memoryAllocateProportion == null) {
      memoryAllocateProportion =
          properties.getProperty("storage_query_schema_consensus_free_memory_proportion", null);
    }
    if (memoryAllocateProportion == null) {
      return IoTDBRestServiceConfig.getDefaultRequestBodyMemoryLimitInBytes();
    }

    String[] proportions = memoryAllocateProportion.split(":");
    int proportionSum = 0;
    for (String proportion : proportions) {
      proportionSum += Integer.parseInt(proportion.trim());
    }
    if (proportionSum == 0 || proportions.length < 6) {
      return IoTDBRestServiceConfig.getDefaultRequestBodyMemoryLimitInBytes();
    }
    return Runtime.getRuntime().maxMemory()
        * Integer.parseInt(proportions[proportions.length - 1].trim())
        / proportionSum
        / 2;
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
