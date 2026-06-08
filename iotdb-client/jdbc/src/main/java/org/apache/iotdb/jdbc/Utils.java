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

package org.apache.iotdb.jdbc;

import java.nio.charset.Charset;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utilities for JDBC URL parsing. */
public class Utils {

  @SuppressWarnings({
    "squid:S5843",
    "squid:S5998"
  }) // Regular expressions should not be too complicated
  static final Pattern SUFFIX_URL_PATTERN = Pattern.compile("(/|\\\\?.*=.*(&.*=.*)*)?");

  static final String COLON = ":";
  static final char SLASH = '/';
  static final String PARAMETER_SEPARATOR = "?";

  static final String RPC_COMPRESS = "rpc_compress";

  /**
   * Parse JDBC connection URL The only supported format of the URL is:
   * jdbc:iotdb://localhost:6667/.
   */
  static IoTDBConnectionParams parseUrl(String url, Properties info) throws IoTDBURLException {
    Properties properties = info == null ? new Properties() : info;
    IoTDBConnectionParams params = new IoTDBConnectionParams(url);
    if (url.trim().equalsIgnoreCase(Config.IOTDB_URL_PREFIX)) {
      return params;
    }

    boolean isUrlLegal = false;
    Matcher matcher = null;
    String host = null;
    String suffixURL = null;
    if (url.startsWith(Config.IOTDB_URL_PREFIX)) {
      String subURL = url.substring(Config.IOTDB_URL_PREFIX.length());
      int i = subURL.lastIndexOf(COLON);
      if (i <= 0) {
        throw new IoTDBURLException(
            "Error url format, url should be jdbc:iotdb://anything:port/[database] or jdbc:iotdb://anything:port[/database]?property1=value1&property2=value2, current url is "
                + url);
      }
      host = subURL.substring(0, i);
      params.setHost(host);
      i++;
      // parse port
      int port = 0;
      for (; i < subURL.length() && Character.isDigit(subURL.charAt(i)); i++) {
        port = port * 10 + (subURL.charAt(i) - '0');
      }
      suffixURL = i < subURL.length() ? subURL.substring(i) : "";
      // legal port
      if (port >= 1 && port <= 65535) {
        params.setPort(port);

        // parse database
        if (i < subURL.length() && subURL.charAt(i) == SLASH) {
          int endIndex = subURL.indexOf(PARAMETER_SEPARATOR, i + 1);
          String database;
          if (endIndex <= i + 1) {
            if (i + 1 == subURL.length()) {
              database = null;
            } else {
              database = subURL.substring(i + 1);
            }
            suffixURL = "";
          } else {
            database = subURL.substring(i + 1, endIndex);
            suffixURL = subURL.substring(endIndex);
          }
          params.setDb(database);
        }

        matcher = SUFFIX_URL_PATTERN.matcher(suffixURL);
        if (matcher.matches() && parseUrlParam(subURL, properties)) {
          isUrlLegal = true;
        }
      }
    }
    if (!isUrlLegal) {
      throw new IoTDBURLException(
          "Error url format, url should be jdbc:iotdb://anything:port/[database] or jdbc:iotdb://anything:port[/database]?property1=value1&property2=value2, current url is "
              + url);
    }

    if (properties.containsKey(Config.AUTH_USER)) {
      params.setUsername(properties.getProperty(Config.AUTH_USER));
    }
    if (properties.containsKey(Config.AUTH_PASSWORD)) {
      params.setPassword(properties.getProperty(Config.AUTH_PASSWORD));
    }
    if (properties.containsKey(Config.DEFAULT_BUFFER_CAPACITY)) {
      params.setThriftDefaultBufferSize(
          parseIntegerProperty(properties, Config.DEFAULT_BUFFER_CAPACITY));
    }
    if (properties.containsKey(Config.THRIFT_FRAME_MAX_SIZE)) {
      params.setThriftMaxFrameSize(parseIntegerProperty(properties, Config.THRIFT_FRAME_MAX_SIZE));
    }
    if (properties.containsKey(Config.VERSION)) {
      params.setVersion(parseVersionProperty(properties));
    }
    if (properties.containsKey(Config.NETWORK_TIMEOUT)) {
      params.setNetworkTimeout(parseIntegerProperty(properties, Config.NETWORK_TIMEOUT));
    }
    if (properties.containsKey(Config.TIME_ZONE)) {
      params.setTimeZone(validateTimeZoneProperty(properties));
    }
    if (properties.containsKey(Config.CHARSET)) {
      params.setCharset(validateCharsetProperty(properties));
    }
    if (properties.containsKey(Config.USE_SSL)) {
      params.setUseSSL(parseBooleanProperty(properties));
    }
    if (properties.containsKey(Config.TRUST_STORE)) {
      params.setTrustStore(properties.getProperty(Config.TRUST_STORE));
    }
    if (properties.containsKey(Config.TRUST_STORE_PWD)) {
      params.setTrustStorePwd(properties.getProperty(Config.TRUST_STORE_PWD));
    }
    if (properties.containsKey(Config.SQL_DIALECT)) {
      params.setSqlDialect(validateSqlDialectProperty(properties));
    }

    return params;
  }

  /**
   * Parse the parameters in the URL and assign values to those that meet the conditions
   *
   * @param subURL Need to deal with url ,format like a=b&c=d
   * @return Judge whether it is legal. Illegal situations include: 1.there is a key that does not
   *     need to be resolved 2.the value corresponding to the key that needs to be resolved does not
   *     match the type
   */
  private static boolean parseUrlParam(String subURL, Properties info) {
    if (!subURL.contains("?")) {
      return true;
    }
    String paramURL = subURL.substring(subURL.indexOf('?') + 1);
    String[] params = paramURL.split("&", -1);
    for (String tmpParam : params) {
      int separatorIndex = tmpParam.indexOf('=');
      if (separatorIndex <= 0 || separatorIndex == tmpParam.length() - 1) {
        return false;
      }
      String key = tmpParam.substring(0, separatorIndex);
      String value = tmpParam.substring(separatorIndex + 1);
      switch (key) {
        case RPC_COMPRESS:
          if (isBoolean(value)) {
            Config.rpcThriftCompressionEnable = Boolean.parseBoolean(value);
          } else {
            return false;
          }
          break;
        case Config.TRUST_STORE:
        case Config.TRUST_STORE_PWD:
          info.put(key, value);
          break;
        case Config.USE_SSL:
          if (!isBoolean(value)) {
            return false;
          }
          info.put(key, value);
          break;
        case Config.VERSION:
          try {
            Constant.Version.valueOf(value);
          } catch (IllegalArgumentException e) {
            return false;
          }
          info.put(key, value);
          break;
        case Config.NETWORK_TIMEOUT:
          try {
            Integer.parseInt(value);
          } catch (NumberFormatException e) {
            return false;
          }
          info.put(key, value);
          break;
        case Config.SQL_DIALECT:
          if (!Constant.TREE.equals(value) && !Constant.TABLE.equals(value)) {
            return false;
          }
          info.put(key, value);
          break;
        case Config.TIME_ZONE:
          try {
            // Check the validity of the time zone string.
            ZoneId.of(value);
          } catch (DateTimeException e) {
            return false;
          }
          info.put(key, value);
          break;
        case Config.CHARSET:
          try {
            Charset.forName(value);
          } catch (Exception e) {
            return false;
          }
          info.put(key, value);
          break;
        default:
          return false;
      }
    }
    return true;
  }

  private static boolean isBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  private static int parseIntegerProperty(Properties properties, String key)
      throws IoTDBURLException {
    String value = getPropertyValue(properties, key);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw invalidPropertyValue(key, value, e);
    }
  }

  private static Constant.Version parseVersionProperty(Properties properties)
      throws IoTDBURLException {
    String value = getPropertyValue(properties, Config.VERSION);
    try {
      return Constant.Version.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw invalidPropertyValue(Config.VERSION, value, e);
    }
  }

  private static boolean parseBooleanProperty(Properties properties) throws IoTDBURLException {
    String value = getPropertyValue(properties, Config.USE_SSL);
    if (!isBoolean(value)) {
      throw invalidPropertyValue(Config.USE_SSL, value, null);
    }
    return Boolean.parseBoolean(value);
  }

  private static String validateTimeZoneProperty(Properties properties) throws IoTDBURLException {
    String value = getPropertyValue(properties, Config.TIME_ZONE);
    try {
      ZoneId.of(value);
    } catch (DateTimeException e) {
      throw invalidPropertyValue(Config.TIME_ZONE, value, e);
    }
    return value;
  }

  private static String validateCharsetProperty(Properties properties) throws IoTDBURLException {
    String value = getPropertyValue(properties, Config.CHARSET);
    try {
      Charset.forName(value);
    } catch (Exception e) {
      throw invalidPropertyValue(Config.CHARSET, value, e);
    }
    return value;
  }

  private static String validateSqlDialectProperty(Properties properties) throws IoTDBURLException {
    String value = getPropertyValue(properties, Config.SQL_DIALECT);
    if (!Constant.TREE.equals(value) && !Constant.TABLE.equals(value)) {
      throw invalidPropertyValue(Config.SQL_DIALECT, value, null);
    }
    return value;
  }

  private static String getPropertyValue(Properties properties, String key)
      throws IoTDBURLException {
    String value = properties.getProperty(key);
    if (value == null) {
      throw invalidPropertyValue(key, null, null);
    }
    return value;
  }

  private static IoTDBURLException invalidPropertyValue(String key, String value, Throwable cause) {
    return new IoTDBURLException(
        "Invalid value for JDBC connection property " + key + ": " + value, cause);
  }

  private Utils() {}
}
