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

    boolean isUrlLegal = url.trim().equalsIgnoreCase(Config.IOTDB_URL_PREFIX);
    Matcher matcher = null;
    String host = null;
    String suffixURL = null;
    if (!isUrlLegal && url.startsWith(Config.IOTDB_URL_PREFIX)) {
      String subURL = url.substring(Config.IOTDB_URL_PREFIX.length());
      int authorityEnd = findAuthorityEnd(subURL);
      String authority = subURL.substring(0, authorityEnd);
      int portSeparatorIndex = authority.lastIndexOf(COLON);
      if (portSeparatorIndex <= 0 || portSeparatorIndex == authority.length() - 1) {
        throw new IoTDBURLException(
            "Error url format, url should be jdbc:iotdb://anything:port/[database] or jdbc:iotdb://anything:port[/database]?property1=value1&property2=value2, current url is "
                + url);
      }
      host = authority.substring(0, portSeparatorIndex);
      params.setHost(host);
      String portText = authority.substring(portSeparatorIndex + 1);
      // parse port
      int port = parsePort(portText);
      suffixURL = subURL.substring(authorityEnd);
      // legal port
      if (port >= 1 && port <= 65535) {
        params.setPort(port);

        // parse database
        if (!suffixURL.isEmpty() && suffixURL.charAt(0) == SLASH) {
          int endIndex = suffixURL.indexOf(PARAMETER_SEPARATOR, 1);
          String database;
          if (endIndex < 0) {
            if (suffixURL.length() == 1) {
              database = null;
            } else {
              database = suffixURL.substring(1);
            }
            suffixURL = "";
          } else {
            database = endIndex == 1 ? null : suffixURL.substring(1, endIndex);
            suffixURL = suffixURL.substring(endIndex);
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
          parsePositiveIntegerProperty(properties, Config.DEFAULT_BUFFER_CAPACITY));
    }
    if (properties.containsKey(Config.THRIFT_FRAME_MAX_SIZE)) {
      params.setThriftMaxFrameSize(
          parsePositiveIntegerProperty(properties, Config.THRIFT_FRAME_MAX_SIZE));
    }
    if (properties.containsKey(Config.VERSION)) {
      params.setVersion(parseVersionProperty(properties));
    }
    if (properties.containsKey(Config.NETWORK_TIMEOUT)) {
      params.setNetworkTimeout(parseNonNegativeIntegerProperty(properties, Config.NETWORK_TIMEOUT));
    }
    if (properties.containsKey(Config.TIME_ZONE)) {
      params.setTimeZone(validateTimeZoneProperty(properties));
    }
    if (properties.containsKey(Config.CHARSET)) {
      params.setCharset(validateCharsetProperty(properties));
    }
    if (properties.containsKey(Config.USE_SSL)) {
      params.setUseSSL(parseBooleanProperty(properties, Config.USE_SSL));
    }
    if (properties.containsKey(Config.TRUST_STORE)) {
      params.setTrustStore(properties.getProperty(Config.TRUST_STORE));
    }
    if (properties.containsKey(Config.TRUST_STORE_PWD)) {
      params.setTrustStorePwd(properties.getProperty(Config.TRUST_STORE_PWD));
    }
    if (properties.containsKey(RPC_COMPRESS)) {
      params.setRpcThriftCompressionEnabled(parseBooleanProperty(properties, RPC_COMPRESS));
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
        case Config.AUTH_USER:
        case Config.AUTH_PASSWORD:
          info.put(key, value);
          break;
        case Config.DEFAULT_BUFFER_CAPACITY:
        case Config.THRIFT_FRAME_MAX_SIZE:
          try {
            if (Integer.parseInt(value) <= 0) {
              return false;
            }
          } catch (NumberFormatException e) {
            return false;
          }
          info.put(key, value);
          break;
        case RPC_COMPRESS:
          if (isBoolean(value)) {
            info.put(key, value);
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
            if (Integer.parseInt(value) < 0) {
              return false;
            }
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

  private static int findAuthorityEnd(String subURL) {
    int slashIndex = subURL.indexOf(SLASH);
    int parameterIndex = subURL.indexOf(PARAMETER_SEPARATOR);
    if (slashIndex < 0) {
      return parameterIndex < 0 ? subURL.length() : parameterIndex;
    }
    if (parameterIndex < 0) {
      return slashIndex;
    }
    return Math.min(slashIndex, parameterIndex);
  }

  private static int parsePort(String portText) {
    if (portText.isEmpty()) {
      return -1;
    }
    for (int i = 0; i < portText.length(); i++) {
      if (!Character.isDigit(portText.charAt(i))) {
        return -1;
      }
    }
    try {
      return Integer.parseInt(portText);
    } catch (NumberFormatException e) {
      return -1;
    }
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

  private static int parsePositiveIntegerProperty(Properties properties, String key)
      throws IoTDBURLException {
    int value = parseIntegerProperty(properties, key);
    if (value <= 0) {
      throw invalidPropertyValue(key, String.valueOf(value), null);
    }
    return value;
  }

  private static int parseNonNegativeIntegerProperty(Properties properties, String key)
      throws IoTDBURLException {
    int value = parseIntegerProperty(properties, key);
    if (value < 0) {
      throw invalidPropertyValue(key, String.valueOf(value), null);
    }
    return value;
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

  private static boolean parseBooleanProperty(Properties properties, String key)
      throws IoTDBURLException {
    String value = getPropertyValue(properties, key);
    if (!isBoolean(value)) {
      throw invalidPropertyValue(key, value, null);
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
