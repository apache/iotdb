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

/** Utils to convert between thrift format and TsFile format. */
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
        if (matcher.matches() && parseUrlParam(subURL, info)) {
          isUrlLegal = true;
        }
      }
    }
    if (!isUrlLegal) {
      throw new IoTDBURLException(
          "Error url format, url should be jdbc:iotdb://anything:port/[database] or jdbc:iotdb://anything:port[/database]?property1=value1&property2=value2, current url is "
              + url);
    }

    if (info.containsKey(Config.AUTH_USER)) {
      params.setUsername(info.getProperty(Config.AUTH_USER));
    }
    if (info.containsKey(Config.AUTH_PASSWORD)) {
      params.setPassword(info.getProperty(Config.AUTH_PASSWORD));
    }
    if (info.containsKey(Config.DEFAULT_BUFFER_CAPACITY)) {
      params.setThriftDefaultBufferSize(
          Integer.parseInt(info.getProperty(Config.DEFAULT_BUFFER_CAPACITY)));
    }
    if (info.containsKey(Config.THRIFT_FRAME_MAX_SIZE)) {
      params.setThriftMaxFrameSize(
          Integer.parseInt(info.getProperty(Config.THRIFT_FRAME_MAX_SIZE)));
    }
    if (info.containsKey(Config.VERSION)) {
      params.setVersion(Constant.Version.valueOf(info.getProperty(Config.VERSION)));
    }
    if (info.containsKey(Config.NETWORK_TIMEOUT)) {
      params.setNetworkTimeout(Integer.parseInt(info.getProperty(Config.NETWORK_TIMEOUT)));
    }
    if (info.containsKey(Config.TIME_ZONE)) {
      params.setTimeZone(info.getProperty(Config.TIME_ZONE));
    }
    if (info.containsKey(Config.CHARSET)) {
      params.setCharset(info.getProperty(Config.CHARSET));
    }
    if (info.containsKey(Config.USE_SSL)) {
      params.setUseSSL(Boolean.parseBoolean(info.getProperty(Config.USE_SSL)));
    }
    if (info.containsKey(Config.TRUST_STORE)) {
      params.setTrustStore(info.getProperty(Config.TRUST_STORE));
    }
    if (info.containsKey(Config.TRUST_STORE_PWD)) {
      params.setTrustStorePwd(info.getProperty(Config.TRUST_STORE_PWD));
    }
    if (info.containsKey(Config.SQL_DIALECT)) {
      params.setSqlDialect(info.getProperty(Config.SQL_DIALECT));
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
    String[] params = paramURL.split("&");
    for (String tmpParam : params) {
      String[] paramSplit = tmpParam.split("=");
      if (paramSplit.length != 2) {
        return false;
      }
      String key = tmpParam.split("=")[0];
      String value = tmpParam.split("=")[1];
      switch (key) {
        case RPC_COMPRESS:
          if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            Config.rpcThriftCompressionEnable = Boolean.parseBoolean(value);
          } else {
            return false;
          }
          break;
        case Config.USE_SSL:
        case Config.TRUST_STORE:
        case Config.TRUST_STORE_PWD:
        case Config.VERSION:
        case Config.NETWORK_TIMEOUT:
        case Config.SQL_DIALECT:
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

  private Utils() {}
}
