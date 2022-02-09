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

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utils to convert between thrift format and TsFile format. */
public class Utils {

  static final Pattern URL_PATTERN = Pattern.compile("([^:]+):([0-9]{1,5})(/|\\?.*=.*(&.*=.*)*)?");

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
    if (url.startsWith(Config.IOTDB_URL_PREFIX)) {
      String subURL = url.substring(Config.IOTDB_URL_PREFIX.length());
      matcher = URL_PATTERN.matcher(subURL);
      if (matcher.matches()) {
        if (parseUrlParam(subURL)) {
          isUrlLegal = true;
        }
      }
    }
    if (!isUrlLegal) {
      throw new IoTDBURLException(
          "Error url format, url should be jdbc:iotdb://anything:port/ or jdbc:iotdb://anything:port?property1=value1&property2=value2");
    }

    params.setHost(matcher.group(1));
    params.setPort(Integer.parseInt(matcher.group(2)));

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
  private static boolean parseUrlParam(String subURL) {
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
            Config.rpcThriftCompressionEnable = Boolean.getBoolean(value);
          } else {
            return false;
          }
          break;
        default:
          return false;
      }
    }
    return true;
  }
}
