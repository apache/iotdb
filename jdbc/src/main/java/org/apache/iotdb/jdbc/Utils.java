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

/**
 * Utils to convert between thrift format and TsFile format.
 */
public class Utils {

  /**
   * Parse JDBC connection URL The only supported format of the URL is:
   * jdbc:iotdb://localhost:6667/.
   */
  static IoTDBConnectionParams parseUrl(String url, Properties info)
      throws IoTDBURLException {
    IoTDBConnectionParams params = new IoTDBConnectionParams(url);
    if (url.trim().equalsIgnoreCase(Config.IOTDB_URL_PREFIX)) {
      return params;
    }
    boolean isUrlLegal = false;
    Pattern pattern = Pattern.compile("^"
        + "(((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}" // Domain name
        + "|"
        + "localhost" // localhost
        + "|"
        + "(([0-9]{1,3}\\.){3})[0-9]{1,3})" // Ip
        + ":"
        + "[0-9]{1,5}" // Port
        + "/?$");
    String subURL = url.substring(Config.IOTDB_URL_PREFIX.length());
    Matcher matcher = pattern.matcher(subURL);
    if(matcher.matches()) {
      isUrlLegal = true;
    }
    String[] DomainAndPort;
    if(subURL.contains("/")) {
      DomainAndPort = subURL.substring(0, subURL.length()-1).split(":");
    } else {
      DomainAndPort = subURL.split(":");
    }
    params.setHost(DomainAndPort[0]);
    params.setPort(Integer.parseInt(DomainAndPort[1]));
    if (!isUrlLegal) {
      throw new IoTDBURLException("Error url format, url should be jdbc:iotdb://domain|ip:port/ or jdbc:iotdb://domain|ip:port");
    }

    if (info.containsKey(Config.AUTH_USER)) {
      params.setUsername(info.getProperty(Config.AUTH_USER));
    }
    if (info.containsKey(Config.AUTH_PASSWORD)) {
      params.setPassword(info.getProperty(Config.AUTH_PASSWORD));
    }

    return params;
  }

  static float bytesToFloat(byte[] b) {
    return Float.intBitsToFloat(bytesToInt(b));
  }

  static int bytesToInt(byte[] b) {
    int l = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      l |= (b[3-i] << (8 * i)) & (0xFF << (8 * i));
    }

    return l;
  }

  static double bytesToDouble(byte[] b) {
    return Double.longBitsToDouble(bytesToLong(b));
  }

  static long bytesToLong(byte[] b) {
    long l = 0;
    for (int i = 0; i < Long.BYTES; i++) {
      l |= ((long) b[7-i] << (8 * i)) & (0xFFL << (8 * i));
    }

    return l;
  }
  
}