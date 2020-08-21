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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils to convert between thrift format and TsFile format.
 */
public class Utils {

  static final Pattern URL_PATTERN = Pattern.compile("([^:]+):([0-9]{1,5})/?");
  static final String PARAMS_SEPARATION = "?";
  static final String PARAMS_ASSIGNMENT = "=";
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
    Matcher matcher = null;
    if (url.startsWith(Config.IOTDB_URL_PREFIX)) {
      String subURL = url.substring(Config.IOTDB_URL_PREFIX.length());
      Map<String,String> paramKV = new HashMap<>();
      if (subURL.contains(PARAMS_SEPARATION)){
        String[] parameters = subURL.split("["+PARAMS_SEPARATION+"]");
        subURL = parameters[0];
        for (int i=1; i<parameters.length; i++){
          String[] kv = parameters[i].split(PARAMS_ASSIGNMENT);
          if (kv.length<2){
            continue;
          }
          paramKV.put(kv[0],kv[1]);
        }
      }
      params.setParams(paramKV);
      matcher = URL_PATTERN.matcher(subURL);
      if (matcher.matches()) {
        isUrlLegal = true;
      }
    }
    if (!isUrlLegal) {
      throw new IoTDBURLException("Error url format, url should be jdbc:iotdb://anything:port/ or jdbc:iotdb://anything:port");
    }
    params.setHost(matcher.group(1));
    params.setPort(Integer.parseInt(matcher.group(2)));

    if (info.containsKey(Config.AUTH_USER)) {
      params.setUsername(info.getProperty(Config.AUTH_USER));
    }
    if (info.containsKey(Config.AUTH_PASSWORD)) {
      params.setPassword(info.getProperty(Config.AUTH_PASSWORD));
    }

    return params;
  }

}
