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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.service.rpc.thrift.EndPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CommonUtils {
  private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

  public static EndPoint parseNodeUrl(String nodeUrl) {
    EndPoint result = new EndPoint();
    String[] split = nodeUrl.split(":");
    if (split.length != 2) {
      logger.warn("Bad node url: {}", nodeUrl);
      return null;
    }
    String ip = split[0];
    try {
      int port = Integer.parseInt(split[1]);
      result.setIp(ip).setPort(port);
    } catch (NumberFormatException e) {
      logger.warn("Bad node url: {}", nodeUrl);
    }
    return result;
  }

  public static List<EndPoint> parseNodeUrls(List<String> nodeUrls) {
    List<EndPoint> result = new ArrayList<>();
    for (String url : nodeUrls) {
      result.add(parseNodeUrl(url));
    }
    return result;
  }
}
