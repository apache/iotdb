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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.BadNodeUrlException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CommonUtils {
  private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

  public static TEndPoint parseSingleNodeUrl(String nodeUrl) throws BadNodeUrlException {
    String[] split = nodeUrl.split(":");
    if (split.length != 2) {
      logger.warn("Bad node url: {}", nodeUrl);
      throw new BadNodeUrlException(String.format("Bad node url: %s", nodeUrl));
    }
    String ip = split[0];
    TEndPoint result;
    try {
      int port = Integer.parseInt(split[1]);
      result = new TEndPoint(ip, port);
    } catch (NumberFormatException e) {
      logger.warn("Bad node url: {}", nodeUrl);
      throw new BadNodeUrlException(String.format("Bad node url: %s", nodeUrl));
    }
    return result;
  }

  /**
   * Split the node urls and parse urls to endpoints.
   *
   * @param nodeUrl the config node urls.
   * @return the node endpoints as a list.
   */
  public static List<TEndPoint> parseNodeUrl(String nodeUrl) throws BadNodeUrlException {
    if (nodeUrl == null) {
      return Collections.emptyList();
    }
    List<TEndPoint> configNodeList = new ArrayList<>();
    String[] split = nodeUrl.split(",");
    for (String singleNodeUrl : split) {
      singleNodeUrl = singleNodeUrl.trim();
      if ("".equals(singleNodeUrl)) {
        continue;
      }
      configNodeList.add(parseSingleNodeUrl(singleNodeUrl));
    }
    return configNodeList;
  }

  public static String toNodeUrl(List<TEndPoint> nodeList) {
    StringBuilder stringBuilder = new StringBuilder();
    for (TEndPoint endPoint : nodeList) {
      stringBuilder.append(endPoint.getIp()).append(':').append(endPoint.getPort()).append(',');
    }
    return stringBuilder.toString();
  }
}
