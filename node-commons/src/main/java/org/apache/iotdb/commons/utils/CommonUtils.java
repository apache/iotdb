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
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CommonUtils {
  private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

  public static TEndPoint parseNodeUrl(String nodeUrl) throws BadNodeUrlException {
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

  public static List<TEndPoint> parseNodeUrls(List<String> nodeUrls) throws BadNodeUrlException {
    List<TEndPoint> result = new ArrayList<>();
    for (String url : nodeUrls) {
      result.add(parseNodeUrl(url));
    }
    return result;
  }

  public static TConfigNodeLocation parseConfigNodeUrl(String configNodeUrl)
      throws BadNodeUrlException {
    String[] split = configNodeUrl.split(":");
    if (split.length != 3) {
      logger.warn("Bad ConfigNode url: {}", configNodeUrl);
      throw new BadNodeUrlException(String.format("Bad node url: %s", configNodeUrl));
    }
    String ip = split[0];
    TConfigNodeLocation result;
    try {
      int rpcPort = Integer.parseInt(split[1]);
      int consensusPort = Integer.parseInt(split[2]);
      result =
          new TConfigNodeLocation(new TEndPoint(ip, rpcPort), new TEndPoint(ip, consensusPort));
    } catch (NumberFormatException e) {
      logger.warn("Bad node url: {}", configNodeUrl);
      throw new BadNodeUrlException(String.format("Bad node url: %s", configNodeUrl));
    }
    return result;
  }

  public static List<TConfigNodeLocation> parseConfigNodeUrls(List<String> configNodeUrls)
      throws BadNodeUrlException {
    List<TConfigNodeLocation> result = new ArrayList<>();
    for (String url : configNodeUrls) {
      result.add(parseConfigNodeUrl(url));
    }
    return result;
  }
}
