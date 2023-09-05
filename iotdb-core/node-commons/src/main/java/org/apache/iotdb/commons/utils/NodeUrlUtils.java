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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.BadNodeUrlException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class NodeUrlUtils {

  private static final Logger logger = LoggerFactory.getLogger(NodeUrlUtils.class);

  /**
   * Convert TEndPoint to TEndPointUrl
   *
   * @param endPoint TEndPoint
   * @return TEndPointUrl with format ip:port
   */
  public static String convertTEndPointUrl(TEndPoint endPoint) {
    StringJoiner url = new StringJoiner(":");
    url.add(endPoint.getIp());
    url.add(String.valueOf(endPoint.getPort()));
    return url.toString();
  }

  /**
   * Convert TEndPoints to TEndPointUrls
   *
   * @param endPoints List<TEndPoint>
   * @return TEndPointUrls with format TEndPointUrl_0,TEndPointUrl_1,...,TEndPointUrl_n
   */
  public static String convertTEndPointUrls(List<TEndPoint> endPoints) {
    StringJoiner urls = new StringJoiner(",");
    for (TEndPoint endPoint : endPoints) {
      urls.add(convertTEndPointUrl(endPoint));
    }
    return urls.toString();
  }

  /**
   * Parse TEndPoint from a given TEndPointUrl
   *
   * @param endPointUrl ip:port
   * @return TEndPoint
   * @throws BadNodeUrlException Throw when unable to parse
   */
  public static TEndPoint parseTEndPointUrl(String endPointUrl) throws BadNodeUrlException {
    String[] split = endPointUrl.split(":");
    if (split.length != 2) {
      logger.warn("Illegal endpoint url format: {}", endPointUrl);
      throw new BadNodeUrlException(String.format("Bad endpoint url: %s", endPointUrl));
    }
    String ip = split[0];
    TEndPoint result;
    try {
      int port = Integer.parseInt(split[1]);
      result = new TEndPoint(ip, port);
    } catch (NumberFormatException e) {
      logger.warn("Illegal endpoint url format: {}", endPointUrl);
      throw new BadNodeUrlException(String.format("Bad node url: %s", endPointUrl));
    }
    return result;
  }

  /**
   * Parse TEndPoints from given TEndPointUrls
   *
   * @param endPointUrls List<TEndPointUrl>
   * @return List<TEndPoint>
   * @throws BadNodeUrlException Throw when unable to parse
   */
  public static List<TEndPoint> parseTEndPointUrls(List<String> endPointUrls)
      throws BadNodeUrlException {
    List<TEndPoint> result = new ArrayList<>();
    for (String url : endPointUrls) {
      result.add(parseTEndPointUrl(url));
    }
    return result;
  }

  /**
   * Parse TEndPoints from given TEndPointUrls
   *
   * @param endPointUrls TEndPointUrls
   * @return List<TEndPoint>
   * @throws BadNodeUrlException Throw when unable to parse
   */
  public static List<TEndPoint> parseTEndPointUrls(String endPointUrls) throws BadNodeUrlException {
    return parseTEndPointUrls(Arrays.asList(endPointUrls.split(",")));
  }

  /**
   * Convert TConfigNodeLocation to TConfigNodeUrl
   *
   * @param configNodeLocation TConfigNodeLocation
   * @return TConfigNodeUrl with format InternalEndPointUrl,ConsensusEndPointUrl
   */
  public static String convertTConfigNodeUrl(TConfigNodeLocation configNodeLocation) {
    StringJoiner url = new StringJoiner(",");
    url.add(String.valueOf(configNodeLocation.getConfigNodeId()));
    url.add(convertTEndPointUrl(configNodeLocation.getInternalEndPoint()));
    url.add(convertTEndPointUrl(configNodeLocation.getConsensusEndPoint()));
    return url.toString();
  }

  /**
   * Convert TConfigNodeLocations to TConfigNodeUrls
   *
   * @param configNodeLocations List<TConfigNodeLocation>
   * @return TConfigNodeUrls with format TConfigNodeUrl_0,TConfigNodeUrl_1,...,TConfigNodeUrl_n
   */
  public static String convertTConfigNodeUrls(List<TConfigNodeLocation> configNodeLocations) {
    StringJoiner urls = new StringJoiner(";");
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      urls.add(convertTConfigNodeUrl(configNodeLocation));
    }
    return urls.toString();
  }

  /**
   * Parse TConfigNodeLocation from given TConfigNodeUrl
   *
   * @param configNodeUrl InternalEndPointUrl,ConsensusEndPointUrl
   * @return TConfigNodeLocation
   * @throws BadNodeUrlException Throw when unable to parse
   */
  public static TConfigNodeLocation parseTConfigNodeUrl(String configNodeUrl)
      throws BadNodeUrlException {
    String[] split = configNodeUrl.split(",");
    if (split.length != 3) {
      logger.warn("Bad ConfigNode url: {}", configNodeUrl);
      throw new BadNodeUrlException(String.format("Bad node url: %s", configNodeUrl));
    }
    return new TConfigNodeLocation(
        Integer.parseInt(split[0]), parseTEndPointUrl(split[1]), parseTEndPointUrl(split[2]));
  }

  /**
   * Parse TConfigNodeLocations from given TConfigNodeUrls
   *
   * @param configNodeUrls List<TConfigNodeUrl>
   * @return List<TConfigNodeLocation>
   * @throws BadNodeUrlException Throw when unable to parse
   */
  public static List<TConfigNodeLocation> parseTConfigNodeUrls(List<String> configNodeUrls)
      throws BadNodeUrlException {
    List<TConfigNodeLocation> result = new ArrayList<>();
    for (String url : configNodeUrls) {
      result.add(parseTConfigNodeUrl(url));
    }
    return result;
  }

  /**
   * Parse TConfigNodeLocations from given TConfigNodeUrls
   *
   * @param configNodeUrls TConfigNodeUrls
   * @return List<TConfigNodeLocation>
   * @throws BadNodeUrlException Throw when unable to parse
   */
  public static List<TConfigNodeLocation> parseTConfigNodeUrls(String configNodeUrls)
      throws BadNodeUrlException {
    return parseTConfigNodeUrls(Arrays.asList(configNodeUrls.split(";")));
  }

  public static boolean isLocalAddress(String ip) {
    if (ip == null) {
      return false;
    }
    return ip.equals("0.0.0.0") || ip.equals("127.0.0.1") || ip.equals("localhost");
  }
}
