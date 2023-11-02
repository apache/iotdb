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

package org.apache.iotdb.rpc;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/** The UrlUtils */
public class UrlUtils {
  private static final Logger logger = LoggerFactory.getLogger(UrlUtils.class);
  public static final String COLON = ":";

  private UrlUtils() {}

  /**
   * Parse TEndPoint from a given TEndPointUrl
   *
   * @param endPointUrl ip:port
   * @return TEndPoint null if parse error
   */
  public static TEndPoint parseTEndPointIpv4AndIpv6Url(String endPointUrl) {
    TEndPoint endPoint = new TEndPoint();
    InetSocketAddress address = getSocketAddress(endPointUrl);
    if (address != null) {
      endPoint.setIp(address.getHostString());
      endPoint.setPort(address.getPort());
    }
    return endPoint;
  }

  /**
   * Generate address from url, support ipv4 and ipv6
   *
   * @param url example:D80:0000:0000:0000:ABAA:0000:00C2:0002:22227
   * @return socket address
   */
  private static InetSocketAddress getSocketAddress(String url) {
    String ip = null;
    String port = null;
    InetSocketAddress address = null;
    if (url.contains(COLON)) {
      int i;
      if (url.contains("%")) {
        i = url.lastIndexOf("%");
        url = url.trim();
      } else {
        i = url.lastIndexOf(COLON);
      }
      port = url.substring(url.lastIndexOf(COLON) + 1);
      ip = url.substring(0, i);

      try {
        address = new InetSocketAddress(ip, Integer.parseInt(port));
      } catch (NumberFormatException e) {
        logger.warn("Bad url: {}", url);
      }
    } else {
      logger.warn("Bad url: {}", url);
    }

    return address;
  }
}
