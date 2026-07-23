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

import java.net.InetAddress;
import java.net.UnknownHostException;

/** The UrlUtils */
public class UrlUtils {
  private static final String PORT_SEPARATOR = ":";
  private static final String IPV6_BEGIN_MARK = "[";
  private static final String IPV6_END_MARK = "]";

  private UrlUtils() {}

  /**
   * Parse TEndPoint from a given TEndPointUrl
   * example:[D80:0000:0000:0000:ABAA:0000:00C2:0002]:22227
   *
   * @param endPointUrl host:port or [ipv6]:port
   * @return parsed TEndPoint
   * @throws NumberFormatException if the bracketed endpoint format is invalid or the port is not a
   *     number
   */
  public static TEndPoint parseTEndPointIpv4AndIpv6Url(String endPointUrl) {
    TEndPoint endPoint = new TEndPoint();
    if (endPointUrl.startsWith(IPV6_BEGIN_MARK)) {
      int endIndex = endPointUrl.indexOf(IPV6_END_MARK);
      if (endIndex <= 1
          || endIndex + 1 >= endPointUrl.length()
          || !PORT_SEPARATOR.equals(endPointUrl.substring(endIndex + 1, endIndex + 2))) {
        throw new NumberFormatException();
      }
      endPoint.setIp(endPointUrl.substring(1, endIndex));
      endPoint.setPort(Integer.parseInt(endPointUrl.substring(endIndex + 2)));
      return endPoint;
    }
    if (endPointUrl.contains(IPV6_BEGIN_MARK) || endPointUrl.contains(IPV6_END_MARK)) {
      throw new NumberFormatException();
    }
    int separatorIndex = endPointUrl.lastIndexOf(PORT_SEPARATOR);
    if (separatorIndex <= 0 || separatorIndex == endPointUrl.length() - 1) {
      throw new NumberFormatException();
    }
    endPoint.setIp(endPointUrl.substring(0, separatorIndex));
    endPoint.setPort(Integer.parseInt(endPointUrl.substring(separatorIndex + 1)));
    return endPoint;
  }

  /**
   * Convert TEndPoint to a URL string. IPv6 literals are surrounded by square brackets so the last
   * colon remains an unambiguous port separator.
   */
  public static String convertTEndPointIpv4AndIpv6Url(TEndPoint endPoint) {
    return formatTEndPointIpv4AndIpv6Url(endPoint.getIp(), endPoint.getPort());
  }

  /** Format host and port as host:port or [ipv6]:port. This method expects host without a port. */
  public static String formatTEndPointIpv4AndIpv6Url(String host, int port) {
    return formatTEndPointIpv4AndIpv6Url(host, String.valueOf(port));
  }

  /**
   * Format host and port as host:port or [ipv6]:port. This method expects host without a port and
   * leaves port validation to the endpoint consumer.
   */
  public static String formatTEndPointIpv4AndIpv6Url(String host, String port) {
    String formattedHost = host;
    if (isIpv6Literal(host) && !isBracketedIpv6Literal(host)) {
      formattedHost = IPV6_BEGIN_MARK + host + IPV6_END_MARK;
    }
    return formattedHost + PORT_SEPARATOR + port;
  }

  public static boolean isWildcardAddress(String host) {
    if (host == null) {
      return false;
    }
    String normalizedHost = host;
    if (isBracketedIpv6Literal(host)) {
      normalizedHost = host.substring(1, host.length() - 1);
    }
    if ("0.0.0.0".equals(normalizedHost)) {
      return true;
    }
    if (!isIpv6Literal(normalizedHost)) {
      return false;
    }
    try {
      return InetAddress.getByName(normalizedHost).isAnyLocalAddress();
    } catch (UnknownHostException e) {
      return false;
    }
  }

  private static boolean isIpv6Literal(String host) {
    return host.contains(PORT_SEPARATOR);
  }

  private static boolean isBracketedIpv6Literal(String host) {
    return host.startsWith(IPV6_BEGIN_MARK) && host.endsWith(IPV6_END_MARK);
  }
}
