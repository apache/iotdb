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

package org.apache.iotdb.postgresql.config;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Checks the address the PostgreSQL endpoint is bound to: an IPv4 or IPv6 literal that is a
 * loopback address, a wildcard or an address of this machine. Host names are refused (a name can
 * resolve to several or changing addresses), and no DNS lookup is ever made.
 */
final class BindAddresses {

  private static final Pattern IPV4 =
      Pattern.compile(
          "^(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}$");

  private BindAddresses() {}

  /**
   * Why an address cannot be used for binding, or empty if it can.
   *
   * @param value the configured address
   * @return the reason, or empty for a usable address
   */
  static Optional<String> problem(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Optional.of("no address given; use an IP address such as 127.0.0.1");
    }
    String trimmed = value.trim();
    InetAddress address = parseLiteral(trimmed);
    if (address == null) {
      return Optional.of(
          "'" + trimmed + "' is not an IP address; use an IP address such as 127.0.0.1");
    }
    if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
      return Optional.empty();
    }
    try {
      if (NetworkInterface.getByInetAddress(address) != null) {
        return Optional.empty();
      }
    } catch (SocketException e) {
      return Optional.of(
          "the network interfaces of this machine cannot be read: " + e.getMessage());
    }
    return Optional.of("'" + trimmed + "' is not an address of this machine");
  }

  /** Whether the value is a wildcard address (all interfaces). */
  static boolean isWildcard(String value) {
    InetAddress address = value == null ? null : parseLiteral(value.trim());
    return address != null && address.isAnyLocalAddress();
  }

  /** Parses an IP literal without any DNS lookup; returns null for anything else. */
  static InetAddress parseLiteral(String value) {
    String candidate = value;
    if (candidate.startsWith("[") && candidate.endsWith("]")) {
      candidate = candidate.substring(1, candidate.length() - 1);
    }
    if (!IPV4.matcher(candidate).matches() && !looksLikeIpv6(candidate)) {
      return null;
    }
    try {
      // a literal never triggers a lookup
      return InetAddress.getByName(candidate);
    } catch (UnknownHostException | SecurityException e) {
      return null;
    }
  }

  /** Hex digits and colons (at least one), an optional embedded IPv4 tail and zone id. */
  private static boolean looksLikeIpv6(String value) {
    int percent = value.indexOf('%');
    String address = percent < 0 ? value : value.substring(0, percent);
    if (address.indexOf(':') < 0) {
      return false;
    }
    for (int i = 0; i < address.length(); i++) {
      char c = address.charAt(i);
      if (!(c == ':' || c == '.' || isAsciiHexDigit(c))) {
        return false;
      }
    }
    if (percent < 0) {
      return true;
    }
    String zone = value.substring(percent + 1);
    if (zone.isEmpty()) {
      return false;
    }
    for (int i = 0; i < zone.length(); i++) {
      char c = zone.charAt(i);
      if (!(isAsciiHexDigit(c)
          || (c >= 'g' && c <= 'z')
          || (c >= 'G' && c <= 'Z')
          || c == '_'
          || c == '.'
          || c == '-')) {
        return false;
      }
    }
    return true;
  }

  private static boolean isAsciiHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
  }
}
