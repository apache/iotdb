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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.timecho.iotdb.rpc;

import com.google.common.net.InetAddresses;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Matches IPv4/IPv6 literals and CIDR ranges while retaining the legacy IPv4 wildcard syntax. */
final class IPMatcher {

  private final Set<String> legacyIpv4Patterns;
  private final List<CidrRule> cidrRules;

  private IPMatcher(Set<String> legacyIpv4Patterns, List<CidrRule> cidrRules) {
    this.legacyIpv4Patterns = legacyIpv4Patterns;
    this.cidrRules = cidrRules;
  }

  static BuildResult build(Collection<String> patterns) {
    Set<String> legacyIpv4Patterns = new LinkedHashSet<>();
    List<CidrRule> cidrRules = new ArrayList<>();
    Set<String> invalidPatterns = new LinkedHashSet<>();

    if (patterns != null) {
      for (String rawPattern : patterns) {
        if (rawPattern == null) {
          continue;
        }
        String pattern = rawPattern.trim();
        if (pattern.isEmpty()) {
          continue;
        }

        // Keep the established IPv4 exact and wildcard syntax unchanged, including
        // any formatting accepted by the original IPv4 validator.
        if (pattern.matches(IPFilter.IP_LIST_PATTERN)) {
          legacyIpv4Patterns.add(pattern);
          continue;
        }

        if (pattern.indexOf('*') >= 0) {
          invalidPatterns.add(pattern);
          continue;
        }

        CidrRule rule = parseCidrRule(pattern);
        if (rule == null) {
          invalidPatterns.add(pattern);
        } else {
          cidrRules.add(rule);
        }
      }
    }

    return new BuildResult(
        new IPMatcher(
            Collections.unmodifiableSet(new LinkedHashSet<>(legacyIpv4Patterns)),
            Collections.unmodifiableList(cidrRules)),
        invalidPatterns);
  }

  boolean matches(String address) {
    if (address == null) {
      return false;
    }
    String normalizedAddress = normalizeAddress(address);
    InetAddress clientAddress = parseAddress(normalizedAddress);
    if (clientAddress == null) {
      return false;
    }

    // Legacy wildcard patterns are an IPv4-only compatibility feature. Check the parsed
    // address family before applying them so a dotted IPv4-mapped IPv6 literal cannot match an
    // IPv4 wildcard rule.
    if (clientAddress.getAddress().length == Integer.BYTES) {
      for (String legacyPattern : legacyIpv4Patterns) {
        if (wildcardMatch(normalizedAddress, legacyPattern)) {
          return true;
        }
      }
    }
    for (CidrRule rule : cidrRules) {
      if (rule.matches(clientAddress.getAddress())) {
        return true;
      }
    }
    return false;
  }

  boolean isEmpty() {
    return legacyIpv4Patterns.isEmpty() && cidrRules.isEmpty();
  }

  private static boolean wildcardMatch(String value, String pattern) {
    int valueIndex = 0;
    int patternIndex = 0;
    int wildcardIndex = -1;
    int valueAfterWildcard = -1;
    while (valueIndex < value.length()) {
      if (patternIndex < pattern.length()
          && value.charAt(valueIndex) == pattern.charAt(patternIndex)) {
        valueIndex++;
        patternIndex++;
      } else if (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
        wildcardIndex = patternIndex++;
        valueAfterWildcard = valueIndex;
      } else if (wildcardIndex >= 0) {
        patternIndex = wildcardIndex + 1;
        valueIndex = ++valueAfterWildcard;
      } else {
        return false;
      }
    }
    while (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
      patternIndex++;
    }
    return patternIndex == pattern.length();
  }

  private static CidrRule parseCidrRule(String pattern) {
    String addressText = pattern;
    int prefixLength = -1;
    int slash = pattern.indexOf('/');
    if (slash >= 0) {
      if (slash != pattern.lastIndexOf('/') || slash == 0 || slash == pattern.length() - 1) {
        return null;
      }
      addressText = pattern.substring(0, slash);
      String prefixText = pattern.substring(slash + 1);
      if (!prefixText.matches("\\d+")) {
        return null;
      }
      try {
        prefixLength = Integer.parseInt(prefixText);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    InetAddress address = parseAddress(addressText);
    if (address == null) {
      return null;
    }
    int addressBits = address.getAddress().length * Byte.SIZE;
    if (slash >= 0 && prefixLength < 0) {
      return null;
    }
    if (prefixLength < 0) {
      prefixLength = addressBits;
    }
    if (prefixLength > addressBits) {
      return null;
    }
    return new CidrRule(address.getAddress(), prefixLength);
  }

  private static InetAddress parseAddress(String address) {
    if (!InetAddresses.isInetAddress(address)) {
      return null;
    }
    InetAddress parsedAddress = InetAddresses.forString(address);
    // Guava deliberately returns an Inet4Address for IPv4-mapped IPv6 literals. Keep the
    // textual address family so an IPv4 rule does not unexpectedly match an IPv6 rule.
    if (address.indexOf(':') >= 0 && parsedAddress.getAddress().length == Integer.BYTES) {
      byte[] ipv6Address = new byte[16];
      ipv6Address[10] = (byte) 0xFF;
      ipv6Address[11] = (byte) 0xFF;
      System.arraycopy(parsedAddress.getAddress(), 0, ipv6Address, 12, Integer.BYTES);
      try {
        return Inet6Address.getByAddress(null, ipv6Address, -1);
      } catch (UnknownHostException e) {
        return null;
      }
    }
    return parsedAddress;
  }

  private static String normalizeAddress(String address) {
    String normalized = address.trim();
    if (normalized.length() > 1
        && normalized.charAt(0) == '['
        && normalized.charAt(normalized.length() - 1) == ']') {
      normalized = normalized.substring(1, normalized.length() - 1);
    }
    int scopeSeparator = normalized.indexOf('%');
    if (scopeSeparator >= 0) {
      normalized = normalized.substring(0, scopeSeparator);
    }
    return normalized;
  }

  static final class BuildResult {
    private final IPMatcher matcher;
    private final Set<String> invalidPatterns;

    private BuildResult(IPMatcher matcher, Set<String> invalidPatterns) {
      this.matcher = matcher;
      this.invalidPatterns = Collections.unmodifiableSet(new LinkedHashSet<>(invalidPatterns));
    }

    IPMatcher getMatcher() {
      return matcher;
    }

    Set<String> getInvalidPatterns() {
      return invalidPatterns;
    }
  }

  private static final class CidrRule {
    private final byte[] networkAddress;
    private final int prefixLength;

    private CidrRule(byte[] address, int prefixLength) {
      this.networkAddress = address.clone();
      this.prefixLength = prefixLength;
      maskTrailingBits(this.networkAddress, prefixLength);
    }

    private boolean matches(byte[] address) {
      if (address.length != networkAddress.length) {
        return false;
      }
      int fullBytes = prefixLength / Byte.SIZE;
      for (int i = 0; i < fullBytes; i++) {
        if (address[i] != networkAddress[i]) {
          return false;
        }
      }
      int remainingBits = prefixLength % Byte.SIZE;
      if (remainingBits == 0) {
        return true;
      }
      int mask = 0xFF << (Byte.SIZE - remainingBits);
      return (address[fullBytes] & mask) == (networkAddress[fullBytes] & mask);
    }

    private static void maskTrailingBits(byte[] address, int prefixLength) {
      int fullBytes = prefixLength / Byte.SIZE;
      int remainingBits = prefixLength % Byte.SIZE;
      if (fullBytes < address.length) {
        if (remainingBits > 0) {
          address[fullBytes] &= (byte) (0xFF << (Byte.SIZE - remainingBits));
          fullBytes++;
        }
        for (int i = fullBytes; i < address.length; i++) {
          address[i] = 0;
        }
      }
    }
  }
}
