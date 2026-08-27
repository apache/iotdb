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
package com.timecho.iotdb.rpc;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

import static com.timecho.iotdb.rpc.IPFilter.IP_LIST_PATTERN;

public class IPPatternTest {

  @Test
  public void testIpPattern() {
    Assert.assertTrue(Pattern.matches(IP_LIST_PATTERN, "192.168.0.1"));
    Assert.assertTrue(Pattern.matches(IP_LIST_PATTERN, "192.*.0.1"));
    Assert.assertTrue(Pattern.matches(IP_LIST_PATTERN, "*.*.*.*"));
    Assert.assertTrue(Pattern.matches(IP_LIST_PATTERN, "127.0.0.1"));
    Assert.assertTrue(Pattern.matches(IP_LIST_PATTERN, "0.1.10.1"));
  }

  @Test
  public void testIpv6AddressAndCidrMatching() {
    IPMatcher.BuildResult result =
        IPMatcher.build(
            Arrays.asList("127.0.*.*", "2001:db8::1", "2001:db8:100::/48", "192.0.2.0/24"));

    Assert.assertTrue(result.getInvalidPatterns().isEmpty());
    Assert.assertTrue(result.getMatcher().matches("127.0.10.20"));
    Assert.assertTrue(result.getMatcher().matches("2001:0DB8:0:0:0:0:0:1"));
    Assert.assertTrue(result.getMatcher().matches("2001:db8:100:1::42"));
    Assert.assertTrue(result.getMatcher().matches("192.0.2.42"));
    Assert.assertFalse(result.getMatcher().matches("2001:db8:101::42"));
    Assert.assertFalse(result.getMatcher().matches("192.0.3.42"));
  }

  @Test
  public void testIpv4AndIpv6AddressFamiliesDoNotCrossMatch() {
    IPMatcher.BuildResult result =
        IPMatcher.build(Arrays.asList("192.0.2.10", "192.*.*.*", "::ffff:192.0.2.11"));

    Assert.assertTrue(result.getMatcher().matches("192.0.2.10"));
    Assert.assertFalse(result.getMatcher().matches("::ffff:192.0.2.10"));
    Assert.assertTrue(result.getMatcher().matches("::ffff:192.0.2.11"));
    Assert.assertFalse(result.getMatcher().matches("::ffff:192.0.2.12"));
    Assert.assertTrue(result.getMatcher().matches("192.0.2.11"));
  }

  @Test
  public void testInvalidIpv6PatternsAreReported() {
    IPMatcher.BuildResult result =
        IPMatcher.build(
            Arrays.asList("2001:db8::/129", "192.0.2.0/-1", "2001:db8:*", "[2001:db8::1]:6667"));

    Assert.assertEquals(4, result.getInvalidPatterns().size());
    Assert.assertFalse(result.getMatcher().matches("2001:db8::1"));
  }

  @Test
  public void testIpv6ScopeIsIgnoredForClientAddress() {
    IPMatcher.BuildResult result = IPMatcher.build(Arrays.asList("fe80::/64"));

    Assert.assertTrue(result.getMatcher().matches("fe80::1%eth0"));
    Assert.assertTrue(result.getMatcher().matches("[fe80::1%eth0]"));
  }
}
