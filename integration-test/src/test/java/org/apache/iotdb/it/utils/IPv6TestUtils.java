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

package org.apache.iotdb.it.utils;

import org.junit.Assume;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.TEST_NODE_ADDRESS;

public final class IPv6TestUtils {

  public static final String IPV6_LOOPBACK_ADDRESS = "::1";

  private IPv6TestUtils() {
    // Utility class
  }

  public static void assumeIPv6LoopbackAvailable() {
    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress(InetAddress.getByName(IPV6_LOOPBACK_ADDRESS), 0));
    } catch (IOException e) {
      Assume.assumeNoException("IPv6 loopback is not available", e);
    }
  }

  public static String setTestNodeAddressToIPv6Loopback() {
    final String previousAddress = System.getProperty(TEST_NODE_ADDRESS);
    System.setProperty(TEST_NODE_ADDRESS, IPV6_LOOPBACK_ADDRESS);
    return previousAddress;
  }

  public static void restoreTestNodeAddress(final String previousAddress) {
    if (previousAddress == null) {
      System.clearProperty(TEST_NODE_ADDRESS);
    } else {
      System.setProperty(TEST_NODE_ADDRESS, previousAddress);
    }
  }
}
