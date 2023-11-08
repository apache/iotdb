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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UrlUtilsTest {

  @Test
  public void testParseIPV6URL() {
    String hostAndPoint = "D80:0000:0000:0000:ABAA:00BB:EEAA:BBDD:22227";
    TEndPoint endPoint = UrlUtils.parseTEndPointIpv4AndIpv6Url(hostAndPoint);
    assertEquals(endPoint.getIp(), "D80:0000:0000:0000:ABAA:00BB:EEAA:BBDD");
    assertEquals(endPoint.getPort(), 22227);
  }

  @Test
  public void testParseIPV6URL_2() {
    String hostAndPoint = "[D80:0000:0000:0000:ABAA:00BB:EEAA:BBDD]:22227";
    TEndPoint endPoint = UrlUtils.parseTEndPointIpv4AndIpv6Url(hostAndPoint);
    assertEquals(endPoint.getIp(), "D80:0000:0000:0000:ABAA:00BB:EEAA:BBDD");
    assertEquals(endPoint.getPort(), 22227);
  }

  @Test
  public void testParseIPV6AbbURL() {
    String hostAndPoint = "[D80::ABAA:0]:22227";
    TEndPoint endPoint = UrlUtils.parseTEndPointIpv4AndIpv6Url(hostAndPoint);
    assertEquals(endPoint.getIp(), "D80::ABAA:0");
    assertEquals(endPoint.getPort(), 22227);
  }

  @Test
  public void testParseIPV4URL() {
    String hostAndPoint = "192.0.0.1:22227";
    TEndPoint endPoint = UrlUtils.parseTEndPointIpv4AndIpv6Url(hostAndPoint);
    assertEquals(endPoint.getIp(), "192.0.0.1");
    assertEquals(endPoint.getPort(), 22227);
  }
}
