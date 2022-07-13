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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.exception.BadNodeUrlException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IoTDBStopCheckTest {
  static List<String> removeIps;
  static List<String> emptyRemoveIps;
  static List<String> reduplicateRemoveIps;
  static List<String> notExistRemovedIps;
  static List<String> onlineIps;
  static List<String> emptyOnlineIps;

  @BeforeClass
  public static void setup() {
    onlineIps = Arrays.asList("192.168.1.1", "192.168.1.2", "192.168.1.3");
    emptyOnlineIps = Collections.emptyList();

    removeIps = Arrays.asList("192.168.1.1", "192.168.1.2");
    notExistRemovedIps = Arrays.asList("192.168.1.1", "192.168.1.4");
    emptyRemoveIps = Collections.emptyList();
    reduplicateRemoveIps = Arrays.asList("192.168.1.1", "192.168.1.2", "192.168.1.2");
  }

  @AfterClass
  public static void after() {
    removeIps = null;
    emptyRemoveIps = null;
    reduplicateRemoveIps = null;
    notExistRemovedIps = null;

    onlineIps = null;
    emptyOnlineIps = null;
  }

  @Test
  public void testGetInstance() {
    IoTDBStopCheck checker = IoTDBStopCheck.getInstance();
    Assert.assertNotNull(checker);
  }

  @Test
  public void testCheckDuplicateIpOK() {
    boolean result = true;
    try {
      IoTDBStopCheck.getInstance().checkDuplicateIp(removeIps);
    } catch (BadNodeUrlException e) {
      result = false;
    }
    Assert.assertTrue(result);
  }

  @Test(expected = BadNodeUrlException.class)
  public void testCheckDuplicateIpEmptyIps() throws BadNodeUrlException {
    IoTDBStopCheck.getInstance().checkDuplicateIp(emptyRemoveIps);
  }

  @Test(expected = BadNodeUrlException.class)
  public void testCheckDuplicateIpException() throws BadNodeUrlException {
    IoTDBStopCheck.getInstance().checkDuplicateIp(reduplicateRemoveIps);
  }

  @Test
  public void testCheckIpInCluster() {
    boolean result = true;
    try {
      IoTDBStopCheck.getInstance().checkIpInCluster(removeIps, onlineIps);
    } catch (BadNodeUrlException e) {
      result = false;
    }

    Assert.assertTrue(result);
  }

  @Test(expected = BadNodeUrlException.class)
  public void testCheckIpInClusterEmptyIp() throws BadNodeUrlException {
    IoTDBStopCheck.getInstance().checkIpInCluster(removeIps, emptyOnlineIps);
  }

  @Test(expected = BadNodeUrlException.class)
  public void testCheckIpInClusterException() throws BadNodeUrlException {
    IoTDBStopCheck.getInstance().checkIpInCluster(notExistRemovedIps, onlineIps);
  }
}
