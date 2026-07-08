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

import org.apache.iotdb.commons.conf.EditionGate;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IoTDBConfigProGateTest {

  @After
  public void tearDown() {
    EditionGate.clearProOverrideForTest();
  }

  @Test
  public void proForcesSecurityOff() {
    EditionGate.setProOverrideForTest(true);
    IoTDBConfig c = IoTDBDescriptor.getInstance().getConfig();

    c.setFailedLoginAttempts(5);
    assertEquals(0, c.getFailedLoginAttempts());

    c.setFailedLoginAttemptsPerUser(1000);
    assertEquals(0, c.getFailedLoginAttemptsPerUser());

    c.setEnableWhiteList(true);
    assertFalse(c.isEnableWhiteList());

    c.setEnableBlackList(true);
    assertFalse(c.isEnableBlackList());

    // idle disabled value MUST be -1 (0 would be coerced to a 1-minute timeout by the loader)
    c.setIdleSessionTimeoutInMinutes(30);
    assertEquals(-1, c.getIdleSessionTimeoutInMinutes());
  }
}
