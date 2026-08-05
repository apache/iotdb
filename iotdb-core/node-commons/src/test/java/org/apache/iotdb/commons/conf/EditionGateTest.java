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

package org.apache.iotdb.commons.conf;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EditionGateTest {

  @After
  public void tearDown() {
    EditionGate.clearIoTDBOverrideForTest();
  }

  @Test
  public void timechodbIsNoOp() {
    EditionGate.setIoTDBOverrideForTest(false);
    assertTrue(EditionGate.forceDisabledInIoTDB(true, IoTDBGatedFeature.WHITE_BLACK_LIST));
    assertTrue(EditionGate.forceDisabledInIoTDB(true, IoTDBGatedFeature.SECURE_ERASE));
    assertEquals(7, EditionGate.forceValueInIoTDB(7, 0, IoTDBGatedFeature.BRUTE_FORCE));
    assertEquals("x", EditionGate.forceValueInIoTDB("x", "UNENCRYPTED", IoTDBGatedFeature.TDE));
    assertFalse(EditionGate.isIoTDBGatedConfigKey("enable_white_list"));
    assertFalse(EditionGate.isIoTDBGatedConfigKey("enable_secure_erase"));
  }

  @Test
  public void iotdbClampsAndDetects() {
    EditionGate.setIoTDBOverrideForTest(true);
    assertFalse(EditionGate.forceDisabledInIoTDB(true, IoTDBGatedFeature.WHITE_BLACK_LIST));
    assertFalse(EditionGate.forceDisabledInIoTDB(true, IoTDBGatedFeature.SECURE_ERASE));
    assertEquals(0, EditionGate.forceValueInIoTDB(5, 0, IoTDBGatedFeature.BRUTE_FORCE));
    assertEquals(-1, EditionGate.forceValueInIoTDB(10, -1, IoTDBGatedFeature.IDLE_EVICTION));
    assertEquals(
        "UNENCRYPTED",
        EditionGate.forceValueInIoTDB("com.timecho.x", "UNENCRYPTED", IoTDBGatedFeature.TDE));
    assertTrue(EditionGate.isIoTDBGatedConfigKey("enable_white_list"));
    assertTrue(EditionGate.isIoTDBGatedConfigKey("enable_secure_erase"));
    Optional<String> hit =
        EditionGate.firstGatedKey(new HashSet<>(Arrays.asList("foo", "enable_internal_ssl")));
    assertTrue(hit.isPresent());
    assertEquals("enable_internal_ssl", hit.get());
  }

  @Test
  public void secureEraseConfigIsDisabledOnlyInIoTDB() {
    CommonConfig config = new CommonConfig();

    EditionGate.setIoTDBOverrideForTest(false);
    config.setEnableSecureErase(true);
    assertTrue(config.isEnableSecureErase());

    EditionGate.setIoTDBOverrideForTest(true);
    config.setEnableSecureErase(true);
    assertFalse(config.isEnableSecureErase());
  }
}
