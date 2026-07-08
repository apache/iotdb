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
    EditionGate.clearProOverrideForTest();
  }

  @Test
  public void maxIsNoOp() {
    EditionGate.setProOverrideForTest(false);
    assertTrue(EditionGate.forceDisabledInPro(true, ProFeature.WHITE_BLACK_LIST));
    assertEquals(7, EditionGate.forceValueInPro(7, 0, ProFeature.BRUTE_FORCE));
    assertEquals("x", EditionGate.forceValueInPro("x", "UNENCRYPTED", ProFeature.TDE));
    assertFalse(EditionGate.isProGatedConfigKey("enable_white_list"));
  }

  @Test
  public void proClampsAndDetects() {
    EditionGate.setProOverrideForTest(true);
    assertFalse(EditionGate.forceDisabledInPro(true, ProFeature.WHITE_BLACK_LIST));
    assertEquals(0, EditionGate.forceValueInPro(5, 0, ProFeature.BRUTE_FORCE));
    assertEquals(-1, EditionGate.forceValueInPro(10, -1, ProFeature.IDLE_EVICTION));
    assertEquals(
        "UNENCRYPTED", EditionGate.forceValueInPro("com.timecho.x", "UNENCRYPTED", ProFeature.TDE));
    assertTrue(EditionGate.isProGatedConfigKey("enable_white_list"));
    Optional<String> hit =
        EditionGate.firstGatedKey(new HashSet<>(Arrays.asList("foo", "enable_internal_ssl")));
    assertTrue(hit.isPresent());
    assertEquals("enable_internal_ssl", hit.get());
  }
}
