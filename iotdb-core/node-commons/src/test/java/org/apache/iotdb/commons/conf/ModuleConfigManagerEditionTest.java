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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ModuleConfigManagerEditionTest {

  // Default (IDE/unfiltered or TimechoDB build) should resolve to TIMECHODB
  @Test
  public void testDefaultEditionIsTimechoDB() {
    Edition edition = ModuleConfigManager.getInstance().getEdition();
    assertEquals(Edition.TIMECHODB, edition);
    assertTrue(ModuleConfigManager.getInstance().isTimechoDB());
    assertFalse(ModuleConfigManager.getInstance().isIoTDB());
  }

  // Parsing robustness: unknown/empty falls back to TIMECHODB
  @Test
  public void testParseEditionFallback() {
    assertEquals(Edition.TIMECHODB, Edition.fromString(null));
    assertEquals(Edition.TIMECHODB, Edition.fromString(""));
    assertEquals(Edition.TIMECHODB, Edition.fromString("garbage"));
    assertEquals(Edition.TIMECHODB, Edition.fromString("${edition}")); // unfiltered placeholder
    assertEquals(Edition.IOTDB, Edition.fromString("IOTDB"));
    assertEquals(Edition.IOTDB, Edition.fromString("iotdb"));
    assertEquals(Edition.TIMECHODB, Edition.fromString("TIMECHODB"));
  }

  @Test
  public void testReleaseCodeAndDisplayName() {
    assertEquals(1, Edition.TIMECHODB.getRelease());
    assertEquals(2, Edition.IOTDB.getRelease());
    assertEquals("TimechoDB", Edition.TIMECHODB.getDisplayName());
    assertEquals("IoTDB", Edition.IOTDB.getDisplayName());
  }
}
