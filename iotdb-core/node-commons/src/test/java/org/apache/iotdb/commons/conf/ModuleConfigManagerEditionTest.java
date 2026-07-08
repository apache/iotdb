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

  // Default (IDE/unfiltered or MAX build) should resolve to MAX
  @Test
  public void testDefaultEditionIsMax() {
    Edition edition = ModuleConfigManager.getInstance().getEdition();
    assertEquals(Edition.MAX, edition);
    assertTrue(ModuleConfigManager.getInstance().isMax());
    assertFalse(ModuleConfigManager.getInstance().isPro());
  }

  // Parsing robustness: unknown/empty falls back to MAX
  @Test
  public void testParseEditionFallback() {
    assertEquals(Edition.MAX, Edition.fromString(null));
    assertEquals(Edition.MAX, Edition.fromString(""));
    assertEquals(Edition.MAX, Edition.fromString("garbage"));
    assertEquals(Edition.MAX, Edition.fromString("${edition}")); // unfiltered placeholder
    assertEquals(Edition.PRO, Edition.fromString("PRO"));
    assertEquals(Edition.PRO, Edition.fromString("pro"));
    assertEquals(Edition.MAX, Edition.fromString("MAX"));
  }
}
