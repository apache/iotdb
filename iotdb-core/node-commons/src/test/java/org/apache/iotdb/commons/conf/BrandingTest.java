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
import static org.junit.Assert.assertTrue;

public class BrandingTest {

  // Default TIMECHODB edition -> TimechoDB
  @Test
  public void brandNameDefaultsToTimechoDb() {
    assertEquals("TimechoDB", IoTDBConstant.BRAND_NAME);
  }

  // LOGO lines must fit an 80-column terminal
  @Test
  public void logoLinesWithin80Columns() {
    for (String line : IoTDBConstant.LOGO.split("\n")) {
      assertTrue("logo line too wide: <" + line + ">", line.length() <= 80);
    }
  }

  // The "Enterprise" qualifier was removed from the banner (it now reads "version X", not
  // "Enterprise version X"). Guard against accidental reintroduction in either edition's art.
  @Test
  public void logoDoesNotContainEnterprise() {
    org.junit.Assert.assertFalse(
        "LOGO must not contain 'Enterprise': <" + IoTDBConstant.LOGO + ">",
        IoTDBConstant.LOGO.contains("Enterprise"));
  }

  // Protocol identifiers must stay "IoTDB" (red line)
  @Test
  public void protocolIdentifiersUnchanged() {
    assertEquals("IoTDB", IoTDBConstant.GLOBAL_DB_NAME);
  }
}
