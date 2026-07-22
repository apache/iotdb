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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Constants;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeviceAttributeStoreTest {

  @Test
  public void testCreateAttributeIgnoresNamesWithoutValues() {
    final DeviceAttributeStore store = new DeviceAttributeStore(null);
    final Binary expectedValue = new Binary("v1", StandardCharsets.UTF_8);

    final int pointer =
        store.createAttribute(
            Arrays.asList("attr1", "attr2"), new Object[] {expectedValue}, "table1");

    final Map<String, Binary> attributes = store.getAttributes(pointer);
    assertEquals(1, attributes.size());
    assertEquals(expectedValue, attributes.get("attr1"));
    assertFalse(attributes.containsKey("attr2"));
  }

  @Test
  public void testCreateAttributeSkipsNoneValue() {
    final DeviceAttributeStore store = new DeviceAttributeStore(null);
    final Binary expectedValue = new Binary("v2", StandardCharsets.UTF_8);

    final int pointer =
        store.createAttribute(
            Arrays.asList("attr1", "attr2"),
            new Object[] {Constants.NONE, expectedValue},
            "table1");

    final Map<String, Binary> attributes = store.getAttributes(pointer);
    assertEquals(1, attributes.size());
    assertFalse(attributes.containsKey("attr1"));
    assertTrue(attributes.containsKey("attr2"));
    assertEquals(expectedValue, attributes.get("attr2"));
  }
}
