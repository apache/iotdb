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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableDeviceSchemaValidatorTest {

  @Test
  public void testNullAttributeNeedsUpdate() {
    final Map<String, Binary> attributeMap = new HashMap<>();
    attributeMap.put("attr", new Binary("x", StandardCharsets.UTF_8));

    Assert.assertTrue(
        TableDeviceSchemaValidator.isAttributeUpdateRequired(
            Collections.singletonList("attr"), new Object[] {null}, attributeMap));
  }

  @Test
  public void testMissingTailAttributeValueEqualsNull() {
    final Map<String, Binary> attributeMap = new HashMap<>();
    attributeMap.put("attr1", new Binary("x", StandardCharsets.UTF_8));

    Assert.assertFalse(
        TableDeviceSchemaValidator.isAttributeUpdateRequired(
            Arrays.asList("attr1", "attr2"),
            new Object[] {new Binary("x", StandardCharsets.UTF_8)},
            attributeMap));
  }
}
