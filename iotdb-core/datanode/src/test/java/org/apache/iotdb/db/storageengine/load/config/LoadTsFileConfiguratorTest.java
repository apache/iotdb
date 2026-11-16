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

package org.apache.iotdb.db.storageengine.load.config;

import org.apache.iotdb.commons.exception.SemanticException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LoadTsFileConfiguratorTest {

  @Test
  public void testPreferLocalNodeIsDisabledByDefault() {
    Assert.assertFalse(
        LoadTsFileConfigurator.parseOrGetDefaultPreferLocalNode(Collections.emptyMap()));
    Assert.assertFalse(
        LoadTsFileConfigurator.parseOrGetDefaultPreferLocalNode(
            Collections.singletonMap("verify", "true")));
  }

  @Test
  public void testPreferLocalNodeAttributeIsParsed() {
    for (final boolean requested : new boolean[] {false, true, false}) {
      final Map<String, String> attributes = new HashMap<>();
      attributes.put(LoadTsFileConfigurator.PREFER_LOCAL_NODE_KEY, String.valueOf(requested));
      Assert.assertEquals(
          requested, LoadTsFileConfigurator.parseOrGetDefaultPreferLocalNode(attributes));
    }
  }

  @Test
  public void testInvalidPreferLocalNodeValueIsRejected() {
    for (final String value : new String[] {"yes", "no", "1", "0", "", "true "}) {
      try {
        LoadTsFileConfigurator.validatePreferLocalNodeParam(value);
        Assert.fail("Expected SemanticException for " + value);
      } catch (final SemanticException e) {
        // expected
      }
    }
    LoadTsFileConfigurator.validatePreferLocalNodeParam("true");
    LoadTsFileConfigurator.validatePreferLocalNodeParam("FALSE");
  }
}
