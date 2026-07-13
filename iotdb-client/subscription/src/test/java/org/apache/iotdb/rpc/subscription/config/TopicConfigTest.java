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

package org.apache.iotdb.rpc.subscription.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopicConfigTest {

  @Test
  public void testColumnFilterKeyIsCaseInsensitive() {
    final TopicConfig topicConfig =
        new TopicConfig(Collections.singletonMap("Column-Filter", "column_name = \"s1\""));

    Assert.assertTrue(topicConfig.hasColumnFilter());
    Assert.assertEquals("column_name = \"s1\"", topicConfig.getColumnFilter());
    Assert.assertEquals(
        "column_name = \"s1\"",
        topicConfig.getAttributesWithSourceColumnFilter().get(TopicConstant.COLUMN_FILTER_KEY));
  }

  @Test
  public void testColumnFilterDefaultsToTrivialWhenAbsent() {
    final TopicConfig topicConfig = new TopicConfig(new HashMap<>());

    Assert.assertFalse(topicConfig.hasColumnFilter());
    Assert.assertTrue(topicConfig.isColumnFilterTrivial());
    Assert.assertEquals(TopicConstant.COLUMN_FILTER_DEFAULT_VALUE, topicConfig.getColumnFilter());
  }

  @Test
  public void testColumnFilterTrivialWithMixedCaseKeyAndValue() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("COLUMN-FILTER", " TRUE ");

    Assert.assertTrue(new TopicConfig(attributes).isColumnFilterTrivial());
  }
}
