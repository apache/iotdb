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

package org.apache.iotdb.metrics.core.utils;

import org.junit.Test;

import javax.management.ObjectName;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class IoTDBMetricObjNameFactoryTest {
  private final ObjectNameFactory factory = IoTDBMetricObjNameFactory.getInstance();

  @Test
  public void testUntaggedNamesStayCompatible() throws Exception {
    ObjectName expected = new ObjectName("org.apache.iotdb.metrics:name=plain,type=IoTDBAutoGauge");
    assertEquals(
        expected, factory.createName("IoTDBAutoGauge", "org.apache.iotdb.metrics", "plain"));
    assertEquals(
        expected,
        factory.createName(
            "IoTDBAutoGauge", "org.apache.iotdb.metrics", "plain", Collections.emptyMap()));
  }

  @Test
  public void testTagsCannotOverwriteMetricNameAndType() {
    ObjectName name =
        factory.createName(
            "IoTDBAutoGauge",
            "org.apache.iotdb.metrics",
            "client_manager",
            Map.of("name", "num_active", "type", "first", "tag.name", "nested"));
    assertEquals("client_manager", name.getKeyProperty("name"));
    assertEquals("IoTDBAutoGauge", name.getKeyProperty("type"));
    assertEquals("num_active", ObjectName.unquote(name.getKeyProperty("tag.name")));
    assertEquals("first", ObjectName.unquote(name.getKeyProperty("tag.type")));
    assertEquals("nested", ObjectName.unquote(name.getKeyProperty("tag.tag.name")));
    assertEquals(5, name.getKeyPropertyList().size());
  }

  @Test
  public void testTagOrderDoesNotChangeIdentity() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("name", "num_active");
    first.put("type", "pool");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("type", "pool");
    second.put("name", "num_active");
    ObjectName a =
        factory.createName("IoTDBAutoGauge", "org.apache.iotdb.metrics", "client_manager", first);
    ObjectName b =
        factory.createName("IoTDBAutoGauge", "org.apache.iotdb.metrics", "client_manager", second);
    assertEquals(a, b);
    assertEquals(a.getCanonicalName(), b.getCanonicalName());
    assertEquals(2, first.size());
    second.put("type", "other");
    assertNotEquals(
        a,
        factory.createName("IoTDBAutoGauge", "org.apache.iotdb.metrics", "client_manager", second));
  }

  @Test
  public void testEscapingIsLiteralAndCollisionFree() {
    String value = "value,=:*?\"\\\n";
    String[] keys = {"", "a:b", "a,b", "a=b", "a?b", "a*b", "a\nb", "a%b", "a.b", "a%003ab", "标签"};
    Set<ObjectName> names = new HashSet<>();
    Map<String, String> tags = new LinkedHashMap<>();
    for (String key : keys) {
      ObjectName name =
          factory.createName(
              "IoTDBAutoGauge", "org.apache.iotdb.metrics", "client_manager", Map.of(key, value));
      assertFalse(name.isPattern());
      names.add(name);
      tags.put(key, value);
    }
    assertEquals(keys.length, names.size());
    ObjectName all =
        factory.createName("IoTDBAutoGauge", "org.apache.iotdb.metrics", "client_manager", tags);
    assertEquals(keys.length + 2, all.getKeyPropertyList().size());
    all.getKeyPropertyList()
        .forEach(
            (key, actual) -> {
              if (key.startsWith("tag.")) {
                assertEquals(value, ObjectName.unquote(actual));
              }
            });
  }

  @Test
  public void testSpecialMetricNamesRetainType() {
    ObjectName name =
        factory.createName(
            "Gauge:*", "org.apache.iotdb.metrics", "metric,=:\"\n?", Map.of("type", "pool"));
    assertEquals("Gauge:*", ObjectName.unquote(name.getKeyProperty("type")));
    assertEquals("metric,=:\"\n?", ObjectName.unquote(name.getKeyProperty("name")));
    assertFalse(name.isPattern());
  }
}
