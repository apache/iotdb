/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.schema.source.TimeSeriesSchemaSource.mapToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TimeSeriesSchemaSourceMapToStringTest {

  /** The pre-escaping implementation, kept here to prove backward compatibility. */
  private static String legacyMapToString(final Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return null;
    }
    final String content =
        map.entrySet().stream()
            .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
            .collect(Collectors.joining(","));
    return "{" + content + "}";
  }

  private static Map<String, String> mapOf(final String key, final String value) {
    final Map<String, String> map = new LinkedHashMap<>();
    map.put(key, value);
    return map;
  }

  private static JsonObject parse(final String json) {
    final JsonElement element = JsonParser.parseString(json);
    assertEquals("not a JSON object: " + json, true, element.isJsonObject());
    return element.getAsJsonObject();
  }

  /** Renders, parses with a standard JSON parser, and checks the round-trip to the source map. */
  private static void assertJsonRoundTrip(final Map<String, String> map) {
    final JsonObject json = parse(mapToString(map));
    assertEquals(map.size(), json.size());
    for (final Map.Entry<String, String> entry : map.entrySet()) {
      assertEquals(true, json.has(entry.getKey()));
      assertEquals(entry.getValue(), json.get(entry.getKey()).getAsString());
    }
  }

  @Test
  public void testPlainValue() {
    final Map<String, String> map = mapOf("room", "101");
    assertEquals("{\"room\":\"101\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testCommaIsNotEscaped() {
    final Map<String, String> map = mapOf("k", "a,b");
    assertEquals("{\"k\":\"a,b\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testDoubleQuote() {
    final Map<String, String> map = mapOf("k", "a\"b");
    assertEquals("{\"k\":\"a\\\"b\"}", mapToString(map));
    assertJsonRoundTrip(map);
    // the escaped quote must not split the document into extra keys
    assertEquals(1, parse(mapToString(map)).size());
  }

  @Test
  public void testBackslash() {
    final Map<String, String> map = mapOf("k", "a\\b");
    assertEquals("{\"k\":\"a\\\\b\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testLiteralBackslashAndN() {
    final Map<String, String> map = mapOf("k", "a\\nb");
    assertEquals("{\"k\":\"a\\\\nb\"}", mapToString(map));
    assertJsonRoundTrip(map);
    assertEquals("a\\nb", parse(mapToString(map)).get("k").getAsString());
  }

  @Test
  public void testDoubleBackslash() {
    final Map<String, String> map = mapOf("k", "a\\\\b");
    assertEquals("{\"k\":\"a\\\\\\\\b\"}", mapToString(map));
    assertJsonRoundTrip(map);
    assertEquals("a\\\\b", parse(mapToString(map)).get("k").getAsString());
  }

  @Test
  public void testTab() {
    final Map<String, String> map = mapOf("k", "\t");
    assertEquals("{\"k\":\"\\t\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testLineFeed() {
    final Map<String, String> map = mapOf("k", "a\nb");
    assertEquals("{\"k\":\"a\\nb\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testCarriageReturnAndLineFeed() {
    final Map<String, String> map = mapOf("k", "a\r\nb");
    assertEquals("{\"k\":\"a\\r\\nb\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testBackspaceAndFormFeed() {
    final Map<String, String> map = mapOf("k", "a\bb\fc");
    assertEquals("{\"k\":\"a\\bb\\fc\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testOtherControlCharacterUsesUnicodeEscape() {
    final Map<String, String> map = mapOf("k", "a\u0001b");
    assertEquals("{\"k\":\"a\\u0001b\"}", mapToString(map));
    assertJsonRoundTrip(map);
    assertEquals("a\u0001b", parse(mapToString(map)).get("k").getAsString());
  }

  @Test
  public void testNonAsciiIsKeptRaw() {
    final Map<String, String> map = mapOf("cn", "中文\uD83D\uDE00");
    assertEquals("{\"cn\":\"中文\uD83D\uDE00\"}", mapToString(map));
    assertJsonRoundTrip(map);
    // no \\uXXXX escaping of non-ASCII characters
    assertEquals(false, mapToString(map).contains("\\u"));
  }

  @Test
  public void testEmptyStringValue() {
    final Map<String, String> map = mapOf("k", "");
    assertEquals("{\"k\":\"\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testEscapingInKey() {
    final Map<String, String> map = mapOf("a\"b\\c\td", "v");
    assertEquals("{\"a\\\"b\\\\c\\td\":\"v\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testMultipleKeys() {
    final Map<String, String> map = new LinkedHashMap<>();
    map.put("root.sg", "101");
    map.put("unit", "celsius");
    map.put("note", "a\"b\\c");
    assertEquals(
        "{\"root.sg\":\"101\",\"unit\":\"celsius\",\"note\":\"a\\\"b\\\\c\"}", mapToString(map));
    assertJsonRoundTrip(map);
  }

  @Test
  public void testNullAndEmptyMap() {
    assertNull(mapToString(null));
    assertNull(mapToString(Collections.emptyMap()));
  }

  @Test
  public void testIdenticalToLegacyOutputWhenNothingNeedsEscaping() {
    final Map<String, String> plain = new LinkedHashMap<>();
    plain.put("room", "101");
    plain.put("sensor", "temperature");
    plain.put("path", "root.sg.d1.s1");
    plain.put("empty", "");
    plain.put("punctuation", "a,b;c=d (e) [f] {g} /h@i#j$k%l^m&n*o-p+q_r");
    plain.put("cjk", "中文😀");
    plain.put("single", "x");

    assertEquals(legacyMapToString(plain), mapToString(plain));
    assertJsonRoundTrip(plain);

    final Map<String, String> single = mapOf("room", "101");
    assertEquals(legacyMapToString(single), mapToString(single));
  }

  @Test
  public void testLegacyComparisonForMultiplePlainMaps() {
    final Map<String, String> map = new LinkedHashMap<>();
    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");
    assertEquals(legacyMapToString(map), mapToString(map));
    assertEquals("{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}", mapToString(map));
  }
}
