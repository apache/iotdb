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
package org.apache.iotdb.tsfile.utils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PairTest {

  @Test
  public void testEqualsObject() {
    Pair<String, Integer> p1 = new Pair<String, Integer>("a", 123123);
    Pair<String, Integer> p2 = new Pair<String, Integer>("a", 123123);
    assertTrue(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", null);
    p2 = new Pair<String, Integer>("a", 123123);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", 123123);
    p2 = new Pair<String, Integer>("a", null);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>(null, 123123);
    p2 = new Pair<String, Integer>("a", 123123);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", 123123);
    p2 = new Pair<String, Integer>(null, 123123);
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>(null, 123123);
    p2 = null;
    assertFalse(p1.equals(p2));
    p1 = new Pair<String, Integer>(null, 123123);
    p2 = new Pair<String, Integer>(null, 123123);
    Map<Pair<String, Integer>, Integer> map = new HashMap<Pair<String, Integer>, Integer>();
    map.put(p1, 1);
    assertTrue(map.containsKey(p2));
    assertTrue(p1.equals(p2));
    p1 = new Pair<String, Integer>("a", null);
    p2 = new Pair<String, Integer>("a", null);
    assertTrue(p1.equals(p2));
    assertTrue(p1.equals(p1));
    assertFalse(p1.equals(new Integer(1)));
  }

  @Test
  public void testToString() {
    Pair<String, Integer> p1 = new Pair<String, Integer>("a", 123123);
    assertEquals("<a,123123>", p1.toString());
    Pair<Float, Double> p2 = new Pair<Float, Double>(32.5f, 123.123d);
    assertEquals("<32.5,123.123>", p2.toString());
  }
}
