/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.traft.paper.core;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LongMapTest {
  @Test
  public void randomOperationsMatchHashMap() {
    for (int seed = 0; seed < 20; seed++) {
      Random random = new Random(seed);
      LongMap<Long> map = new LongMap<>();
      Map<Long, Long> expected = new HashMap<>();
      // A narrow key range forces collisions, long probe chains and backward shifts.
      int range = 8 + random.nextInt(512);
      for (int op = 0; op < 20_000; op++) {
        long key = random.nextInt(range) * (random.nextBoolean() ? 1L : 1L << 32);
        switch (random.nextInt(4)) {
          case 0:
          case 1:
            long value = random.nextLong();
            assertEquals(expected.put(key, value), map.put(key, value));
            break;
          case 2:
            assertEquals(expected.remove(key), map.remove(key));
            break;
          default:
            assertEquals(expected.get(key), map.get(key));
            assertEquals(expected.containsKey(key), map.containsKey(key));
        }
        assertEquals(expected.size(), map.size());
      }
      assertTrue(map.sameAs(expected));
      assertEquals(expected.size(), map.keys().length);
      assertEquals(expected.size(), map.values().size());
      long cut = range / 2;
      map.removeKeysIf(key -> key % 3 == cut % 3);
      expected.keySet().removeIf(key -> key % 3 == cut % 3);
      assertTrue(map.sameAs(expected));
      map.clear();
      assertTrue(map.isEmpty());
      TreeMap<Long, Long> sorted = new TreeMap<>(expected);
      map.putAll(sorted);
      assertTrue(map.sameAs(sorted));
    }
  }
}
