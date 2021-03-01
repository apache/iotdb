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

import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

  @Test
  public void testIn() {
    String value1 = "device1.s1";
    String value2 = "device1.s2";
    String value3 = "device1.s3";
    BloomFilter filter = BloomFilter.getEmptyBloomFilter(0.05, 3);
    filter.add(value1);
    filter.add(value2);
    filter.add(value3);

    assertTrue(filter.contains(value1));
    assertTrue(filter.contains(value2));
    assertTrue(filter.contains(value3));
    System.out.println(filter.contains("12iuedyauydua"));
    System.out.println(filter.contains("device_1.s1"));
    System.out.println(filter.contains("device1.s_2"));
    System.out.println(filter.contains("device2.s1"));
    System.out.println(filter.contains("device3.s2"));
    System.out.println(filter.contains("device4.s2"));
    System.out.println(filter.contains("device1.s4"));
  }

  @Test
  public void testSerialize() {
    String value1 = "device1.s1";
    String value2 = "device1.s2";
    String value3 = "device1.s3";
    BloomFilter filter = BloomFilter.getEmptyBloomFilter(0.05, 3);
    filter.add(value1);
    filter.add(value2);
    filter.add(value3);

    BloomFilter filter1 =
        BloomFilter.buildBloomFilter(
            filter.serialize(), filter.getSize(), filter.getHashFunctionSize());
    assertTrue(filter1.contains(value1));
    assertTrue(filter1.contains(value2));
    assertTrue(filter1.contains(value3));
  }
}
