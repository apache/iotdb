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

package org.apache.iotdb.db.metadata.cache.dualkeycache;

import org.apache.iotdb.db.metadata.cache.dualkeycache.impl.DualKeyCacheBuilder;
import org.apache.iotdb.db.metadata.cache.dualkeycache.impl.DualKeyCachePolicy;

import org.junit.Assert;
import org.junit.Test;

public class DualKeyCacheTest {

  @Test
  public void testBasicReadPut() {
    DualKeyCacheBuilder<String, String, String> dualKeyCacheBuilder = new DualKeyCacheBuilder<>();
    IDualKeyCache<String, String, String> dualKeyCache =
        dualKeyCacheBuilder
            .cacheEvictionPolicy(DualKeyCachePolicy.LRU)
            .memoryCapacity(230)
            .firstKeySizeComputer(this::computeStringSize)
            .secondKeySizeComputer(this::computeStringSize)
            .valueSizeComputer(this::computeStringSize)
            .build();

    String[] firstKeyList = new String[] {"root.db.d1", "root.db.d2"};
    String[] secondKeyList = new String[] {"s1", "s2"};
    String[][] valueTable =
        new String[][] {new String[] {"1-1", "1-2"}, new String[] {"2-1", "2-2"}};

    for (int i = 0; i < firstKeyList.length; i++) {
      for (int j = 0; j < secondKeyList.length; j++) {
        dualKeyCache.put(firstKeyList[i], secondKeyList[j], valueTable[i][j]);
      }
    }

    for (int i = 0; i < firstKeyList.length; i++) {
      for (int j = 0; j < secondKeyList.length; j++) {
        int finalI = i;
        int finalJ = j;
        dualKeyCache.compute(
            new IDualKeyCacheComputation<String, String, String>() {
              @Override
              public String getFirstKey() {
                return firstKeyList[finalI];
              }

              @Override
              public String[] getSecondKeyList() {
                return new String[] {secondKeyList[finalJ]};
              }

              @Override
              public void computeValue(int index, String value) {
                Assert.assertEquals(0, index);
                if (value != null) {
                  Assert.assertEquals(valueTable[finalI][finalJ], value);
                }
              }
            });
      }
    }

    Assert.assertEquals(230, dualKeyCache.stats().memoryUsage());
    Assert.assertEquals(4, dualKeyCache.stats().requestCount());
    Assert.assertEquals(3, dualKeyCache.stats().hitCount());
  }

  private int computeStringSize(String string) {
    return 8 + 8 + 4 + 2 * string.length();
  }
}
