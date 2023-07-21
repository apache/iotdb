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

package org.apache.iotdb.commons.structure;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class BalanceTreeMapTest {

  @Test
  public void testGetKeyWithMinValue() {
    Random random = new Random();
    BalanceTreeMap<TSeriesPartitionSlot, Integer> balanceTreeMap = new BalanceTreeMap<>();
    for (int i = 0; i < 100; i++) {
      balanceTreeMap.put(new TSeriesPartitionSlot(i), random.nextInt(Integer.MAX_VALUE));
    }
    TSeriesPartitionSlot minSlot = new TSeriesPartitionSlot(100);
    balanceTreeMap.put(minSlot, Integer.MIN_VALUE);
    for (int i = 101; i < 200; i++) {
      balanceTreeMap.put(new TSeriesPartitionSlot(i), random.nextInt(Integer.MAX_VALUE));
    }
    Assert.assertEquals(minSlot, balanceTreeMap.getKeyWithMinValue());

    int currentValue = Integer.MIN_VALUE;
    for (int i = 0; i < 200; i++) {
      TSeriesPartitionSlot slot = balanceTreeMap.getKeyWithMinValue();
      Assert.assertTrue(balanceTreeMap.get(slot) >= currentValue);
      currentValue = balanceTreeMap.get(slot);
      balanceTreeMap.remove(slot);
    }
  }

  @Test
  public void testKeysDuplicate() {
    BalanceTreeMap<TSeriesPartitionSlot, Integer> balanceTreeMap = new BalanceTreeMap<>();
    Set<TSeriesPartitionSlot> duplicateSet0 = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      TSeriesPartitionSlot slot = new TSeriesPartitionSlot(i);
      balanceTreeMap.put(slot, 0);
      duplicateSet0.add(slot);
    }
    Set<TSeriesPartitionSlot> duplicateSet1 = new HashSet<>();
    for (int i = 10; i < 20; i++) {
      TSeriesPartitionSlot slot = new TSeriesPartitionSlot(i);
      balanceTreeMap.put(slot, 1);
      duplicateSet1.add(slot);
    }

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(duplicateSet0.contains(balanceTreeMap.getKeyWithMinValue()));
      balanceTreeMap.remove(balanceTreeMap.getKeyWithMinValue());
    }
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(duplicateSet1.contains(balanceTreeMap.getKeyWithMinValue()));
      balanceTreeMap.remove(balanceTreeMap.getKeyWithMinValue());
    }
    Assert.assertTrue(balanceTreeMap.isEmpty());
  }
}
