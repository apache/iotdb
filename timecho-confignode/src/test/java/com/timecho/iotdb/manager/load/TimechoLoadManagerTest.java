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

package com.timecho.iotdb.manager.load;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.manager.load.cache.node.ActivationStatusCache;

import com.timecho.iotdb.commons.commission.obligation.ObligationStatus;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TimechoLoadManagerTest {

  @Test
  public void testRemoveOnlyUnregisteredActivationStatusCaches() {
    ActivationStatusCache configNodeCache =
        new ActivationStatusCache(System.nanoTime(), ObligationStatus.ACTIVE_ACTIVATED);
    ActivationStatusCache dataNodeCache =
        new ActivationStatusCache(System.nanoTime(), ObligationStatus.ACTIVATED);
    ActivationStatusCache aiNodeCache =
        new ActivationStatusCache(System.nanoTime(), ObligationStatus.ACTIVATED);
    ActivationStatusCache removedNodeCache =
        new ActivationStatusCache(System.nanoTime(), ObligationStatus.UNACTIVATED);

    Map<Integer, ActivationStatusCache> activationStatusCacheMap = new HashMap<>();
    activationStatusCacheMap.put(0, configNodeCache);
    activationStatusCacheMap.put(1, dataNodeCache);
    activationStatusCacheMap.put(2, aiNodeCache);
    activationStatusCacheMap.put(3, removedNodeCache);

    TDataNodeConfiguration dataNode =
        new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(1));
    TAINodeConfiguration aiNode =
        new TAINodeConfiguration().setLocation(new TAINodeLocation().setAiNodeId(2));

    TimechoLoadManager.removeUnregisteredActivationStatusCaches(
        activationStatusCacheMap,
        Collections.singleton(0),
        Collections.singleton(dataNode),
        Collections.singleton(aiNode));

    assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)), activationStatusCacheMap.keySet());
    assertSame(configNodeCache, activationStatusCacheMap.get(0));
    assertSame(dataNodeCache, activationStatusCacheMap.get(1));
    assertSame(aiNodeCache, activationStatusCacheMap.get(2));
  }
}
