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

package org.apache.iotdb.confignode.consensus.request.read.partition;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GetOrCreateDataPartitionPlanTest {

  @Test
  public void testPreferredDataNodeIdIsConverted() {
    // A request from a client that does not prefer a DataNode keeps the "no preference" value
    final TDataPartitionReq req = new TDataPartitionReq(samplePartitionSlotsMap());
    Assert.assertFalse(req.isSetPreferredDataNodeId());
    final GetOrCreateDataPartitionPlan planWithoutPreference =
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(req);
    Assert.assertEquals(-1, planWithoutPreference.getPreferredDataNodeId());

    // A request from a LOAD that prefers its local DataNode carries the hint into the plan
    req.setPreferredDataNodeId(7);
    final GetOrCreateDataPartitionPlan planWithPreference =
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(req);
    Assert.assertEquals(7, planWithPreference.getPreferredDataNodeId());

    // The hint is part of the plan identity, so plans that allocate differently are not equal
    Assert.assertNotEquals(planWithoutPreference, planWithPreference);
    Assert.assertEquals(
        planWithPreference,
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(
            new TDataPartitionReq(samplePartitionSlotsMap()).setPreferredDataNodeId(7)));
    Assert.assertEquals(
        planWithPreference.hashCode(),
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(
                new TDataPartitionReq(samplePartitionSlotsMap()).setPreferredDataNodeId(7))
            .hashCode());
  }

  private static Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> samplePartitionSlotsMap() {
    final Map<TSeriesPartitionSlot, TTimeSlotList> seriesSlotsMap = new HashMap<>();
    seriesSlotsMap.put(new TSeriesPartitionSlot(0), new TTimeSlotList());
    final Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    partitionSlotsMap.put("db1", seriesSlotsMap);
    return partitionSlotsMap;
  }
}
