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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota;

import org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange;
import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot;
import org.apache.iotdb.confignode.rpc.thrift.TUserResourceQuotaResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ShowUserResourceQuotaTaskTest {

  @Test
  public void testBuildTsBlockExpandsPerAliveDataNode() throws Exception {
    TUserResourceQuota quota = new TUserResourceQuota();
    Map<TResourceType, TResourceQuotaRange> read = new HashMap<>();
    read.put(TResourceType.CPU, new TResourceQuotaRange(1, 4));
    quota.setReadQuota(read);

    TUserResourceQuotaResp resp = new TUserResourceQuotaResp();
    Map<String, TUserResourceQuota> quotas = new HashMap<>();
    quotas.put("u1", quota);
    resp.setUserResourceQuota(quotas);

    Map<Integer, TUserResourceUsageSnapshot> usageByNode = new HashMap<>();
    TUserResourceUsageSnapshot snap1 = new TUserResourceUsageSnapshot();
    Map<String, Map<TResourceType, Long>> readInUse1 = new HashMap<>();
    Map<TResourceType, Long> u1Cpu = new HashMap<>();
    u1Cpu.put(TResourceType.CPU, 2L);
    readInUse1.put("u1", u1Cpu);
    snap1.setReadInUse(readInUse1);
    usageByNode.put(1, snap1);
    // Running DN without usage report yet: empty snapshot still expands a NodeID row set.
    usageByNode.put(2, new TUserResourceUsageSnapshot());
    resp.setUsageByDataNode(usageByNode);

    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowUserResourceQuotaTask.buildTSBlock(resp, future);
    ConfigTaskResult result = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS, result.getStatusCode());
    TsBlock block = result.getResultSet();
    Assert.assertNotNull(block);

    Set<String> nodeIds = new HashSet<>();
    int usedForNode1 = -1;
    int usedForNode2 = -1;
    for (int i = 0; i < block.getPositionCount(); i++) {
      String nodeId = block.getColumn(1).getBinary(i).toString();
      String type = block.getColumn(3).getBinary(i).toString();
      if ("cpu".equals(type)) {
        nodeIds.add(nodeId);
        int used = Integer.parseInt(block.getColumn(6).getBinary(i).toString());
        if ("1".equals(nodeId)) {
          usedForNode1 = used;
        } else if ("2".equals(nodeId)) {
          usedForNode2 = used;
        }
      }
    }
    Assert.assertTrue(nodeIds.contains("1"));
    Assert.assertTrue(nodeIds.contains("2"));
    Assert.assertEquals(2, usedForNode1);
    Assert.assertEquals(0, usedForNode2);
  }
}
