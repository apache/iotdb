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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.read.ttl.ShowTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.response.ttl.ShowTTLResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.read.common.parser.PathNodesGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;
import static org.junit.Assert.assertEquals;

public class TTLInfoTest {

  private TTLInfo ttlInfo;
  private final File snapshotDir = new File(BASE_OUTPUT_PATH, "ttlInfo-snapshot");
  private final long ttl = 123435565323L;
  private long[] originTTLArr;

  @Before
  public void setup() throws IOException {
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
    originTTLArr = CommonDescriptor.getInstance().getConfig().getTierTTLInMs();
    long[] ttlArr = new long[2];
    ttlArr[0] = 10000000L;
    ttlArr[1] = ttl;
    CommonDescriptor.getInstance().getConfig().setTierTTLInMs(ttlArr);
    ttlInfo = new TTLInfo();
  }

  @After
  public void tearDown() throws IOException {
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
    CommonDescriptor.getInstance().getConfig().setTierTTLInMs(originTTLArr);
  }

  @Test
  public void testSetAndUnsetTTL() throws IllegalPathException {
    ShowTTLResp resp = ttlInfo.showTTL(new ShowTTLPlan());
    Map<String, Long> ttlMap = resp.getPathTTLMap();

    Map<String, Long> resultMap = new HashMap<>();
    resultMap.put("root.**", Long.MAX_VALUE);
    Assert.assertEquals(resultMap, ttlMap);
    Assert.assertEquals(1, ttlInfo.getTTLCount());

    // set ttl
    PartialPath path = new PartialPath("root.test.db1.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 121322323L));
    resultMap.put(path.getFullPath(), 121322323L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(2, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test.db1.group1.group1.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 2222L));
    resultMap.put(path.getFullPath(), 2222L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(3, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 1));
    resultMap.put(path.getFullPath(), 1L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(4, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 599722L));
    resultMap.put(path.getFullPath(), 599722L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(5, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 9999999L));
    resultMap.put(path.getFullPath(), 9999999L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(6, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.d2.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 888888L));
    resultMap.put(path.getFullPath(), 888888L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(7, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.d2");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 9898989898L));
    resultMap.put(path.getFullPath(), 9898989898L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(8, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 11111222L));
    resultMap.put(path.getFullPath(), 11111222L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(8, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.test.db1.group1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), Long.MAX_VALUE));
    resultMap.put(path.getFullPath(), Long.MAX_VALUE);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(9, ttlInfo.getTTLCount());

    // set ttl
    path = new PartialPath("root.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 222222L));
    resultMap.put(path.getFullPath(), 222222L);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(9, ttlInfo.getTTLCount());

    // set ttl, not support negative ttl
    path = new PartialPath("root.test.db1.group1.group2");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(9, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.test.db1.group1.group2");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.remove(path.getFullPath());
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(9, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.test.db1.group1");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.remove(path.getFullPath());
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(8, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.**");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.put(path.getFullPath(), Long.MAX_VALUE);
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(8, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.d2");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.remove(path.getFullPath());
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(7, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.test.db1.**");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.remove(path.getFullPath());
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(6, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.remove(path.getFullPath());
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(5, ttlInfo.getTTLCount());

    // unset ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.d2.**");
    ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));
    resultMap.remove(path.getFullPath());
    Assert.assertEquals(resultMap, ttlInfo.showTTL(new ShowTTLPlan()).getPathTTLMap());
    Assert.assertEquals(4, ttlInfo.getTTLCount());
  }

  @Test
  public void testUnsetNonExistTTL() throws IllegalPathException {
    assertEquals(
        TSStatusCode.ILLEGAL_PATH.getStatusCode(),
        ttlInfo.unsetTTL(new SetTTLPlan(-1, "root")).getCode());
    assertEquals(
        TSStatusCode.PATH_NOT_EXIST.getStatusCode(),
        ttlInfo.unsetTTL(new SetTTLPlan(-1, "root", "sg100", "f10", "d1")).getCode());

    PartialPath path = new PartialPath("root.test.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 11111222L));
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 11111222L)).getCode());
    assertEquals(
        TSStatusCode.PATH_NOT_EXIST.getStatusCode(),
        ttlInfo.unsetTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 11111222L)).getCode());
  }

  @Test
  public void testTooManyTTL() {
    final int tTlRuleCapacity = CommonDescriptor.getInstance().getConfig().getTTlRuleCapacity();
    for (int i = 0; i < tTlRuleCapacity - 1; i++) {
      SetTTLPlan setTTLPlan =
          new SetTTLPlan(PathNodesGenerator.splitPathToNodes("root.sg1.d" + i + ".**"), 1000);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), ttlInfo.setTTL(setTTLPlan).code);
    }
    SetTTLPlan setTTLPlan =
        new SetTTLPlan(
            PathNodesGenerator.splitPathToNodes("root.sg1.d" + tTlRuleCapacity + ".**"), 1000);
    final TSStatus status = ttlInfo.setTTL(setTTLPlan);
    assertEquals(TSStatusCode.OVERSIZE_TTL.getStatusCode(), status.code);
    assertEquals(
        "The number of TTL rules has reached the limit (1000). Please delete some existing rules first.",
        status.message);
  }

  @Test
  public void testSnapshot() throws TException, IOException, IllegalPathException {
    // set ttl
    PartialPath path = new PartialPath("root.test.db1.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 121322323L));

    // set ttl
    path = new PartialPath("root.test.db1.group1.group1.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 2222L));

    // set ttl
    path = new PartialPath("root.test.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 1));

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 599722L));

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 9999999L));

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.d2.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 888888L));

    // set ttl
    path = new PartialPath("root.test1.db1.group1.group2.d1.d2");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 9898989898L));

    // set ttl
    path = new PartialPath("root.test.db1.group1.group2.d1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 11111222L));

    // set ttl
    path = new PartialPath("root.test.db1.group1");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), Long.MAX_VALUE));

    // set ttl
    path = new PartialPath("root.**");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), 222222L));

    // set ttl, not support negative ttl
    path = new PartialPath("root.test.db1.group1.group2");
    ttlInfo.setTTL(new SetTTLPlan(Arrays.asList(path.getNodes()), -1));

    ttlInfo.processTakeSnapshot(snapshotDir);
    TTLInfo actualTTLInfo = new TTLInfo();
    actualTTLInfo.processLoadSnapshot(snapshotDir);
    Assert.assertTrue(ttlInfo.equals(actualTTLInfo));
  }
}
