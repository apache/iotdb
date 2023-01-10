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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class NodeInfoTest {

  private static NodeInfo nodeInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() {
    nodeInfo = new NodeInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    nodeInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException {
    registerConfigNodes();
    registerDataNodes();
    nodeInfo.processTakeSnapshot(snapshotDir);

    NodeInfo nodeInfo1 = new NodeInfo();
    nodeInfo1.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(nodeInfo, nodeInfo1);
  }

  private void registerConfigNodes() {
    for (int i = 0; i < 3; i++) {
      ApplyConfigNodePlan applyConfigNodePlan =
          new ApplyConfigNodePlan(
              new TConfigNodeLocation(
                  10000 + i,
                  new TEndPoint("127.0.0.1", 22200 + i),
                  new TEndPoint("127.0.0.1", 22300 + i)));
      nodeInfo.applyConfigNode(applyConfigNodePlan);
    }
  }

  private void registerDataNodes() {
    for (int i = 3; i < 6; i++) {
      RegisterDataNodePlan registerDataNodePlan =
          new RegisterDataNodePlan(generateTDataNodeConfiguration(i));
      nodeInfo.registerDataNode(registerDataNodePlan);
    }
  }

  private TDataNodeConfiguration generateTDataNodeConfiguration(int flag) {
    TDataNodeLocation location = generateTDataNodeLocation(flag);
    TNodeResource resource = new TNodeResource(16, 34359738368L);
    return new TDataNodeConfiguration(location, resource);
  }

  private TDataNodeLocation generateTDataNodeLocation(int flag) {
    return new TDataNodeLocation(
        10000 + flag,
        new TEndPoint("127.0.0.1", 6600 + flag),
        new TEndPoint("127.0.0.1", 7700 + flag),
        new TEndPoint("127.0.0.1", 8800 + flag),
        new TEndPoint("127.0.0.1", 9900 + flag),
        new TEndPoint("127.0.0.1", 11000 + flag));
  }
}
