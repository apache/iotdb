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

import org.apache.iotdb.confignode.consensus.request.write.OperateReceiverPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TOperateReceiverPipeReq;
import org.apache.iotdb.db.sync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.service.transport.thrift.RequestType;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class ClusterReceiverInfoTest {

  private static ClusterReceiverInfo clusterReceiverInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() {
    clusterReceiverInfo = new ClusterReceiverInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    clusterReceiverInfo.close();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws IOException {
    String pipeName1 = "p1";
    String remoteIp1 = "192.168.11.11";
    long createTime1 = System.currentTimeMillis();
    String pipeName2 = "p2";
    String remoteIp2 = "192.168.22.22";
    long createTime2 = System.currentTimeMillis() + 1;

    TOperateReceiverPipeReq req1 =
        new TOperateReceiverPipeReq(RequestType.CREATE, pipeName1, remoteIp1, createTime1);
    TOperateReceiverPipeReq req2 =
        new TOperateReceiverPipeReq(RequestType.CREATE, pipeName2, remoteIp2, createTime2);
    TOperateReceiverPipeReq req3 =
        new TOperateReceiverPipeReq(RequestType.START, pipeName1, remoteIp1, createTime1);
    TOperateReceiverPipeReq req4 =
        new TOperateReceiverPipeReq(RequestType.DROP, pipeName2, remoteIp2, createTime2);
    clusterReceiverInfo.operatePipe(new OperateReceiverPipeReq(req1));
    clusterReceiverInfo.operatePipe(new OperateReceiverPipeReq(req2));
    clusterReceiverInfo.operatePipe(new OperateReceiverPipeReq(req3));
    clusterReceiverInfo.operatePipe(new OperateReceiverPipeReq(req4));
    clusterReceiverInfo.processTakeSnapshot(snapshotDir);
    clusterReceiverInfo.clear();
    clusterReceiverInfo.processLoadSnapshot(snapshotDir);
    PipeInfo pipeInfo1 = clusterReceiverInfo.getPipeInfo(pipeName1, remoteIp1, createTime1);
    Assert.assertEquals(pipeInfo1.getPipeName(), pipeName1);
    Assert.assertEquals(pipeInfo1.getRemoteIp(), remoteIp1);
    Assert.assertEquals(pipeInfo1.getCreateTime(), createTime1);
    Assert.assertEquals(pipeInfo1.getStatus(), Pipe.PipeStatus.RUNNING);
    PipeInfo pipeInfo2 = clusterReceiverInfo.getPipeInfo(pipeName2, remoteIp2, createTime2);
    Assert.assertEquals(pipeInfo2.getPipeName(), pipeName2);
    Assert.assertEquals(pipeInfo2.getRemoteIp(), remoteIp2);
    Assert.assertEquals(pipeInfo2.getCreateTime(), createTime2);
    Assert.assertEquals(pipeInfo2.getStatus(), Pipe.PipeStatus.DROP);
  }
}
