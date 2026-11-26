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

import org.apache.iotdb.confignode.consensus.request.read.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.consensus.response.cq.ShowCQResp;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class CQInfoTest {

  private static CQInfo cqInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws IOException {
    cqInfo = new CQInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException {
    long executionTime = System.currentTimeMillis();
    AddCQPlan addCQPlan =
        new AddCQPlan(
            new TCreateCQReq(
                "testCq1",
                1000,
                0,
                1000,
                0,
                (byte) 0,
                "select s1 into root.backup.d1.s1 from root.sg.d1",
                "create cq testCq1 BEGIN select s1 into root.backup.d1.s1 from root.sg.d1 END",
                "Asia",
                "root"),
            "testCq1Token",
            executionTime);

    cqInfo.addCQ(addCQPlan);

    executionTime = System.currentTimeMillis();
    addCQPlan =
        new AddCQPlan(
            new TCreateCQReq(
                "testCq2",
                1000,
                0,
                1000,
                0,
                (byte) 1,
                "select s1 into root.backup.d2.s1 from root.sg.d2",
                "create cq testCq2 BEGIN select s1 into root.backup.d2.s1 from root.sg.d2 END",
                "Asia",
                "root"),
            "testCq2Token",
            executionTime);
    cqInfo.addCQ(addCQPlan);

    cqInfo.processTakeSnapshot(snapshotDir);
    CQInfo actualCQInfo = new CQInfo();
    actualCQInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(cqInfo, actualCQInfo);
  }

  @Test
  public void testOldCallbackCannotTouchRecreatedCQ() throws Exception {
    long executionTime = System.currentTimeMillis();
    TCreateCQReq req =
        new TCreateCQReq(
            "testCq3",
            1000,
            0,
            1000,
            0,
            (byte) 0,
            "select s1 into root.backup.d3.s1 from root.sg.d3",
            "create cq testCq3 BEGIN select s1 into root.backup.d3.s1 from root.sg.d3 END",
            "Asia",
            "root");

    cqInfo.addCQ(new AddCQPlan(req, "oldToken", executionTime));
    cqInfo.dropCQ(new DropCQPlan("testCq3"));
    cqInfo.addCQ(new AddCQPlan(req, "newToken", executionTime));

    Assert.assertEquals(
        TSStatusCode.NO_SUCH_CQ.getStatusCode(),
        cqInfo.updateCQLastExecutionTime(
                new UpdateCQLastExecTimePlan("testCq3", executionTime + 1000, "oldToken"))
            .code);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        cqInfo.updateCQLastExecutionTime(
                new UpdateCQLastExecTimePlan("testCq3", executionTime + 1000, "newToken"))
            .code);
  }

  @Test
  public void testShowCQCanFilterByCQId() throws Exception {
    long executionTime = System.currentTimeMillis();
    TCreateCQReq req =
        new TCreateCQReq(
            "testCq4",
            1000,
            0,
            1000,
            0,
            (byte) 0,
            "select s1 into root.backup.d4.s1 from root.sg.d4",
            "create cq testCq4 BEGIN select s1 into root.backup.d4.s1 from root.sg.d4 END",
            "Asia",
            "root");
    cqInfo.addCQ(new AddCQPlan(req, "testCq4Token", executionTime));

    ShowCQResp showCQResp = cqInfo.showCQ(new ShowCQPlan("testCq4"));

    Assert.assertEquals(1, showCQResp.getCqList().size());
    Assert.assertEquals("testCq4", showCQResp.getCqList().get(0).getCqId());
  }
}
