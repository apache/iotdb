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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class TriggerInfoTest {

  private static TriggerInfo triggerInfo;
  private static TriggerInfo triggerInfoSaveBefore;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws IOException {
    triggerInfo = new TriggerInfo();
    triggerInfoSaveBefore = new TriggerInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    triggerInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException, IllegalPathException {
    TriggerInformation triggerInformation =
        new TriggerInformation(
            new PartialPath("root.test.**"),
            "test1",
            "test1.class",
            true,
            "test1.jar",
            null,
            TriggerEvent.AFTER_INSERT,
            TTriggerState.INACTIVE,
            false,
            null,
            FailureStrategy.OPTIMISTIC,
            "testMD5test");
    AddTriggerInTablePlan addTriggerInTablePlan =
        new AddTriggerInTablePlan(triggerInformation, new Binary(new byte[] {1, 2, 3}));
    triggerInfo.addTriggerInTable(addTriggerInTablePlan);
    triggerInfoSaveBefore.addTriggerInTable(addTriggerInTablePlan);

    Map<String, String> attributes = new HashMap<>();
    attributes.put("test-key", "test-value");
    triggerInformation =
        new TriggerInformation(
            new PartialPath("root.test.**"),
            "test2",
            "test2.class",
            true,
            "test2.jar",
            attributes,
            TriggerEvent.BEFORE_INSERT,
            TTriggerState.INACTIVE,
            false,
            new TDataNodeLocation(
                10000,
                new TEndPoint("127.0.0.1", 6600),
                new TEndPoint("127.0.0.1", 7700),
                new TEndPoint("127.0.0.1", 8800),
                new TEndPoint("127.0.0.1", 9900),
                new TEndPoint("127.0.0.1", 11000)),
            FailureStrategy.OPTIMISTIC,
            "testMD5test");
    addTriggerInTablePlan = new AddTriggerInTablePlan(triggerInformation, null);
    triggerInfo.addTriggerInTable(addTriggerInTablePlan);
    triggerInfoSaveBefore.addTriggerInTable(addTriggerInTablePlan);

    triggerInfo.processTakeSnapshot(snapshotDir);
    triggerInfo.clear();
    triggerInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(
        triggerInfoSaveBefore.getRawTriggerTable(), triggerInfo.getRawTriggerTable());
    Assert.assertEquals(
        triggerInfoSaveBefore.getRawExistedJarToMD5(), triggerInfo.getRawExistedJarToMD5());
  }
}
