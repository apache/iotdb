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

package org.apache.iotdb.db.trigger.executor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TriggerExecuteTest {
  @After
  public void clear() {
    TriggerManagementService.getInstance().getTriggerTable().clear();
  }

  @Test
  public void testTriggerExecutor() {
    try {
      TriggerInformation triggerInformation =
          new TriggerInformation(
              new PartialPath("root.sg.**"),
              "test1",
              "org.apache.iotdb.db.trigger.service.TestTrigger",
              false,
              "test1.jar",
              null,
              TriggerEvent.AFTER_INSERT,
              TTriggerState.ACTIVE,
              false,
              null,
              FailureStrategy.OPTIMISTIC,
              "testMD5test");
      Trigger trigger = new TestTrigger();
      TriggerExecutor executor = new TriggerExecutor(triggerInformation, trigger, false);
      TriggerManagementService.getInstance().fakeRegister(triggerInformation, executor);
      InsertRowNode insertRowNode = getInsertRowNodeWithMeasurementSchemas();
      TriggerFireVisitor fireVisitor = new TriggerFireVisitor();
      TriggerFireResult result = fireVisitor.process(insertRowNode, TriggerEvent.AFTER_INSERT);
      Assert.assertEquals(TriggerFireResult.SUCCESS, result);
      executor.onDrop();
      TestTrigger test = (TestTrigger) trigger;
      Assert.assertTrue((test.onCreated));
      Assert.assertTrue((test.onValidated));
      Assert.assertTrue((test.onFired));
      Assert.assertTrue((test.onDropped));
      Assert.assertEquals(FailureStrategy.OPTIMISTIC, executor.getFailureStrategy());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private InsertRowNode getInsertRowNodeWithMeasurementSchemas() throws IllegalPathException {
    long time = 80L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = 5.0;
    columns[1] = 6.0f;
    columns[2] = 1000l;
    columns[3] = 10;
    columns[4] = true;

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode 2"),
            new PartialPath("root.sg"),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            time,
            columns,
            false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN)
        });

    return insertRowNode;
  }

  private static class TestTrigger implements Trigger {
    boolean onDropped = false;
    boolean onValidated = false;
    boolean onCreated = false;

    boolean onFired = false;

    @Override
    public void validate(TriggerAttributes attributes) throws Exception {
      onValidated = true;
    }

    @Override
    public void onCreate(TriggerAttributes attributes) {
      onCreated = true;
    }

    @Override
    public boolean fire(Tablet tablet) {
      onFired = true;
      return true;
    }

    @Override
    public void onDrop() {
      onDropped = true;
    }
  }
}
