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

package org.apache.iotdb.db.queryengine.plan.planner.node.process;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class IntoNodeSerdeTest {

  @Test
  public void testIntoSerde() throws IllegalPathException, IOException {
    IntoPathDescriptor descriptor = new IntoPathDescriptor();
    descriptor.specifyTargetPath("root.sg1.d1.s1", "", new PartialPath("root.sg1.new_d1.s1"));
    descriptor.specifyTargetPath("root.sg1.d1.s2", "", new PartialPath("root.sg1.new_d1.s2"));
    descriptor.specifyTargetPath("root.sg1.d2.s1", "", new PartialPath("root.sg1.new_d2.s1"));
    descriptor.specifyTargetPath("root.sg1.d2.s2", "", new PartialPath("root.sg1.new_d2.s2"));
    descriptor.specifyDeviceAlignment("root.sg1.new_d1", true);
    descriptor.specifyDeviceAlignment("root.sg1.new_d2", false);
    IntoNode expectedNode = new IntoNode(new PlanNodeId("TestIntoNode"), descriptor);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    expectedNode.serialize(dataOutputStream);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    Assert.assertEquals(expectedNode, PlanNodeDeserializeHelper.deserialize(buffer));
  }

  @Test
  public void testDeviceIntoSerde() throws IllegalPathException, IOException {
    DeviceViewIntoPathDescriptor descriptor = new DeviceViewIntoPathDescriptor();
    descriptor.specifyTargetDeviceMeasurement(
        new PartialPath("root.sg1.d1"), new PartialPath("root.sg1.new_d1"), "s1", "s1");
    descriptor.specifyTargetDeviceMeasurement(
        new PartialPath("root.sg1.d1"), new PartialPath("root.sg1.new_d1"), "s2", "s2");
    descriptor.specifyTargetDeviceMeasurement(
        new PartialPath("root.sg1.d2"), new PartialPath("root.sg1.new_d2"), "s1", "s1");
    descriptor.specifyTargetDeviceMeasurement(
        new PartialPath("root.sg1.d2"), new PartialPath("root.sg1.new_d2"), "s2", "s2");
    descriptor.specifyDeviceAlignment("root.sg1.new_d1", true);
    descriptor.specifyDeviceAlignment("root.sg1.new_d2", false);
    DeviceViewIntoNode expectedNode =
        new DeviceViewIntoNode(new PlanNodeId("TestIntoNode"), descriptor);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    expectedNode.serialize(dataOutputStream);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    Assert.assertEquals(expectedNode, PlanNodeDeserializeHelper.deserialize(buffer));
  }

  @Test
  public void testTableIntoSerde() throws IllegalPathException, IOException {
    org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode expectedNode =
        getRelationIntoNode();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    expectedNode.serialize(dataOutputStream);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    Assert.assertEquals(expectedNode, PlanNodeDeserializeHelper.deserialize(buffer));
  }

  private static org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode
      getRelationIntoNode() {
    List<ColumnSchema> sourceColumns =
        ImmutableList.of(
            new ColumnSchema("time", TimestampType.TIMESTAMP, false, TsTableColumnCategory.TIME),
            new ColumnSchema("id", StringType.STRING, false, TsTableColumnCategory.TAG),
            new ColumnSchema("voltage", FloatType.FLOAT, false, TsTableColumnCategory.FIELD));

    List<Symbol> neededInputColumnNames =
        ImmutableList.of(new Symbol("time"), new Symbol("voltage"), new Symbol("id"));

    return new org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode(
        new PlanNodeId("TestIntoNode"),
        null,
        "testdb",
        "testtb",
        sourceColumns,
        neededInputColumnNames,
        new Symbol("rows"));
  }
}
