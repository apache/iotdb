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
package org.apache.iotdb.db.mpp.sql.plan.node.process;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.junit.Test;

public class AggregateNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() {
    List<PlanNode> planNodes = new ArrayList<>();
    planNodes.add(new ShowDevicesNode(new PlanNodeId("TestShowDevice")));
    List<String> columnNames = new ArrayList<>();
    columnNames.add("s1"); columnNames.add("s2");
    AggregateNode aggregateNode = new AggregateNode(new PlanNodeId("TestAggregateNode"), null, planNodes, columnNames);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    aggregateNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), aggregateNode);
  }
}
