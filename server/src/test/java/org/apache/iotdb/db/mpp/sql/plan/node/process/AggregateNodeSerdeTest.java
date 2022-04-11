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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AggregateNodeSerdeTest {

  @Test
  public void TestSerializeAndDeserialize() throws IllegalPathException {
    Map<PartialPath, Set<AggregationType>> aggregateFuncMap = new HashMap<>();
    Set<AggregationType> aggregationTypes = new HashSet<>();
    aggregationTypes.add(AggregationType.MAX_TIME);
    aggregateFuncMap.put(
        new MeasurementPath("root.sg.d1.s1", TSDataType.BOOLEAN), aggregationTypes);
    AggregateNode aggregateNode =
        new AggregateNode(new PlanNodeId("TestAggregateNode"), null, aggregateFuncMap, null);
    aggregateNode.addChild(new ShowDevicesNode(new PlanNodeId("TestShowDevice")));
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    aggregateNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals((AggregateNode) PlanNodeDeserializeHelper.deserialize(byteBuffer), aggregateNode);
  }
}
