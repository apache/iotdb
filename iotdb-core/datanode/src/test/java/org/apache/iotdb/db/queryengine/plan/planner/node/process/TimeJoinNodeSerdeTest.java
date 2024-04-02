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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TimeJoinNodeSerdeTest {

  private static SeriesScanNode seriesScanNode1;
  private static SeriesScanNode seriesScanNode2;

  static {
    try {
      seriesScanNode1 =
          new SeriesScanNode(
              new PlanNodeId("TestSeriesScanNode"),
              new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
              Ordering.DESC,
              null,
              100,
              100,
              null);
      seriesScanNode2 =
          new SeriesScanNode(
              new PlanNodeId("TestSeriesScanNode"),
              new MeasurementPath("root.sg.d1.s2", TSDataType.INT32),
              Ordering.DESC,
              null,
              100,
              100,
              null);
    } catch (IllegalPathException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSerializeAndDeserializeFullOuterTimeJoin() throws IllegalPathException {
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(new PlanNodeId("TestFullOuterTimeJoinNode"), Ordering.ASC);
    fullOuterTimeJoinNode.addChild(seriesScanNode1);
    fullOuterTimeJoinNode.addChild(seriesScanNode2);

    testSerde(fullOuterTimeJoinNode);
  }

  @Test
  public void testSerializeAndDeserializeInnerTimeJoin1() throws IllegalPathException {
    InnerTimeJoinNode innerTimeJoinNode =
        new InnerTimeJoinNode(new PlanNodeId("TestInnerTimeJoinNode"), Ordering.ASC, null, null);
    innerTimeJoinNode.addChild(seriesScanNode1);
    innerTimeJoinNode.addChild(seriesScanNode2);

    testSerde(innerTimeJoinNode);
  }

  @Test
  public void testSerializeAndDeserializeInnerTimeJoin2() throws IllegalPathException {
    InnerTimeJoinNode innerTimeJoinNode =
        new InnerTimeJoinNode(
            new PlanNodeId("TestInnerTimeJoinNode"),
            Ordering.ASC,
            Arrays.asList(0L, 1L),
            Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"));
    innerTimeJoinNode.addChild(seriesScanNode1);
    innerTimeJoinNode.addChild(seriesScanNode2);

    testSerde(innerTimeJoinNode);
  }

  @Test
  public void testSerializeAndDeserializeLeftOuterTimeJoin() throws IllegalPathException {
    LeftOuterTimeJoinNode leftOuterTimeJoinNode =
        new LeftOuterTimeJoinNode(new PlanNodeId("TestLeftOuterTimeJoinNode"), Ordering.ASC);
    leftOuterTimeJoinNode.addChild(seriesScanNode1);
    leftOuterTimeJoinNode.addChild(seriesScanNode2);

    testSerde(leftOuterTimeJoinNode);
  }

  private void testSerde(PlanNode node) throws IllegalPathException {
    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    node.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), node);
  }
}
