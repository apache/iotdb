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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.NextFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class NextFillNodeSerdeTest {

  @Test
  public void testNextFillNodeSerde() throws Exception {
    NextFillNode node =
        new NextFillNode(
            new PlanNodeId("nextFill"),
            null,
            new TimeDuration(0, 2),
            new Symbol("time"),
            ImmutableList.of(new Symbol("city")));

    NextFillNode deserialized = assertNextFillNodeSerde(node);
    assertTrue(deserialized.getTimeBound().isPresent());
    assertEquals(new TimeDuration(0, 2), deserialized.getTimeBound().get());
    assertTrue(deserialized.getHelperColumn().isPresent());
    assertEquals(new Symbol("time"), deserialized.getHelperColumn().get());
    assertTrue(deserialized.getGroupingKeys().isPresent());
    assertEquals(ImmutableList.of(new Symbol("city")), deserialized.getGroupingKeys().get());
  }

  @Test
  public void testPlainNextFillNodeSerde() throws Exception {
    NextFillNode node = new NextFillNode(new PlanNodeId("plainNextFill"), null, null, null, null);

    NextFillNode deserialized = assertNextFillNodeSerde(node);
    assertFalse(deserialized.getTimeBound().isPresent());
    assertFalse(deserialized.getHelperColumn().isPresent());
    assertFalse(deserialized.getGroupingKeys().isPresent());
  }

  @Test
  public void testPartialNextFillNodeSerde() throws Exception {
    NextFillNode node =
        new NextFillNode(
            new PlanNodeId("partialNextFill"),
            null,
            new TimeDuration(0, 5),
            new Symbol("time"),
            null);

    NextFillNode deserialized = assertNextFillNodeSerde(node);
    assertTrue(deserialized.getTimeBound().isPresent());
    assertEquals(new TimeDuration(0, 5), deserialized.getTimeBound().get());
    assertTrue(deserialized.getHelperColumn().isPresent());
    assertEquals(new Symbol("time"), deserialized.getHelperColumn().get());
    assertFalse(deserialized.getGroupingKeys().isPresent());
  }

  @Test
  public void testPreviousFillNodeEqualsIncludesGroupingKeys() {
    PreviousFillNode cityGroupNode =
        new PreviousFillNode(
            new PlanNodeId("previousFill"), null, null, null, ImmutableList.of(new Symbol("city")));
    PreviousFillNode deviceGroupNode =
        new PreviousFillNode(
            new PlanNodeId("previousFill"),
            null,
            null,
            null,
            ImmutableList.of(new Symbol("device")));

    assertNotEquals(cityGroupNode, deviceGroupNode);
    assertNotEquals(cityGroupNode.hashCode(), deviceGroupNode.hashCode());
  }

  private static NextFillNode assertNextFillNodeSerde(NextFillNode node) throws Exception {
    ByteBuffer byteBuffer = node.serializeToByteBuffer();
    PlanNode deserialized = PlanNodeDeserializeHelper.deserialize(byteBuffer);
    assertEquals(node, deserialized);
    return (NextFillNode) deserialized;
  }
}
