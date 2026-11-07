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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.CommonPlanNodeDeserializer;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class UnpivotNodeTest {
  private UnpivotNode node() {
    return new UnpivotNode(
        new PlanNodeId("unpivot"),
        null,
        Collections.singletonList(new Symbol("time")),
        Arrays.asList(
            Arrays.asList(new Symbol("a"), new Symbol("b")),
            Arrays.asList(new Symbol("c"), new Symbol("d"))),
        Arrays.asList("first", "second"),
        Arrays.asList(
            new Symbol("time"), new Symbol("name"), new Symbol("value1"), new Symbol("value2")),
        true);
  }

  @Test
  public void testByteBufferRoundTrip() {
    UnpivotNode expected = node();
    assertEquals(PlanNodeType.TABLE_UNPIVOT_NODE, expected.getType());
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    expected.serialize(buffer);
    buffer.flip();
    UnpivotNode actual = (UnpivotNode) CommonPlanNodeDeserializer.INSTANCE.deserialize(buffer);
    assertEquals(expected, actual);
    assertEquals(expected.getNames(), actual.getNames());
    assertEquals(expected.isIncludeNulls(), actual.isIncludeNulls());
  }

  @Test
  public void testStreamRoundTrip() throws Exception {
    UnpivotNode expected = node();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    expected.serialize(new DataOutputStream(bytes));
    UnpivotNode actual =
        (UnpivotNode)
            CommonPlanNodeDeserializer.INSTANCE.deserialize(ByteBuffer.wrap(bytes.toByteArray()));
    assertEquals(expected, actual);
  }
}
