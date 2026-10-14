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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ValuesNodeSerdeTest {
  @Test
  public void testEmptyValuesWithColumns() throws IOException {
    assertRoundTrip(
        new ValuesNode(
            new PlanNodeId("empty"),
            ImmutableList.of(new Symbol("time"), new Symbol("value")),
            ImmutableList.of()));
  }

  @Test
  public void testEmptyValuesWithoutColumns() throws IOException {
    assertRoundTrip(new ValuesNode(new PlanNodeId("empty"), 0));
  }

  @Test
  public void testSingleRowWithoutColumns() throws IOException {
    assertRoundTrip(new ValuesNode(new PlanNodeId("single"), 1));
  }

  private static void assertRoundTrip(ValuesNode expected) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    expected.serialize(buffer);
    buffer.flip();

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream stream = new DataOutputStream(bytes)) {
      expected.serialize(stream);
    }
    assertEquals(ByteBuffer.wrap(bytes.toByteArray()), buffer);

    // Deserialization must only read the buffer, including when a plan arrives over the network.
    ByteBuffer input = buffer.asReadOnlyBuffer();
    ValuesNode actual = (ValuesNode) PlanNodeType.deserialize(input);
    assertEquals(expected.getPlanNodeId(), actual.getPlanNodeId());
    assertEquals(expected.getOutputSymbols(), actual.getOutputSymbols());
    assertEquals(expected.getRowCount(), actual.getRowCount());
    assertEquals(expected.getRows(), actual.getRows());
    assertEquals(0, input.getInt()); // child count
    assertFalse(input.hasRemaining());
  }
}
