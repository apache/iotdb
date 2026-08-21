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
import org.apache.iotdb.commons.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.commons.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.commons.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.FunctionNullability;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.google.common.collect.ImmutableList;
import org.apache.iotdb.google.common.collect.ImmutableMap;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationNodeRateSerdeTest {

  @Test
  public void testInputOrderedByTimeAscendingSurvivesSerialization() throws Exception {
    Symbol output = new Symbol("rate_value");
    ResolvedFunction function =
        new ResolvedFunction(
            new BoundSignature(
                "rate",
                TypeFactory.getType(TSDataType.DOUBLE),
                ImmutableList.of(
                    TypeFactory.getType(TSDataType.DOUBLE),
                    TypeFactory.getType(TSDataType.TIMESTAMP))),
            new FunctionId("rate"),
            FunctionKind.AGGREGATE,
            true,
            FunctionNullability.getAggregationFunctionNullability(2));
    AggregationNode.Aggregation aggregation =
        new AggregationNode.Aggregation(
            function,
            ImmutableList.of(new SymbolReference("value"), new SymbolReference("time")),
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            true);
    AggregationNode node =
        new AggregationNode(
            new PlanNodeId("rateAggregation"),
            null,
            ImmutableMap.of(output, aggregation),
            AggregationNode.singleGroupingSet(Collections.emptyList()),
            Collections.emptyList(),
            AggregationNode.Step.SINGLE,
            Optional.empty(),
            Optional.empty());

    ByteBuffer byteBuffer = node.serializeToByteBuffer();
    PlanNode deserialized = PlanNodeDeserializeHelper.deserialize(byteBuffer);

    AggregationNode deserializedAggregationNode = (AggregationNode) deserialized;
    assertEquals(aggregation, deserializedAggregationNode.getAggregations().get(output));
    assertTrue(
        deserializedAggregationNode.getAggregations().get(output).isInputOrderedByTimeAscending());
  }
}
