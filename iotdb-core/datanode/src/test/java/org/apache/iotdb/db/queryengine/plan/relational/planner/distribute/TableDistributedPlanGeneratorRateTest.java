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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.commons.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.commons.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.FunctionNullability;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableDistributedPlanGeneratorRateTest {

  private static final Symbol DEVICE = new Symbol("device");
  private static final Symbol TIME = new Symbol("time");

  @Test
  public void testRateInputIsOrderedWhenTimeFollowsGroupingKeys() {
    OrderingScheme ordering =
        new OrderingScheme(
            ImmutableList.of(DEVICE, TIME),
            ImmutableMap.of(DEVICE, SortOrder.ASC_NULLS_LAST, TIME, SortOrder.ASC_NULLS_LAST));

    assertTrue(
        TableDistributedPlanGenerator.isInputOrderedByTimeAscending(
            aggregation("rate"),
            AggregationNode.Step.SINGLE,
            Collections.singletonList(DEVICE),
            ordering));
  }

  @Test
  public void testRateInputIsNotOrderedForPartialDescendingOrNonGroupingPrefix() {
    OrderingScheme descending =
        new OrderingScheme(
            Collections.singletonList(TIME),
            Collections.singletonMap(TIME, SortOrder.DESC_NULLS_LAST));
    assertFalse(
        TableDistributedPlanGenerator.isInputOrderedByTimeAscending(
            aggregation("increase"),
            AggregationNode.Step.SINGLE,
            Collections.emptyList(),
            descending));

    OrderingScheme ascending =
        new OrderingScheme(
            Collections.singletonList(TIME),
            Collections.singletonMap(TIME, SortOrder.ASC_NULLS_LAST));
    assertFalse(
        TableDistributedPlanGenerator.isInputOrderedByTimeAscending(
            aggregation("irate"),
            AggregationNode.Step.PARTIAL,
            Collections.emptyList(),
            ascending));

    Symbol nonGroupingPrefix = new Symbol("value_order");
    OrderingScheme invalidPrefix =
        new OrderingScheme(
            ImmutableList.of(nonGroupingPrefix, TIME),
            ImmutableMap.of(
                nonGroupingPrefix, SortOrder.ASC_NULLS_LAST, TIME, SortOrder.ASC_NULLS_LAST));
    assertFalse(
        TableDistributedPlanGenerator.isInputOrderedByTimeAscending(
            aggregation("delta"),
            AggregationNode.Step.SINGLE,
            Collections.singletonList(DEVICE),
            invalidPrefix));
  }

  private static AggregationNode.Aggregation aggregation(String functionName) {
    ResolvedFunction function =
        new ResolvedFunction(
            new BoundSignature(
                functionName,
                TypeFactory.getType(TSDataType.DOUBLE),
                ImmutableList.of(
                    TypeFactory.getType(TSDataType.DOUBLE),
                    TypeFactory.getType(TSDataType.TIMESTAMP))),
            new FunctionId(functionName),
            FunctionKind.AGGREGATE,
            true,
            FunctionNullability.getAggregationFunctionNullability(2));
    return new AggregationNode.Aggregation(
        function,
        ImmutableList.of(new SymbolReference("value"), new SymbolReference("time")),
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
