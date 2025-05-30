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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.FunctionNullability;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.LongType;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class WindowNodeSerdeTest {
  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException, IOException {
    // Construct other parameters
    SeriesScanNode child =
        new SeriesScanNode(
            new PlanNodeId("TestSeriesScanNode"),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Ordering.DESC,
            null,
            100,
            100,
            null);
    PlanNodeId nodeId = new PlanNodeId("testWindowFunctionNode");
    DataOrganizationSpecification specification = getDataOrganizationSpecification();
    Optional<Symbol> hashSymbol = Optional.of(new Symbol("hash_col"));

    // Construct window function
    BoundSignature signature =
        new BoundSignature("count", LongType.INT64, Collections.singletonList(DoubleType.DOUBLE));
    FunctionId functionId = new FunctionId("count_id");
    FunctionKind functionKind = FunctionKind.AGGREGATE;
    FunctionNullability nullability = FunctionNullability.getAggregationFunctionNullability(1);
    ResolvedFunction resolvedFunction =
        new ResolvedFunction(signature, functionId, functionKind, true, nullability);

    WindowNode.Frame frame = WindowNode.Frame.DEFAULT_FRAME;
    Expression arguments = new Identifier("col1");
    WindowNode.Function function =
        new WindowNode.Function(
            resolvedFunction, Collections.singletonList(arguments), frame, true);

    Symbol symbol = new Symbol("cnt");
    HashMap<Symbol, WindowNode.Function> map = new HashMap<>();
    map.put(symbol, function);

    WindowNode windowNode =
        new WindowNode(nodeId, child, specification, map, hashSymbol, new HashSet<>(), 0);

    // Serialize and deserialize the node
    ByteBuffer buffer = ByteBuffer.allocate(8196);
    windowNode.serialize(buffer);
    buffer.flip();
    PlanNode deserialized = PlanNodeDeserializeHelper.deserialize(buffer);
    assertEquals(windowNode, deserialized);
  }

  private static DataOrganizationSpecification getDataOrganizationSpecification() {
    // Partition By
    ImmutableList<Symbol> partitionBy = ImmutableList.of(new Symbol("col1"), new Symbol("col2"));

    // Order By
    Symbol col3 = new Symbol("col3");
    List<Symbol> orderBy = Collections.singletonList(col3);
    Map<Symbol, SortOrder> orderings = Collections.singletonMap(col3, SortOrder.ASC_NULLS_LAST);
    Optional<OrderingScheme> orderingScheme = Optional.of(new OrderingScheme(orderBy, orderings));

    return new DataOrganizationSpecification(partitionBy, orderingScheme);
  }
}
