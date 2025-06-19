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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SkipToPosition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.LogicalIndexPointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ScalarValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.concatenation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.excluded;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.label;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.Patterns.starQuantified;
import static org.junit.Assert.assertEquals;

public class PatternRecognitionNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException, IOException {
    SeriesScanNode child =
        new SeriesScanNode(
            new PlanNodeId("TestSeriesScanNode"),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Ordering.DESC,
            null,
            100,
            100,
            null);

    PlanNodeId nodeId = new PlanNodeId("testPatternRecognitionNode");

    // partition by
    ImmutableList<Symbol> partitionBy = ImmutableList.of(new Symbol("col1"), new Symbol("col2"));

    // order by
    Symbol col3 = new Symbol("col3");
    List<Symbol> orderBy = Collections.singletonList(col3);
    Map<Symbol, SortOrder> orderings = Collections.singletonMap(col3, SortOrder.ASC_NULLS_LAST);
    Optional<OrderingScheme> orderingScheme = Optional.of(new OrderingScheme(orderBy, orderings));

    Optional<Symbol> hashSymbol = Optional.of(new Symbol("hash_col"));

    // measures
    Expression expression =
        new FunctionCall(QualifiedName.of("RPR_LAST"), ImmutableList.of(new Identifier("price")));

    ExpressionAndValuePointers evp = getExpressionAndValuePointers(expression);
    Type type = DoubleType.DOUBLE;
    Measure measure = new Measure(evp, type);

    Map<Symbol, Measure> measures = new HashMap<>();
    measures.put(new Symbol("last_price"), measure);

    // rows per match
    RowsPerMatch rowsPerMatch = RowsPerMatch.ALL_SHOW_EMPTY;

    // skip-to labels
    Set<IrLabel> skipToLabels = new HashSet<>();
    skipToLabels.add(new IrLabel("A"));
    skipToLabels.add(new IrLabel("B"));

    // skip-to position
    SkipToPosition skipToPosition = SkipToPosition.LAST;

    // row pattern
    IrRowPattern pattern = concatenation(label("A"), starQuantified(excluded(label("B")), true));

    // variable definitions
    Map<IrLabel, ExpressionAndValuePointers> variableDefinitions = new HashMap<>();
    variableDefinitions.put(new IrLabel("A"), getExpressionAndValuePointers(expression));

    // Create the PatternRecognitionNode
    PatternRecognitionNode node =
        new PatternRecognitionNode(
            nodeId,
            child,
            partitionBy,
            orderingScheme,
            hashSymbol,
            measures,
            rowsPerMatch,
            skipToLabels,
            skipToPosition,
            pattern,
            variableDefinitions);

    // Serialize and deserialize the node
    ByteBuffer buffer = ByteBuffer.allocate(8192);
    node.serialize(buffer);
    buffer.flip();
    PlanNode deserialized = PlanNodeDeserializeHelper.deserialize(buffer);
    assertEquals(node, deserialized);
  }

  private static ExpressionAndValuePointers getExpressionAndValuePointers(Expression expression) {
    LogicalIndexPointer logicalIndexPointer =
        new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0);

    Symbol inputSymbol = new Symbol("price");
    ScalarValuePointer scalarValuePointer =
        new ScalarValuePointer(logicalIndexPointer, inputSymbol);

    ExpressionAndValuePointers.Assignment assignment =
        new ExpressionAndValuePointers.Assignment(inputSymbol, scalarValuePointer);
    ExpressionAndValuePointers evp =
        new ExpressionAndValuePointers(expression, ImmutableList.of(assignment));

    return evp;
  }
}
