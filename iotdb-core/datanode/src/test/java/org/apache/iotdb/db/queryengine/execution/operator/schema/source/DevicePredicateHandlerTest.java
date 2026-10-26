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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.FailFunctionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.TableCaseWhenThenColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.CastFunctionColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.UpperColumnTransformer;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DevicePredicateHandlerTest {

  private static final List<TsTableColumnSchema> COLUMNS =
      Collections.singletonList(new TagColumnSchema("tag", TSDataType.STRING));

  @Test
  public void testPredicateFilterDoesNotReuseFullBatches() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer predicate = graph.matchesCase(graph.input(0));
    try (DevicePredicateFilter filter = predicateFilter(graph, predicate)) {
      int batchSize = DevicePredicateHandler.DEFAULT_MAX_TS_BLOCK_LINE_NUMBER;
      for (String value : new String[] {"match", "other"}) {
        for (int i = 0; i < batchSize; i++) {
          filter.addBatch(device(value));
        }
        // addBatch evaluates full batches automatically; exhausting the iterator resets the batch.
        List<String> result = consume(filter);
        assertEquals(batchSize, result.size());
        for (String actual : result) {
          assertEquals(value, actual);
        }
      }
    }
  }

  @Test
  public void testPredicateFilterDoesNotReuseDifferentSizedBatches() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer predicate = graph.matchesCase(graph.input(0));
    try (DevicePredicateFilter filter = predicateFilter(graph, predicate)) {
      assertEquals(Collections.singletonList("match"), filterBatch(filter, "match"));
      assertEquals(Arrays.asList("other", "match"), filterBatch(filter, "other", "match"));
      assertEquals(Collections.emptyList(), filterBatch(filter, "skip"));
    }
  }

  @Test
  public void testBlackListConstructorDoesNotReusePredicateAcrossBatches() throws Exception {
    Graph graph = new Graph();
    ColumnTransformer predicate = graph.matchesCase(graph.input(0));
    List<IDeviceMNode<IMemMNode>> nodes =
        Arrays.asList(node("match", 0), node("other", 1), node("match", 2));
    try (DeviceBlackListConstructor constructor =
        new DeviceBlackListConstructor(
            graph.leaves,
            reference(predicate),
            "table",
            COLUMNS,
            (pointer, name) -> null,
            mock(MemSchemaRegionStatistics.class),
            "test",
            new TsTable("table"))) {
      constructor.handleDeviceNode(nodes.get(0));
      constructor.prepareBatchResult();
      constructor.update();
      constructor.handleDeviceNode(nodes.get(1));
      constructor.handleDeviceNode(nodes.get(2));
      constructor.prepareBatchResult();
      constructor.update();
      assertEquals(3, constructor.getPreDeletedNum());
      for (IDeviceMNode<IMemMNode> node : nodes) {
        verify(node).preDeactivateSelfOrTemplate();
      }
    }
  }

  @Test
  public void testAttributeUpdaterDoesNotReuseFilterAcrossBatches() throws Exception {
    Graph filter = new Graph();
    ColumnTransformer predicate = filter.matchesCase(filter.input(0));
    Graph projection = new Graph();
    List<List<String>> updates = new ArrayList<>();
    try (DeviceAttributeUpdater updater =
        attributeUpdater(
            filter, predicate, Collections.emptyList(), projection, updates, projection.input(0))) {
      updateBatch(updater, "match");
      updateBatch(updater, "other", "match");
      assertEquals(
          Arrays.asList(
              Arrays.asList("match", "match"),
              Arrays.asList("other", "other"),
              Arrays.asList("match", "match")),
          updates);
    }
  }

  @Test
  public void testAttributeUpdaterDoesNotReuseProjectionAcrossBatches() throws Exception {
    Graph projection = new Graph();
    ColumnTransformer input = projection.input(0);
    ColumnTransformer caseWhen = projection.caseResult(input);
    List<List<String>> updates = new ArrayList<>();
    try (DeviceAttributeUpdater updater =
        attributeUpdater(
            null, null, Collections.emptyList(), projection, updates, caseWhen, input)) {
      updateBatch(updater, "match");
      updateBatch(updater, "other", "match");
      assertEquals(
          Arrays.asList(
              Arrays.asList("match", "match", "match"),
              Arrays.asList("other", "other", "other"),
              Arrays.asList("match", "match", "match")),
          updates);
    }
  }

  @Test
  public void testAttributeUpdaterRecomputesCommonExpressionClearedByCase() throws Exception {
    Graph filter = new Graph();
    ColumnTransformer shared = new UpperColumnTransformer(STRING, reference(filter.input(0)));
    ColumnTransformer predicate =
        filter.caseWhen(
            filter.equal(shared, filter.constant("MATCH")),
            filter.constant(true),
            filter.constant(false));
    Graph projection = new Graph();
    List<List<String>> updates = new ArrayList<>();
    try (DeviceAttributeUpdater updater =
        attributeUpdater(
            filter,
            predicate,
            Collections.singletonList(shared),
            projection,
            updates,
            projection.input(1))) {
      updateBatch(updater, "match");
      updateBatch(updater, "other", "match");
      assertEquals(
          Arrays.asList(Arrays.asList("match", "MATCH"), Arrays.asList("match", "MATCH")), updates);
    }
  }

  @Test
  public void testAttributeUpdaterSharedCastAndDiffDoNotWriteNullInEitherOrder() throws Exception {
    for (boolean diffFirst : new boolean[] {false, true}) {
      Graph filter = new Graph();
      ColumnTransformer input = filter.input(0);
      ColumnTransformer value =
          new CastFunctionColumnTransformer(DOUBLE, reference(input), ZoneId.of("UTC"));
      ColumnTransformer diff =
          new DiffColumnTransformer(
              DOUBLE, reference(value), reference(filter.equal(input, filter.constant("x"))));
      ColumnTransformer zero =
          filter.leaf(
              new ConstantColumnTransformer(
                  DOUBLE, new DoubleColumn(1, Optional.empty(), new double[] {0})));
      ColumnTransformer predicate =
          filter.caseWhen(
              filter.equal(input, filter.constant("n/a")),
              filter.constant(false),
              new CompareGreaterThanColumnTransformer(BOOLEAN, reference(diff), reference(zero)));
      List<ColumnTransformer> common =
          diffFirst
              ? Arrays.asList(reference(diff), reference(value))
              : Arrays.asList(reference(value), reference(diff));
      Graph projection = new Graph();
      ColumnTransformer castInput =
          projection.leaf(new IdentityColumnTransformer(DOUBLE, diffFirst ? 2 : 1));
      ColumnTransformer diffInput =
          projection.leaf(new IdentityColumnTransformer(DOUBLE, diffFirst ? 1 : 2));
      ColumnTransformer a =
          new CastFunctionColumnTransformer(STRING, reference(castInput), ZoneId.of("UTC"));
      ColumnTransformer b =
          new CastFunctionColumnTransformer(STRING, reference(diffInput), ZoneId.of("UTC"));
      List<List<String>> updates = new ArrayList<>();
      try (DeviceAttributeUpdater updater =
          new DeviceAttributeUpdater(
              Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE, TSDataType.DOUBLE),
              filter.leaves,
              reference(predicate),
              common,
              COLUMNS,
              projection.leaves,
              Arrays.asList(reference(a), reference(b)),
              (pointer, name) -> null,
              (deviceId, pointer, values) -> {
                List<String> row = new ArrayList<>();
                row.add(deviceId[0]);
                for (Object result : values) {
                  row.add(
                      result == null
                          ? null
                          : ((Binary) result).getStringValue(TSFileConfig.STRING_CHARSET));
                }
                updates.add(row);
              },
              Arrays.asList("a", "b"),
              "test",
              new TsTable("table"))) {
        updateBatch(updater, "1", "9", "5");
        assertEquals(Collections.singletonList(Arrays.asList("9", "9.0", "8.0")), updates);
      }
    }
  }

  @Test
  public void testAttributeUpdaterSkipsGuardedCommonDiffWhenNoDevicesMatch() throws Exception {
    Graph filter = new Graph();
    ColumnTransformer input = filter.input(0);
    ColumnTransformer value =
        new CastFunctionColumnTransformer(DOUBLE, reference(input), ZoneId.of("UTC"));
    ColumnTransformer diff =
        new DiffColumnTransformer(
            DOUBLE, reference(value), reference(filter.equal(input, filter.constant("x"))));
    ColumnTransformer zero =
        filter.leaf(
            new ConstantColumnTransformer(
                DOUBLE, new DoubleColumn(1, Optional.empty(), new double[] {0})));
    ColumnTransformer predicate =
        filter.caseWhen(
            filter.equal(input, filter.constant("n/a")),
            filter.constant(false),
            new CompareGreaterThanColumnTransformer(BOOLEAN, reference(diff), reference(zero)));
    Graph projection = new Graph();
    ColumnTransformer commonInput = projection.leaf(new IdentityColumnTransformer(DOUBLE, 1));
    ColumnTransformer output =
        new CastFunctionColumnTransformer(STRING, reference(commonInput), ZoneId.of("UTC"));
    List<List<String>> updates = new ArrayList<>();
    List<Integer> updatedPointers = new ArrayList<>();
    try (DeviceAttributeUpdater updater =
        new DeviceAttributeUpdater(
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            filter.leaves,
            reference(predicate),
            Collections.singletonList(reference(diff)),
            COLUMNS,
            projection.leaves,
            Collections.singletonList(reference(output)),
            (pointer, name) -> null,
            (deviceId, pointer, values) -> {
              updates.add(
                  Arrays.asList(
                      deviceId[0],
                      ((Binary) values[0]).getStringValue(TSFileConfig.STRING_CHARSET)));
              updatedPointers.add(pointer);
            },
            Collections.singletonList("a"),
            "test",
            new TsTable("table"))) {
      // The CASE guard excludes the invalid CAST, and the first numeric DIFF is null.
      updateBatch(updater, 10, "n/a", "5");
      assertTrue(updates.isEmpty());
      assertFalse(updater.hasComputedResult());
      assertTrue(updater.deviceSchemaBatch.isEmpty());

      // Retain the filter's DIFF state, but not the skipped batch's attribute pointers.
      updateBatch(updater, 20, "10", "20");
      assertEquals(Arrays.asList(Arrays.asList("10", "5.0"), Arrays.asList("20", "10.0")), updates);
      assertEquals(Arrays.asList(20, 21), updatedPointers);

      updateBatch(updater, 30, "n/a");
      updateBatch(updater, 40, "30", "40");
      assertEquals(
          Arrays.asList(
              Arrays.asList("10", "5.0"), Arrays.asList("20", "10.0"), Arrays.asList("40", "10.0")),
          updates);
      assertEquals(Arrays.asList(20, 21, 41), updatedPointers);
    }
  }

  @Test
  public void testAttributeUpdaterSkipsProjectionWhenNoDevicesMatch() throws Exception {
    Graph filter = new Graph();
    ColumnTransformer predicate = filter.equal(filter.input(0), filter.constant("match"));
    List<List<String>> updates = new ArrayList<>();
    try (DeviceAttributeUpdater updater =
        attributeUpdater(
            filter,
            predicate,
            Collections.emptyList(),
            new Graph(),
            updates,
            new FailFunctionColumnTransformer(
                STRING, "Empty batches must not evaluate projections"))) {
      updateBatch(updater, "skip");
      assertTrue(updates.isEmpty());
      assertFalse(updater.hasComputedResult());
    }
  }

  private static DevicePredicateFilter predicateFilter(Graph graph, ColumnTransformer predicate) {
    return new DevicePredicateFilter(
        graph.leaves, reference(predicate), COLUMNS, "test", new TsTable("table"));
  }

  private static List<String> filterBatch(DevicePredicateFilter filter, String... values) {
    for (String value : values) {
      filter.addBatch(device(value));
    }
    filter.prepareBatchResult();
    return consume(filter);
  }

  private static List<String> consume(DevicePredicateFilter filter) {
    List<String> result = new ArrayList<>();
    while (filter.hasNext()) {
      result.add(filter.next().getRawNodes()[3]);
    }
    return result;
  }

  private static DeviceAttributeUpdater attributeUpdater(
      Graph filter,
      ColumnTransformer predicate,
      List<ColumnTransformer> common,
      Graph projection,
      List<List<String>> updates,
      ColumnTransformer... outputs) {
    List<ColumnTransformer> projectOutputs = new ArrayList<>();
    for (ColumnTransformer output : outputs) {
      projectOutputs.add(reference(output));
    }
    common.forEach(ColumnTransformer::addReferenceCount);
    return new DeviceAttributeUpdater(
        Collections.nCopies(1 + common.size(), TSDataType.STRING),
        filter == null ? Collections.emptyList() : filter.leaves,
        predicate == null ? null : reference(predicate),
        common,
        COLUMNS,
        projection.leaves,
        projectOutputs,
        (pointer, name) -> null,
        (deviceId, pointer, values) -> {
          List<String> row = new ArrayList<>();
          row.add(deviceId[0]);
          for (Object value : values) {
            row.add(((Binary) value).getStringValue(TSFileConfig.STRING_CHARSET));
          }
          updates.add(row);
        },
        Collections.nCopies(outputs.length, "attribute"),
        "test",
        new TsTable("table"));
  }

  private static void updateBatch(DeviceAttributeUpdater updater, String... values)
      throws Exception {
    updateBatch(updater, 0, values);
  }

  private static void updateBatch(
      DeviceAttributeUpdater updater, int firstPointer, String... values) throws Exception {
    for (int i = 0; i < values.length; i++) {
      updater.handleDeviceNode(node(values[i], firstPointer + i));
    }
    updater.prepareBatchResult();
    updater.update();
  }

  private static ShowDevicesResult device(String value) {
    return new ShowDevicesResult(null, true, 0, new String[] {"root", "test", "table", value});
  }

  @SuppressWarnings("unchecked")
  private static IDeviceMNode<IMemMNode> node(String value, int pointer) {
    IDeviceMNode<IMemMNode> node = mock(IDeviceMNode.class);
    TableDeviceInfo<IMemMNode> info = new TableDeviceInfo<>();
    info.setAttributePointer(pointer);
    when(node.getDeviceInfo()).thenReturn(info);
    when(node.getPartialPath()).thenReturn(new PartialPath(device(value).getRawNodes()));
    when(node.isAlignedNullable()).thenReturn(true);
    return node;
  }

  private static ColumnTransformer reference(ColumnTransformer transformer) {
    transformer.addReferenceCount();
    return transformer;
  }

  private static class Graph {
    private final List<LeafColumnTransformer> leaves = new ArrayList<>();

    private ColumnTransformer input(int index) {
      return leaf(new IdentityColumnTransformer(STRING, index));
    }

    private ColumnTransformer constant(String value) {
      return leaf(
          new ConstantColumnTransformer(
              STRING,
              new BinaryColumn(
                  1,
                  Optional.empty(),
                  new Binary[] {new Binary(value, TSFileConfig.STRING_CHARSET)})));
    }

    private ColumnTransformer constant(boolean value) {
      return leaf(
          new ConstantColumnTransformer(
              BOOLEAN, new BooleanColumn(1, Optional.empty(), new boolean[] {value})));
    }

    private ColumnTransformer leaf(LeafColumnTransformer transformer) {
      leaves.add(transformer);
      return transformer;
    }

    private ColumnTransformer equal(ColumnTransformer left, ColumnTransformer right) {
      return new CompareEqualToColumnTransformer(BOOLEAN, reference(left), reference(right));
    }

    private ColumnTransformer caseWhen(
        ColumnTransformer when, ColumnTransformer then, ColumnTransformer otherwise) {
      return new TableCaseWhenThenColumnTransformer(
          then.getType(),
          Collections.singletonList(reference(when)),
          Collections.singletonList(reference(then)),
          reference(otherwise));
    }

    private ColumnTransformer caseResult(ColumnTransformer input) {
      return caseWhen(equal(input, constant("match")), constant("match"), constant("other"));
    }

    private ColumnTransformer matchesCase(ColumnTransformer input) {
      return equal(caseResult(input), input);
    }
  }
}
