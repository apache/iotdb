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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DiffColumnTransformer;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;

import org.apache.ratis.util.function.TriConsumer;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.calc.execution.operator.process.FilterAndProjectOperator.constructFilteredTsBlock;

public class DeviceAttributeUpdater extends DeviceUpdater {
  private final List<ColumnTransformer> commonTransformerList;
  private final boolean[] commonSupportsSelection;
  private final boolean[] commonUsesRetainedDiff;
  private final List<DiffColumnTransformer> commonDiffTransformers = new ArrayList<>();
  private final List<LeafColumnTransformer> projectLeafColumnTransformerList;
  private final List<ColumnTransformer> projectOutputTransformerList;
  private final ColumnTransformer[] projectCacheTransformers;
  private final TriConsumer<String[], Integer, Object[]> attributeUpdater;
  private final TsBlockBuilder filterTsBlockBuilder;
  private final List<Integer> attributePointers = new ArrayList<>();

  // Only for error log
  private final List<String> attributeNames;

  @SuppressWarnings("squid:S107")
  public DeviceAttributeUpdater(
      final List<TSDataType> filterOutputDataTypes,
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final List<ColumnTransformer> commonTransformerList,
      final List<TsTableColumnSchema> columnSchemaList,
      final List<LeafColumnTransformer> projectLeafColumnTransformerList,
      final List<ColumnTransformer> projectOutputTransformerList,
      final BiFunction<Integer, String, Binary> attributeProvider,
      final TriConsumer<String[], Integer, Object[]> attributeUpdater,
      final List<String> attributeNames,
      final String database,
      final TsTable table) {
    super(
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        commonTransformerList,
        columnSchemaList,
        attributeProvider,
        database,
        table);
    this.commonTransformerList = commonTransformerList;
    this.commonSupportsSelection = new boolean[commonTransformerList.size()];
    this.commonUsesRetainedDiff = new boolean[commonTransformerList.size()];
    for (int i = 0; i < commonTransformerList.size(); i++) {
      ColumnTransformer common = commonTransformerList.get(i);
      commonSupportsSelection[i] = common.canEvaluateWithSelection();
      if (!commonSupportsSelection[i]) {
        for (ColumnTransformer transformer :
            ColumnTransformer.collectTransformers(Collections.singletonList(common))) {
          if (transformer instanceof DiffColumnTransformer) {
            commonUsesRetainedDiff[i] = true;
            break;
          }
        }
      }
    }
    for (ColumnTransformer transformer :
        ColumnTransformer.collectTransformers(commonTransformerList)) {
      if (transformer instanceof DiffColumnTransformer) {
        DiffColumnTransformer diff = (DiffColumnTransformer) transformer;
        diff.retainResultForReuse();
        commonDiffTransformers.add(diff);
      }
    }
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.projectCacheTransformers =
        ColumnTransformer.collectTransformers(projectOutputTransformerList);
    this.attributeUpdater = attributeUpdater;
    this.filterTsBlockBuilder = new TsBlockBuilder(filterOutputDataTypes);
    this.attributeNames = attributeNames;
  }

  @Override
  public void handleDeviceNode(final IDeviceMNode<IMemMNode> node) throws MetadataException {
    attributePointers.add(
        ((TableDeviceInfo<IMemMNode>) node.getDeviceInfo()).getAttributePointer());
    super.handleDeviceNode(node);
  }

  protected void update() throws MetadataException {
    try {
      updateAttributes();
    } finally {
      commonDiffTransformers.forEach(DiffColumnTransformer::releaseRetainedResult);
    }
  }

  private void updateAttributes() throws MetadataException {
    if (!withoutFilter() && indexes.isEmpty()) {
      // No device needs an update. Do not retry expressions excluded by the filter's CASE guard.
      attributePointers.clear();
      super.clear();
      return;
    }

    final TsBlock block = getFilterTsBlock();

    for (ColumnTransformer transformer : projectCacheTransformers) {
      transformer.clearCacheForNewInput();
    }
    projectLeafColumnTransformerList.forEach(
        leafColumnTransformer -> leafColumnTransformer.initFromTsBlock(block));

    final List<Column> resultColumns =
        projectOutputTransformerList.stream()
            .map(
                columnTransformer -> {
                  columnTransformer.tryEvaluate();
                  return columnTransformer.getColumn();
                })
            .collect(Collectors.toList());

    for (int i = 0; i < (withoutFilter() ? attributePointers.size() : indexes.size()); ++i) {
      final String[] nodes =
          deviceSchemaBatch.get(withoutFilter() ? i : indexes.get(i)).getRawNodes();
      final Object[] results = new Object[resultColumns.size()];
      for (int j = 0; j < resultColumns.size(); ++j) {
        final Object o = resultColumns.get(j).isNull(i) ? null : resultColumns.get(j).getObject(i);
        if (Objects.nonNull(o) && !(o instanceof Binary)) {
          throw new MetadataException(
              String.format(
                  DataNodeQueryMessages
                      .QUERY_EXCEPTION_RESULT_TYPE_MISMATCH_FOR_ATTRIBUTE_S_EXPECTED_S_ACTUAL_S_E5637B91,
                  attributeNames.get(j),
                  Binary.class,
                  o.getClass()));
        }
        results[j] = o;
      }
      attributeUpdater.accept(
          Arrays.copyOfRange(nodes, 3, nodes.length),
          attributePointers.get(withoutFilter() ? i : indexes.get(i)),
          results);
    }

    attributePointers.clear();
    super.clear();
  }

  private TsBlock getFilterTsBlock() {
    if (withoutFilter()) {
      return curBlock;
    }
    filterTsBlockBuilder.reset();

    final List<Column> filterResultColumns =
        new ArrayList<>(Arrays.asList(curBlock.getValueColumns()));

    if (!commonTransformerList.isEmpty()) {
      boolean[] selection = null;
      // CASE may clear a common expression. Recompute only rows whose filter guard passed.
      commonDiffTransformers.forEach(DiffColumnTransformer::reuseRetainedResult);
      for (int i = 0; i < commonTransformerList.size(); i++) {
        ColumnTransformer transformer = commonTransformerList.get(i);
        if (commonSupportsSelection[i] && transformer.getColumnCachePositionCount() > 0) {
          filterResultColumns.add(transformer.getColumn());
          continue;
        }
        if (commonSupportsSelection[i] || commonUsesRetainedDiff[i]) {
          if (selection == null) {
            selection = new boolean[deviceSchemaBatch.size()];
            for (int index : indexes) {
              selection[index] = true;
            }
          }
          if (commonSupportsSelection[i]) {
            transformer.tryEvaluateWithSelection(selection);
          } else {
            // Rebuild wrappers from the DIFF results actually used by WHERE, including nulls.
            transformer.evaluateWithSelection(selection);
          }
        } else {
          // Stateful transformers must be re-evaluated over every row: a masked evaluation
          // would read the masked rows as nulls and corrupt their running state. The snapshot
          // taken at the input boundary makes the full re-evaluation idempotent.
          // A preceding common may have left a masked child cached. Rebuild the subtree as well,
          // otherwise this supposedly full pass can still read masked nulls as actual input.
          transformer.tryEvaluateWithFreshInputs();
        }
        filterResultColumns.add(transformer.getColumn());
      }
    }
    final ColumnBuilder[] columnBuilders = filterTsBlockBuilder.getValueColumnBuilders();

    final int rowCount =
        constructFilteredTsBlock(
            filterResultColumns, curFilterColumn, columnBuilders, deviceSchemaBatch.size());
    filterTsBlockBuilder.declarePositions(rowCount);

    return filterTsBlockBuilder.build(new TimeColumn(rowCount, new long[rowCount]));
  }

  @Override
  public void close() throws MetadataException {
    super.close();
    projectOutputTransformerList.forEach(ColumnTransformer::close);
  }
}
