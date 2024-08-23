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

import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;

import org.apache.ratis.util.function.TriConsumer;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator.constructFilteredTsBlock;

public class DeviceAttributeUpdater extends DevicePredicateHandler {
  private final List<ColumnTransformer> commonTransformerList;
  private final List<LeafColumnTransformer> projectLeafColumnTransformerList;
  private final List<ColumnTransformer> projectOutputTransformerList;
  final BiFunction<Integer, String, String> attributeProvider;
  private final TriConsumer<String[], Integer, Object[]> attributeUpdater;
  private final TsBlockBuilder filterTsBlockBuilder;
  private final List<Integer> attributePointers = new ArrayList<>();

  @SuppressWarnings("squid:S107")
  public DeviceAttributeUpdater(
      final List<TSDataType> filterOutputDataTypes,
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final List<ColumnTransformer> commonTransformerList,
      final String database,
      final String tableName,
      final List<ColumnHeader> columnHeaderList,
      final List<LeafColumnTransformer> projectLeafColumnTransformerList,
      final List<ColumnTransformer> projectOutputTransformerList,
      final BiFunction<Integer, String, String> attributeProvider,
      final TriConsumer<String[], Integer, Object[]> attributeUpdater) {
    super(
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        database,
        tableName,
        columnHeaderList);
    this.commonTransformerList = commonTransformerList;
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.attributeProvider = attributeProvider;
    this.attributeUpdater = attributeUpdater;
    this.filterTsBlockBuilder = new TsBlockBuilder(filterOutputDataTypes);
  }

  public void handleDeviceNode(final IDeviceMNode<IMemMNode> node) {
    final ShowDevicesResult result =
        new ShowDevicesResult(
            null,
            node.isAlignedNullable(),
            node.getSchemaTemplateId(),
            node.getPartialPath().getNodes());
    final int pointer = ((TableDeviceInfo<IMemMNode>) node.getDeviceInfo()).getAttributePointer();
    result.setAttributeProvider(k -> attributeProvider.apply(pointer, k));
    attributePointers.add(pointer);
    addBatch(result);
    if (hasComputedResult()) {
      update();
    }
  }

  private void update() {
    final TsBlock block = getFilterTsBlock();

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
      final int finalI = i;
      final String[] nodes =
          deviceSchemaBatch.get(withoutFilter() ? i : indexes.get(i)).getRawNodes();
      attributeUpdater.accept(
          Arrays.copyOfRange(nodes, 3, nodes.length),
          attributePointers.get(withoutFilter() ? i : indexes.get(i)),
          resultColumns.stream().map(column -> column.getObject(finalI)).toArray(Object[]::new));
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

    // Get result of calculated common sub expressions
    commonTransformerList.forEach(
        columnTransformer -> filterResultColumns.add(columnTransformer.getColumn()));
    final ColumnBuilder[] columnBuilders = filterTsBlockBuilder.getValueColumnBuilders();

    final int rowCount =
        constructFilteredTsBlock(
            filterResultColumns, curFilterColumn, columnBuilders, deviceSchemaBatch.size());
    filterTsBlockBuilder.declarePositions(rowCount);

    return filterTsBlockBuilder.build(new TimeColumn(rowCount, new long[rowCount]));
  }

  @Override
  public void close() {
    prepareBatchResult();
    if (hasComputedResult()) {
      update();
    }
    super.close();
    projectOutputTransformerList.forEach(ColumnTransformer::close);
  }
}
