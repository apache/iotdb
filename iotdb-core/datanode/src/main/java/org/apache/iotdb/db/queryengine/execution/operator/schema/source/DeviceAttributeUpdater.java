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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator.constructFilteredTsBlock;
import static org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator.satisfy;
import static org.apache.iotdb.db.queryengine.execution.operator.schema.source.TableDeviceQuerySource.transformToTsBlockColumns;

public class DeviceAttributeUpdater extends DevicePredicateFilter {
  private final List<LeafColumnTransformer> projectLeafColumnTransformerList;
  private final List<ColumnTransformer> projectOutputTransformerList;
  final BiFunction<Integer, String, String> attributeProvider;
  private final BiConsumer<Integer, Object[]> attributeUpdater;
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
      final BiConsumer<Integer, Object[]> attributeUpdater) {
    super(
        filterOutputDataTypes,
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        commonTransformerList,
        database,
        tableName,
        columnHeaderList);
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.attributeProvider = attributeProvider;
    this.attributeUpdater = attributeUpdater;
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
    if (addBatch(result)) {
      update();
    }
  }

  private void update() {
    final TsBlock block = getTsBlock();
    if (Objects.isNull(block)) {
      return;
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

    for (int i = 0; i < indexes.size(); ++i) {
      final int finalI = i;
      attributeUpdater.accept(
          attributePointers.get(indexes.get(i)),
          resultColumns.stream().map(column -> column.getObject(finalI)).toArray(Object[]::new));
    }

    indexes.clear();
    attributePointers.clear();
    super.clear();
  }

  public TsBlock getTsBlock() {
    final TsBlockBuilder builder = new TsBlockBuilder(filterOutputDataTypes);
    deviceSchemaBatch.forEach(
        deviceSchemaInfo ->
            transformToTsBlockColumns(
                deviceSchemaInfo, builder, database, tableName, columnHeaderList, 3));

    final TsBlock tsBlock = builder.build();
    if (Objects.isNull(filterOutputTransformer)) {
      return tsBlock;
    }

    // feed Filter ColumnTransformer, including TimeStampColumnTransformer and constant
    filterLeafColumnTransformerList.forEach(
        leafColumnTransformer -> leafColumnTransformer.initFromTsBlock(tsBlock));
    filterOutputTransformer.tryEvaluate();
    final Column filterColumn = filterOutputTransformer.getColumn();

    // reuse this builder
    filterTsBlockBuilder.reset();

    final List<Column> resultColumns = Arrays.asList(tsBlock.getValueColumns());

    // get result of calculated common sub expressions
    commonTransformerList.forEach(
        columnTransformer -> resultColumns.add(columnTransformer.getColumn()));
    final ColumnBuilder[] columnBuilders = filterTsBlockBuilder.getValueColumnBuilders();

    filterTsBlockBuilder.declarePositions(
        constructFilteredTsBlock(
            resultColumns, filterColumn, columnBuilders, deviceSchemaBatch.size()));

    for (int j = 0; j < deviceSchemaBatch.size(); j++) {
      if (satisfy(filterColumn, j)) {
        indexes.add(j);
      }
    }

    return filterTsBlockBuilder.build();
  }

  @Override
  public void close() {
    update();
    super.close();
    projectOutputTransformerList.forEach(ColumnTransformer::close);
  }
}
