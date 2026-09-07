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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.plan.planner.DataNodeTableOperatorGenerator;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Projects tree-model tablets captured for a tree-view table into table-model view tablets. */
public class TreeViewTabletProjector {

  private final String databaseName;
  private final TsTable viewTable;
  private final TreePattern treePattern;
  private final IDeviceID.TreeDeviceIdColumnValueExtractor tagValueExtractor;

  public TreeViewTabletProjector(final String databaseName, final TsTable viewTable) {
    this.databaseName = databaseName;
    this.viewTable = viewTable;
    this.treePattern = new IoTDBTreePattern(TreeViewSchema.getPrefixPattern(viewTable).toString());
    this.tagValueExtractor =
        DataNodeTableOperatorGenerator.createTreeDeviceIdColumnValueExtractor(
            DataNodeTreeViewSchemaUtils.getPrefixPath(viewTable));
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Tablet project(final Tablet treeTablet) {
    if (Objects.isNull(treeTablet)
        || Objects.isNull(treeTablet.getDeviceId())
        || !treeTablet.getDeviceId().startsWith("root.")
        || Objects.isNull(treeTablet.getValues())) {
      return null;
    }

    final IDeviceID deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(treeTablet.getDeviceId());
    if (!treePattern.mayOverlapWithDevice(deviceId)) {
      return null;
    }

    final List<IMeasurementSchema> projectedSchemas = new ArrayList<>();
    final List<ColumnCategory> projectedCategories = new ArrayList<>();
    final List<Object> projectedValues = new ArrayList<>();
    final List<BitMap> projectedBitMaps = new ArrayList<>();
    final BitMap[] sourceBitMaps = treeTablet.getBitMaps();
    int tagColumnIndex = 0;

    for (final TsTableColumnSchema columnSchema : viewTable.getColumnList()) {
      if (Objects.isNull(columnSchema)
          || columnSchema.getColumnCategory() == TsTableColumnCategory.TIME
          || columnSchema.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE) {
        continue;
      }

      if (columnSchema.getColumnCategory() == TsTableColumnCategory.TAG) {
        final Pair<Binary[], BitMap> tagColumn =
            buildTagColumn(deviceId, tagColumnIndex++, treeTablet.getRowSize());
        projectedSchemas.add(
            new MeasurementSchema(columnSchema.getColumnName(), columnSchema.getDataType()));
        projectedCategories.add(ColumnCategory.TAG);
        projectedValues.add(tagColumn.left);
        projectedBitMaps.add(tagColumn.right);
        continue;
      }

      final int sourceColumnIndex =
          findSourceColumnIndex(
              treeTablet.getSchemas(), TreeViewSchema.getSourceName(columnSchema));
      if (sourceColumnIndex < 0 || sourceColumnIndex >= treeTablet.getValues().length) {
        continue;
      }
      projectedSchemas.add(
          new MeasurementSchema(
              TreeViewSchema.getSourceName(columnSchema), columnSchema.getDataType()));
      projectedCategories.add(ColumnCategory.FIELD);
      projectedValues.add(treeTablet.getValues()[sourceColumnIndex]);
      projectedBitMaps.add(
          Objects.nonNull(sourceBitMaps) && sourceColumnIndex < sourceBitMaps.length
              ? sourceBitMaps[sourceColumnIndex]
              : null);
    }

    if (projectedSchemas.isEmpty()) {
      return null;
    }

    return new Tablet(
        viewTable.getTableName(),
        projectedSchemas,
        projectedCategories,
        treeTablet.getTimestamps(),
        projectedValues.toArray(new Object[0]),
        projectedBitMaps.stream().anyMatch(Objects::nonNull)
            ? projectedBitMaps.toArray(new BitMap[0])
            : null,
        treeTablet.getRowSize());
  }

  private Pair<Binary[], BitMap> buildTagColumn(
      final IDeviceID deviceId, final int tagColumnIndex, final int rowSize) {
    final Binary[] values = new Binary[rowSize];
    final Object tagValue = tagValueExtractor.extract(deviceId, tagColumnIndex);
    final Binary binaryValue =
        Objects.nonNull(tagValue)
            ? new Binary(tagValue.toString(), TSFileConfig.STRING_CHARSET)
            : Binary.EMPTY_VALUE;
    final BitMap bitMap = Objects.isNull(tagValue) ? new BitMap(rowSize) : null;
    for (int row = 0; row < rowSize; row++) {
      values[row] = binaryValue;
      if (Objects.nonNull(bitMap)) {
        bitMap.mark(row);
      }
    }
    return new Pair<>(values, bitMap);
  }

  private static int findSourceColumnIndex(
      final List<IMeasurementSchema> schemas, final String sourceName) {
    if (Objects.isNull(schemas) || Objects.isNull(sourceName)) {
      return -1;
    }
    for (int i = 0; i < schemas.size(); i++) {
      final IMeasurementSchema schema = schemas.get(i);
      if (Objects.nonNull(schema) && sourceName.equals(schema.getMeasurementName())) {
        return i;
      }
    }
    return -1;
  }
}
