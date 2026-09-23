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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Prunes table-model Tablets according to a subscription column matcher. */
public class TabletColumnPruner {

  private TabletColumnPruner() {
    // utility class
  }

  public static Tablet pruneTableModelTablet(
      final Tablet tablet, final String databaseName, final ColumnFilterMatcher matcher) {
    if (Objects.isNull(tablet) || Objects.isNull(databaseName)) {
      return tablet;
    }

    final ColumnFilterMatcher effectiveMatcher =
        Objects.nonNull(matcher) ? matcher : ColumnFilterMatcher.matchAll();
    if (effectiveMatcher.isMatchAll()) {
      return tablet;
    }
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.getValues();
    if (Objects.isNull(schemas) || schemas.isEmpty() || Objects.isNull(values)) {
      return null;
    }
    final boolean timeSelected =
        effectiveMatcher.isTimeSelected(databaseName, tablet.getTableName());

    final List<ColumnCategory> categories = getColumnCategories(tablet, schemas.size());
    final boolean[] selectedColumns = new boolean[schemas.size()];
    boolean hasMatchedColumn = timeSelected;

    for (int i = 0; i < schemas.size(); i++) {
      if (!isValidColumn(schemas, values, i)) {
        continue;
      }
      final IMeasurementSchema schema = schemas.get(i);
      final ColumnCategory category = categories.get(i);
      if (effectiveMatcher.match(
          databaseName,
          tablet.getTableName(),
          schema.getMeasurementName(),
          schema.getType(),
          category)) {
        hasMatchedColumn = true;
        if (category != ColumnCategory.ATTRIBUTE) {
          selectedColumns[i] = true;
        }
      }
    }

    if (!hasMatchedColumn) {
      return null;
    }

    if (effectiveMatcher.shouldAutoRetainTagsAtRuntime()) {
      for (int i = 0; i < schemas.size(); i++) {
        if (isValidColumn(schemas, values, i)
            && categories.get(i) == ColumnCategory.TAG
            && hasValueArray(values, i)) {
          selectedColumns[i] = true;
        }
      }
    }

    final List<Integer> selectedIndices = getSelectedIndices(selectedColumns);
    if (selectedIndices.isEmpty()) {
      return timeSelected ? pruneAllPhysicalColumns(tablet) : null;
    }
    if (selectedIndices.size() == schemas.size()) {
      return tablet;
    }

    final List<IMeasurementSchema> prunedSchemas = new ArrayList<>(selectedIndices.size());
    final List<ColumnCategory> prunedCategories = new ArrayList<>(selectedIndices.size());
    final Object[] prunedValues = new Object[selectedIndices.size()];
    final BitMap[] bitMaps = tablet.getBitMaps();
    final BitMap[] prunedBitMaps =
        Objects.nonNull(bitMaps) ? new BitMap[selectedIndices.size()] : null;

    for (int i = 0; i < selectedIndices.size(); i++) {
      final int originalIndex = selectedIndices.get(i);
      final IMeasurementSchema originalSchema = schemas.get(originalIndex);
      prunedSchemas.add(
          new MeasurementSchema(originalSchema.getMeasurementName(), originalSchema.getType()));
      prunedCategories.add(categories.get(originalIndex));
      prunedValues[i] = values[originalIndex];
      if (Objects.nonNull(bitMaps) && originalIndex < bitMaps.length) {
        prunedBitMaps[i] = bitMaps[originalIndex];
      }
    }

    return new Tablet(
        tablet.getTableName(),
        prunedSchemas,
        prunedCategories,
        tablet.getTimestamps(),
        prunedValues,
        prunedBitMaps,
        tablet.getRowSize());
  }

  private static Tablet pruneAllPhysicalColumns(final Tablet tablet) {
    return new Tablet(
        tablet.getTableName(),
        Collections.emptyList(),
        Collections.emptyList(),
        tablet.getTimestamps(),
        new Object[0],
        null,
        tablet.getRowSize());
  }

  private static List<ColumnCategory> getColumnCategories(
      final Tablet tablet, final int columnCount) {
    final List<ColumnCategory> categories = tablet.getColumnTypes();
    if (Objects.isNull(categories) || categories.isEmpty()) {
      return Collections.nCopies(columnCount, ColumnCategory.FIELD);
    }
    final List<ColumnCategory> result = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      result.add(
          i < categories.size() && Objects.nonNull(categories.get(i))
              ? categories.get(i)
              : ColumnCategory.FIELD);
    }
    return result;
  }

  private static boolean isValidColumn(
      final List<IMeasurementSchema> schemas, final Object[] values, final int index) {
    if (index < 0 || index >= schemas.size() || index >= values.length) {
      return false;
    }
    final IMeasurementSchema schema = schemas.get(index);
    final TSDataType dataType = Objects.nonNull(schema) ? schema.getType() : null;
    return Objects.nonNull(schema)
        && Objects.nonNull(schema.getMeasurementName())
        && Objects.nonNull(dataType);
  }

  private static boolean hasValueArray(final Object[] values, final int index) {
    return Objects.nonNull(values)
        && index >= 0
        && index < values.length
        && Objects.nonNull(values[index]);
  }

  private static List<Integer> getSelectedIndices(final boolean[] selectedColumns) {
    final List<Integer> result = new ArrayList<>(selectedColumns.length);
    for (int i = 0; i < selectedColumns.length; i++) {
      if (selectedColumns[i]) {
        result.add(i);
      }
    }
    return result;
  }
}
