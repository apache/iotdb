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

package org.apache.iotdb.db.subscription.tagfilter;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Filters rows of a table-model Tablet according to TAG column values. */
public class TabletTagFilter {

  private static final String DELIMITED_FIELD_PREFIX = "D:";
  private static final String UNDELIMITED_FIELD_PREFIX = "U:";

  private TabletTagFilter() {
    // utility class
  }

  public static Tablet filter(final Tablet tablet, final TagFilterMatcher matcher) {
    return filter(tablet, matcher, null);
  }

  public static Tablet filter(
      final Tablet tablet, final TagFilterMatcher matcher, final String databaseName) {
    if (Objects.isNull(tablet)) {
      return null;
    }

    final TagFilterMatcher effectiveMatcher =
        Objects.nonNull(matcher) ? matcher : TagFilterMatcher.matchAll();
    effectiveMatcher.throwIfFailure();
    if (effectiveMatcher.isMatchAll()) {
      return tablet;
    }
    if (effectiveMatcher.isMatchNone() || tablet.getRowSize() <= 0) {
      return null;
    }

    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    // Upstream event conversion supplies a structurally valid Tablet. Only check the metadata
    // needed to bind the predicate here; do not rescan every value array and null bitmap.
    final List<ColumnCategory> categories = getColumnCategories(tablet, schemas.size());
    validateBinding(effectiveMatcher, databaseName, tablet.getTableName());
    final Map<String, Integer> resolvedFields =
        resolveReferencedTagColumns(effectiveMatcher.getReferencedFields(), schemas, categories);

    final int[] selectedRows = new int[tablet.getRowSize()];
    int selectedRowCount = 0;
    try {
      for (int rowIndex = 0; rowIndex < tablet.getRowSize(); rowIndex++) {
        final int currentRowIndex = rowIndex;
        if (effectiveMatcher.matches(
            identifier ->
                getTagValue(tablet, currentRowIndex, resolvedFields.get(toFieldKey(identifier))))) {
          selectedRows[selectedRowCount++] = rowIndex;
        }
      }
    } catch (final RuntimeException e) {
      if (e instanceof TagFilterEvaluationException) {
        throw e;
      }
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_FAILED_TO_EVALUATE_A_TABLET_ROW_7E4E94CE, e);
    }

    if (selectedRowCount == 0) {
      return null;
    }
    if (selectedRowCount == tablet.getRowSize()) {
      return tablet;
    }
    if (selectedRows[selectedRowCount - 1] == selectedRowCount - 1) {
      // A prefix needs no row compaction. Clone the outer arrays because the TsFile sorter may
      // replace their entries; the column arrays and bitmaps themselves can be shared.
      return new Tablet(
          tablet.getTableName(),
          schemas,
          categories,
          tablet.getTimestamps(),
          tablet.getValues().clone(),
          tablet.getBitMaps() == null ? null : tablet.getBitMaps().clone(),
          selectedRowCount);
    }
    return copySelectedRows(tablet, schemas, categories, selectedRows, selectedRowCount);
  }

  private static void validateBinding(
      final TagFilterMatcher matcher, final String databaseName, final String tableName) {
    if (!matcher.isBindingEnforced()) {
      return;
    }
    final TagFilterMatcher.TableBinding binding =
        matcher.getTableBindings().get(TagFilterMatcher.TableKey.of(databaseName, tableName));
    if (binding == null) {
      throw TagFilterEvaluationException.schemaBinding(
          DataNodeMiscMessages.EXCEPTION_TABLE_BINDING_IS_NOT_AVAILABLE_882C5F2F);
    }
    if (binding.isFailed()) {
      throw TagFilterEvaluationException.schemaBinding(binding.getFailureReason());
    }
    for (final Identifier identifier : matcher.getReferencedFields()) {
      final String normalizedName = identifier.getValue().toLowerCase(Locale.ROOT);
      if (!binding.getTagNames().contains(normalizedName)) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_REFERENCED_COLUMN_IS_NOT_A_TAG_COLUMN_33FA34BC);
      }
    }
  }

  private static Map<String, Integer> resolveReferencedTagColumns(
      final List<Identifier> referencedFields,
      final List<IMeasurementSchema> schemas,
      final List<ColumnCategory> categories) {
    final Map<String, Integer> exactColumns = new HashMap<>();
    final Map<String, Integer> normalizedColumns = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      final IMeasurementSchema schema = schemas.get(i);
      exactColumns.put(schema.getMeasurementName(), i);
      normalizedColumns.put(schema.getMeasurementName().toLowerCase(Locale.ROOT), i);
    }

    final Map<String, Integer> result = new HashMap<>();
    for (final Identifier identifier : referencedFields) {
      final Integer columnIndex =
          identifier.isDelimited()
              ? exactColumns.get(identifier.getValue())
              : normalizedColumns.get(identifier.getValue().toLowerCase(Locale.ROOT));
      if (Objects.isNull(columnIndex)) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_REFERENCED_TAG_COLUMN_IS_MISSING_55E07377);
      }
      if (categories.get(columnIndex) != ColumnCategory.TAG) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_REFERENCED_COLUMN_IS_NOT_A_TAG_COLUMN_33FA34BC);
      }
      result.put(toFieldKey(identifier), columnIndex);
    }
    return result;
  }

  private static String getTagValue(
      final Tablet tablet, final int rowIndex, final Integer columnIndex) {
    if (Objects.isNull(columnIndex) || tablet.isNull(rowIndex, columnIndex)) {
      return null;
    }
    final Object value = tablet.getValue(rowIndex, columnIndex);
    if (value instanceof Binary) {
      return ((Binary) value).getStringValue(TSFileConfig.STRING_CHARSET);
    }
    return Objects.nonNull(value) ? String.valueOf(value) : null;
  }

  private static Tablet copySelectedRows(
      final Tablet tablet,
      final List<IMeasurementSchema> schemas,
      final List<ColumnCategory> categories,
      final int[] selectedRows,
      final int selectedRowCount) {
    try {
      final long[] timestamps = new long[selectedRowCount];
      for (int targetRow = 0; targetRow < selectedRowCount; targetRow++) {
        timestamps[targetRow] = tablet.getTimestamp(selectedRows[targetRow]);
      }
      final Object[] sourceValues = tablet.getValues();
      final Object[] values = new Object[schemas.size()];
      final BitMap[] sourceBitMaps = tablet.getBitMaps();
      final BitMap[] bitMaps = sourceBitMaps == null ? null : new BitMap[schemas.size()];
      for (int columnIndex = 0; columnIndex < schemas.size(); columnIndex++) {
        final Object sourceColumn = sourceValues[columnIndex];
        if (sourceColumn != null) {
          values[columnIndex] =
              Array.newInstance(sourceColumn.getClass().getComponentType(), selectedRowCount);
          for (int targetRow = 0; targetRow < selectedRowCount; targetRow++) {
            System.arraycopy(
                sourceColumn, selectedRows[targetRow], values[columnIndex], targetRow, 1);
          }
        }
        if (sourceBitMaps != null && sourceBitMaps[columnIndex] != null) {
          bitMaps[columnIndex] = new BitMap(selectedRowCount);
          for (int targetRow = 0; targetRow < selectedRowCount; targetRow++) {
            if (sourceBitMaps[columnIndex].isMarked(selectedRows[targetRow])) {
              bitMaps[columnIndex].mark(targetRow);
            }
          }
        }
      }
      // Filtering changes rows only; schema descriptors and column categories stay valid.
      return new Tablet(
          tablet.getTableName(),
          schemas,
          categories,
          timestamps,
          values,
          bitMaps,
          selectedRowCount);
    } catch (final RuntimeException e) {
      if (e instanceof TagFilterEvaluationException) {
        throw e;
      }
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_FAILED_TO_COMPACT_FILTERED_TABLET_5D4AD8AA, e);
    }
  }

  private static List<ColumnCategory> getColumnCategories(
      final Tablet tablet, final int columnCount) {
    final List<ColumnCategory> categories = tablet.getColumnTypes();
    if (Objects.isNull(categories) || categories.size() < columnCount) {
      throw TagFilterEvaluationException.schemaBinding(
          DataNodeMiscMessages.EXCEPTION_TABLET_COLUMN_CATEGORIES_ARE_MISSING_2C660532);
    }
    return categories;
  }

  private static String toFieldKey(final Identifier identifier) {
    return (identifier.isDelimited() ? DELIMITED_FIELD_PREFIX : UNDELIMITED_FIELD_PREFIX)
        + (identifier.isDelimited()
            ? identifier.getValue()
            : identifier.getValue().toLowerCase(Locale.ROOT));
  }
}
