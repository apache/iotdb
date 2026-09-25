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
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
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
    final Object[] values = tablet.getValues();
    if (Objects.isNull(schemas) || schemas.isEmpty()) {
      throw TagFilterEvaluationException.schemaBinding(
          DataNodeMiscMessages.EXCEPTION_TABLET_SCHEMA_IS_MISSING_164075B0);
    }
    if (Objects.isNull(values) || values.length < schemas.size()) {
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_TABLET_VALUE_COLUMNS_ARE_INCOMPLETE_DCD09F3C);
    }

    final List<ColumnCategory> categories = getColumnCategories(tablet, schemas.size());
    validateTabletStructure(tablet, schemas, values, categories);
    validateBinding(effectiveMatcher, databaseName, tablet.getTableName(), schemas, categories);
    final Map<String, Integer> resolvedFields =
        resolveReferencedTagColumns(effectiveMatcher.getReferencedFields(), schemas, categories);

    final List<Integer> selectedRows = new ArrayList<>();
    try {
      for (int rowIndex = 0; rowIndex < tablet.getRowSize(); rowIndex++) {
        final int currentRowIndex = rowIndex;
        if (effectiveMatcher.matches(
            identifier ->
                getTagValue(tablet, currentRowIndex, resolvedFields.get(toFieldKey(identifier))))) {
          selectedRows.add(rowIndex);
        }
      }
    } catch (final RuntimeException e) {
      if (e instanceof TagFilterEvaluationException) {
        throw e;
      }
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_FAILED_TO_EVALUATE_A_TABLET_ROW_7E4E94CE, e);
    }

    if (selectedRows.isEmpty()) {
      return null;
    }
    if (selectedRows.size() == tablet.getRowSize()) {
      return tablet;
    }
    return copySelectedRows(tablet, schemas, categories, selectedRows);
  }

  private static void validateBinding(
      final TagFilterMatcher matcher,
      final String databaseName,
      final String tableName,
      final List<IMeasurementSchema> schemas,
      final List<ColumnCategory> categories) {
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
    // A runtime tablet must carry category metadata consistent with the binding. Missing or
    // changed categories are structural/schema failures, never SQL NULL values.
    for (int i = 0; i < schemas.size(); i++) {
      final String name = schemas.get(i).getMeasurementName();
      if (binding.getAllNames().contains(name.toLowerCase(Locale.ROOT))
          && categories.get(i) != ColumnCategory.TAG
          && matcher.getReferencedFields().stream()
              .anyMatch(identifier -> identifier.getValue().equalsIgnoreCase(name))) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_REFERENCED_COLUMN_CATEGORY_CHANGED_564594D8);
      }
    }
  }

  private static Map<String, Integer> resolveReferencedTagColumns(
      final List<Identifier> referencedFields,
      final List<IMeasurementSchema> schemas,
      final List<ColumnCategory> categories) {
    final Map<String, Integer> exactTagColumns = new HashMap<>();
    final Map<String, Integer> normalizedTagColumns = new HashMap<>();
    final Map<String, ColumnCategory> exactColumns = new HashMap<>();
    final Map<String, ColumnCategory> normalizedColumns = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      final IMeasurementSchema schema = schemas.get(i);
      if (Objects.isNull(schema) || Objects.isNull(schema.getMeasurementName())) {
        continue;
      }
      exactColumns.put(schema.getMeasurementName(), categories.get(i));
      normalizedColumns.put(
          schema.getMeasurementName().toLowerCase(Locale.ROOT), categories.get(i));
      if (categories.get(i) != ColumnCategory.TAG) {
        continue;
      }
      exactTagColumns.put(schema.getMeasurementName(), i);
      normalizedTagColumns.put(schema.getMeasurementName().toLowerCase(Locale.ROOT), i);
    }

    final Map<String, Integer> result = new HashMap<>();
    for (final Identifier identifier : referencedFields) {
      final Integer columnIndex =
          identifier.isDelimited()
              ? exactTagColumns.get(identifier.getValue())
              : normalizedTagColumns.get(identifier.getValue().toLowerCase(Locale.ROOT));
      if (Objects.isNull(columnIndex)) {
        final ColumnCategory category =
            identifier.isDelimited()
                ? exactColumns.get(identifier.getValue())
                : normalizedColumns.get(identifier.getValue().toLowerCase(Locale.ROOT));
        if (category != null && category != ColumnCategory.TAG) {
          throw TagFilterEvaluationException.schemaBinding(
              DataNodeMiscMessages.EXCEPTION_REFERENCED_COLUMN_IS_NOT_A_TAG_COLUMN_33FA34BC);
        }
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_REFERENCED_TAG_COLUMN_IS_MISSING_55E07377);
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
      final List<Integer> selectedRows) {
    final List<String> columnNames = new ArrayList<>(schemas.size());
    final List<TSDataType> dataTypes = new ArrayList<>(schemas.size());
    for (final IMeasurementSchema schema : schemas) {
      if (Objects.isNull(schema)
          || Objects.isNull(schema.getMeasurementName())
          || Objects.isNull(schema.getType())) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_TABLET_MEASUREMENT_SCHEMA_IS_INCOMPLETE_6A813472);
      }
      columnNames.add(schema.getMeasurementName());
      dataTypes.add(schema.getType());
    }

    final Tablet result =
        new Tablet(tablet.getTableName(), columnNames, dataTypes, categories, selectedRows.size());
    try {
      for (int targetRow = 0; targetRow < selectedRows.size(); targetRow++) {
        final int sourceRow = selectedRows.get(targetRow);
        PipeTabletUtils.putTimestamp(result, targetRow, tablet.getTimestamp(sourceRow));
        for (int columnIndex = 0; columnIndex < schemas.size(); columnIndex++) {
          if (tablet.isNull(sourceRow, columnIndex)) {
            PipeTabletUtils.markNullValue(result, targetRow, columnIndex);
          } else {
            final Object value = tablet.getValue(sourceRow, columnIndex);
            if (Objects.isNull(value)) {
              PipeTabletUtils.markNullValue(result, targetRow, columnIndex);
            } else {
              PipeTabletUtils.putValue(
                  result, targetRow, columnIndex, dataTypes.get(columnIndex), value);
            }
          }
        }
      }
      return result;
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
    final List<ColumnCategory> result = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      if (Objects.isNull(categories.get(i))) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_TABLET_COLUMN_CATEGORY_IS_MISSING_A812B500);
      }
      result.add(categories.get(i));
    }
    return result;
  }

  private static void validateTabletStructure(
      final Tablet tablet,
      final List<IMeasurementSchema> schemas,
      final Object[] values,
      final List<ColumnCategory> categories) {
    if (tablet.getTimestamps() == null || tablet.getTimestamps().length < tablet.getRowSize()) {
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_TABLET_TIMESTAMPS_ARE_INCOMPLETE_24F8CE6F);
    }
    final org.apache.tsfile.utils.BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null && bitMaps.length < schemas.size()) {
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_TABLET_BITMAPS_ARE_INCOMPLETE_7BBC8035);
    }
    for (int i = 0; i < schemas.size(); i++) {
      final IMeasurementSchema schema = schemas.get(i);
      if (schema == null || schema.getMeasurementName() == null || schema.getType() == null) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_TABLET_MEASUREMENT_SCHEMA_IS_INCOMPLETE_6A813472);
      }
      if (values[i] == null) {
        // A column whose every row is NULL may legitimately omit its value array and represent
        // the values solely through a bitmap.  It still has a complete schema and must remain
        // distinguishable from a missing or malformed column.
        if (!isEntirelyNull(bitMaps, i, tablet.getRowSize())) {
          throw new TagFilterEvaluationException(
              DataNodeMiscMessages.EXCEPTION_TABLET_VALUE_COLUMN_IS_INCOMPLETE_845721FE);
        }
        continue;
      }
      if (!hasArrayLength(values[i], tablet.getRowSize())) {
        throw new TagFilterEvaluationException(
            DataNodeMiscMessages.EXCEPTION_TABLET_VALUE_COLUMN_IS_INCOMPLETE_845721FE);
      }
      if (categories.get(i) == null) {
        throw TagFilterEvaluationException.schemaBinding(
            DataNodeMiscMessages.EXCEPTION_TABLET_COLUMN_CATEGORY_IS_MISSING_A812B500);
      }
    }
  }

  private static boolean hasArrayLength(final Object value, final int rowSize) {
    if (value instanceof boolean[]) {
      return ((boolean[]) value).length >= rowSize;
    }
    if (value instanceof int[]) {
      return ((int[]) value).length >= rowSize;
    }
    if (value instanceof long[]) {
      return ((long[]) value).length >= rowSize;
    }
    if (value instanceof float[]) {
      return ((float[]) value).length >= rowSize;
    }
    if (value instanceof double[]) {
      return ((double[]) value).length >= rowSize;
    }
    if (value instanceof Binary[]) {
      return ((Binary[]) value).length >= rowSize;
    }
    if (value instanceof java.time.LocalDate[]) {
      return ((java.time.LocalDate[]) value).length >= rowSize;
    }
    return false;
  }

  private static boolean isEntirelyNull(
      final org.apache.tsfile.utils.BitMap[] bitMaps, final int columnIndex, final int rowSize) {
    if (bitMaps == null || columnIndex >= bitMaps.length || bitMaps[columnIndex] == null) {
      return false;
    }
    for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
      if (!bitMaps[columnIndex].isMarked(rowIndex)) {
        return false;
      }
    }
    return true;
  }

  private static String toFieldKey(final Identifier identifier) {
    return (identifier.isDelimited() ? DELIMITED_FIELD_PREFIX : UNDELIMITED_FIELD_PREFIX)
        + (identifier.isDelimited()
            ? identifier.getValue()
            : identifier.getValue().toLowerCase(Locale.ROOT));
  }
}
