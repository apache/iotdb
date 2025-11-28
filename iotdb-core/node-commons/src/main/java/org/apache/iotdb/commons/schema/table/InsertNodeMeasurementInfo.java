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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import org.apache.tsfile.enums.TSDataType;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * InsertNodeMeasurementInfo is a class that stores the measurement list from InsertNode and table
 * name, and provides methods to convert to TsTable.
 */
public class InsertNodeMeasurementInfo {

  /** Table name */
  private final String tableName;

  /** Column category list (TAG, ATTRIBUTE, FIELD, etc.) */
  private final TsTableColumnCategory[] columnCategories;

  /** Measurement names */
  private final String[] measurements;

  /** Data types */
  private final TSDataType[] dataTypes;

  /** Function to get first value of index for type inference */
  private final Function<Integer, TSDataType> typeInferenceFunction;

  /** Delegate to trigger lower-case conversion on the underlying InsertNode */
  private final Runnable toLowerCaseAction;

  /** Delegate to trigger semantic validation on the underlying InsertNode */
  private final Runnable semanticCheckAction;

  /** Setter to update the ATTRIBUTE column presence flag on InsertNode. */
  private final Consumer<Boolean> attributeColumnsPresentSetter;

  /** Setter to update the lower-case application state on InsertNode. */
  private final Consumer<Boolean> lowerCaseAppliedSetter;

  /** Setter to update the semantic-check state on InsertNode. */
  private final Consumer<Boolean> semanticCheckedSetter;

  /**
   * Constructor with measurements and dataTypes for lazy schema building
   *
   * @param tableName table name
   * @param columnCategories column category list
   * @param measurements measurement names
   * @param dataTypes data types
   * @param firstValueGetter function to get first value of index
   */
  public InsertNodeMeasurementInfo(
      final String tableName,
      final TsTableColumnCategory[] columnCategories,
      final String[] measurements,
      final TSDataType[] dataTypes,
      final Function<Integer, TSDataType> firstValueGetter,
      final Runnable toLowerCaseAction,
      final Runnable semanticCheckAction,
      final Consumer<Boolean> attributeColumnsPresentSetter,
      final Consumer<Boolean> lowerCaseAppliedSetter,
      final Consumer<Boolean> semanticCheckedSetter) {
    this.tableName = tableName.toLowerCase();
    this.columnCategories = columnCategories;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.typeInferenceFunction = firstValueGetter;
    this.toLowerCaseAction = toLowerCaseAction;
    this.semanticCheckAction = semanticCheckAction;
    this.attributeColumnsPresentSetter = attributeColumnsPresentSetter;
    this.lowerCaseAppliedSetter = lowerCaseAppliedSetter;
    this.semanticCheckedSetter = semanticCheckedSetter;
  }

  /**
   * Get table name
   *
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get column category list
   *
   * @return column category array
   */
  public TsTableColumnCategory[] getColumnCategories() {
    return columnCategories;
  }

  /**
   * Get measurements array
   *
   * @return measurements array
   */
  public String[] getMeasurements() {
    return measurements;
  }

  /**
   * Get the count of measurements
   *
   * @return measurement count
   */
  public int getMeasurementCount() {
    if (measurements != null) {
      return measurements.length;
    }
    return 0;
  }

  public TSDataType getType(int index) {
    if (dataTypes == null) {
      return null;
    }
    return dataTypes[index];
  }

  /**
   * Get measurement name at specific index
   *
   * @param index the index
   * @return measurement name or null
   */
  public String getMeasurementName(int index) {
    if (measurements != null && index < measurements.length) {
      return measurements[index];
    }
    return null;
  }

  /**
   * Get measurement schema at specific index (lazily build if needed)
   *
   * @param index the index
   * @return measurement schema or null
   */
  public TSDataType getTypeForFirstValue(int index) {
    return this.typeInferenceFunction.apply(index);
  }

  /**
   * Apply lower-case transformation on the underlying InsertNode.
   *
   * <p>The delegate is optional. If absent, this method is a no-op.
   */
  public void toLowerCase() {
    if (toLowerCaseAction != null) {
      toLowerCaseAction.run();
    }
  }

  /**
   * Perform semantic validation on the underlying InsertNode.
   *
   * <p>The delegate is optional. If absent, this method is a no-op.
   */
  public void semanticCheck() {
    if (semanticCheckAction != null) {
      semanticCheckAction.run();
    }
  }

  /**
   * Update the ATTRIBUTE column presence flag.
   *
   * @param attributeColumnsPresent whether ATTRIBUTE columns should be treated as present
   * @return current instance for chained operations
   */
  public InsertNodeMeasurementInfo setAttributeColumnsPresent(boolean attributeColumnsPresent) {
    if (attributeColumnsPresentSetter != null) {
      attributeColumnsPresentSetter.accept(attributeColumnsPresent);
    }
    return this;
  }

  /**
   * Update the lower-case application flag.
   *
   * @param applied whether lower-case transformation has been applied
   * @return current instance for chained operations
   */
  public InsertNodeMeasurementInfo setToLowerCaseApplied(boolean applied) {
    if (lowerCaseAppliedSetter != null) {
      lowerCaseAppliedSetter.accept(applied);
    }
    return this;
  }

  /**
   * Update the semantic-check flag.
   *
   * @param checked whether semantic check has been completed
   * @return current instance for chained operations
   */
  public InsertNodeMeasurementInfo setSemanticChecked(boolean checked) {
    if (semanticCheckedSetter != null) {
      semanticCheckedSetter.accept(checked);
    }
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InsertNodeMeasurementInfo that = (InsertNodeMeasurementInfo) o;
    return Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }
}
