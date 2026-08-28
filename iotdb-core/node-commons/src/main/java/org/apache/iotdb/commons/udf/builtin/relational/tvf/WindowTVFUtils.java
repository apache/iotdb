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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.exception.UDFColumnNotFoundException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFTypeMismatchException;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class WindowTVFUtils {

  private static final Set<Type> ALLOWED_CALCULATION_TYPES =
      Set.of(Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);

  private static final Set<Type> SUPPORTED_PARTITION_TYPES =
      new HashSet<>(
          Arrays.asList(
              Type.BOOLEAN,
              Type.INT32,
              Type.INT64,
              Type.FLOAT,
              Type.DOUBLE,
              Type.TEXT,
              Type.TIMESTAMP,
              Type.DATE,
              Type.BLOB,
              Type.STRING));

  /**
   * Find the index of the column in the table argument.
   *
   * @param tableArgument the table argument
   * @param expectedFieldName the expected field name
   * @param expectedTypes the expected types
   * @return the index of the time column, -1 if not found
   */
  public static int findColumnIndex(
      TableArgument tableArgument, String expectedFieldName, Set<Type> expectedTypes)
      throws UDFException {
    for (int i = 0; i < tableArgument.getFieldTypes().size(); i++) {
      Optional<String> fieldName = tableArgument.getFieldNames().get(i);
      if (fieldName.isPresent() && expectedFieldName.equalsIgnoreCase(fieldName.get())) {
        if (!expectedTypes.contains(tableArgument.getFieldTypes().get(i))) {
          throw new UDFTypeMismatchException(
              String.format(
                  CommonMessages.EXCEPTION_TYPE_COLUMN_ARG_NOT_AS_EXPECTED_7A81636E,
                  expectedFieldName));
        }
        return i;
      }
    }
    throw new UDFColumnNotFoundException(
        String.format(
            CommonMessages.EXCEPTION_REQUIRED_COLUMN_ARG_NOT_FOUND_SOURCE_TABLE_ARGUMENT_993E1C08,
            expectedFieldName));
  }

  public static void validateOrderBy(TableArgument tableArgument, String timeColumn) {
    if (tableArgument.getOrderBy().size() != 1
        || !tableArgument.getOrderBy().get(0).equalsIgnoreCase(timeColumn)) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_CONTAIN_EXACTLY_THE_TIME_COLUMN_SPECIFIED_BY_THE_TIMECOL_ARGUMENT_4375BAE9);
    }
  }

  public static List<Integer> getPartitionIndexes(TableArgument tableArgument) {
    List<Integer> indexes = new ArrayList<>();
    for (String partitionColumn : tableArgument.getPartitionBy()) {
      indexes.add(findColumnIndex(tableArgument, partitionColumn, SUPPORTED_PARTITION_TYPES));
    }
    return indexes;
  }

  /**
   * Collect calculation-column indexes after excluding partition and time columns.
   *
   * <p>If {@code calculationColumnConsumer} is provided, it is invoked with each calculation column
   * name so the caller can append the corresponding result field to its output schema.
   */
  public static List<Integer> getCalculationIndexes(
      TableArgument tableArgument,
      Set<Integer> excludedIndexes,
      Consumer<String> calculationColumnConsumer) {
    List<Integer> calculationIndexes = new ArrayList<>();
    for (int i = 0; i < tableArgument.getFieldTypes().size(); i++) {
      if (excludedIndexes.contains(i)) {
        continue;
      }

      Type type = tableArgument.getFieldTypes().get(i);
      String columnName = tableArgument.getFieldNames().get(i).get();
      if (!ALLOWED_CALCULATION_TYPES.contains(type)) {
        throw new SemanticException(
            String.format(CommonMessages.EXCEPTION_NOT_ALLOWED_COLUMNS, columnName, type));
      }

      calculationIndexes.add(i);
      if (calculationColumnConsumer != null) {
        calculationColumnConsumer.accept(columnName);
      }
    }
    return calculationIndexes;
  }

  public static String joinTypes(List<Type> types) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < types.size(); i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(types.get(i).name());
    }
    return builder.toString();
  }

  /** check the order by column is the timeColumn */
  public static int checkOrderByColumn(
      Map<String, Argument> arguments, String dataParameterName, String timeParameterName) {
    TableArgument tableArgument = (TableArgument) arguments.get(dataParameterName);
    if (tableArgument.getOrderBy().isEmpty()) {
      throw new SemanticException(
          CommonMessages
              .EXCEPTION_TABLE_ARGUMENT_WITH_SET_SEMANTICS_REQUIRES_AN_ORDER_BY_CLAUSE_10C986D9);
    }

    String timeColumn = (String) ((ScalarArgument) arguments.get(timeParameterName)).getValue();
    int timeColumnIndex =
        findColumnIndex(tableArgument, timeColumn, Collections.singleton(Type.TIMESTAMP));
    WindowTVFUtils.validateOrderBy(tableArgument, timeColumn);
    return timeColumnIndex;
  }

  public static Type[] parseTypes(String value) {
    if (value.isEmpty()) {
      return new Type[0];
    }
    String[] values = value.split(",");
    Type[] types = new Type[values.length];
    for (int i = 0; i < values.length; i++) {
      types[i] = Type.valueOf(values[i]);
    }
    return types;
  }

  public static Object readValue(Record input, int columnIndex, Type partitionType) {
    switch (partitionType) {
      case BOOLEAN:
        return input.getBoolean(columnIndex);
      case INT32:
        return input.getInt(columnIndex);
      case INT64:
      case TIMESTAMP:
        return input.getLong(columnIndex);
      case FLOAT:
        return input.getFloat(columnIndex);
      case DOUBLE:
        return input.getDouble(columnIndex);
      case TEXT:
      case STRING:
      case BLOB:
        return input.getBinary(columnIndex);
      case DATE:
        return input.getLocalDate(columnIndex);
      default:
        throw new IllegalArgumentException(String.valueOf(partitionType));
    }
  }

  public static void writeValue(ColumnBuilder builder, Object value, Type type) {
    if (value == null) {
      builder.appendNull();
      return;
    }

    switch (type) {
      case BOOLEAN:
        builder.writeBoolean((Boolean) value);
        break;
      case INT32:
        builder.writeInt((Integer) value);
        break;
      case INT64:
      case TIMESTAMP:
        builder.writeLong((Long) value);
        break;
      case FLOAT:
        builder.writeFloat((Float) value);
        break;
      case DOUBLE:
        builder.writeDouble((Double) value);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        builder.writeBinary((org.apache.tsfile.utils.Binary) value);
        break;
      case DATE:
        builder.writeObject(value);
        break;
      default:
        throw new IllegalArgumentException(String.valueOf(type));
    }
  }
}
