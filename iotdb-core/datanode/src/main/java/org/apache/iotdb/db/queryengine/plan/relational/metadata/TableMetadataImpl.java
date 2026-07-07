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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils;
import org.apache.iotdb.calc.utils.constant.SqlConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.commons.queryengine.plan.relational.function.TableFunctionFactory;
import org.apache.iotdb.commons.queryengine.plan.relational.function.arithmetic.AdditionResolver;
import org.apache.iotdb.commons.queryengine.plan.relational.function.arithmetic.DivisionResolver;
import org.apache.iotdb.commons.queryengine.plan.relational.function.arithmetic.ModulusResolver;
import org.apache.iotdb.commons.queryengine.plan.relational.function.arithmetic.MultiplicationResolver;
import org.apache.iotdb.commons.queryengine.plan.relational.function.arithmetic.SubtractionResolver;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.commons.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.commons.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.commons.queryengine.plan.udf.TableUDFUtils;
import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTableColumnDisorderException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.function.DataNodeTableBuiltinTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.TableFunction;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isDecimalType;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isNumericType;
import static org.apache.iotdb.calc.transformation.dag.column.FailFunctionColumnTransformer.FAIL_FUNCTION_NAME;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class TableMetadataImpl implements Metadata {

  private final TypeManager typeManager = new InternalTypeManager();

  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();

  private final DataNodeTableCache tableCache = DataNodeTableCache.getInstance();

  @Override
  public TableFunction getTableFunction(String functionName) {
    if (DataNodeTableBuiltinTableFunction.isBuiltInTableFunction(functionName)) {
      return DataNodeTableBuiltinTableFunction.getBuiltinTableFunction(functionName);
    }
    return TableFunctionFactory.getTableFunction(functionName);
  }

  @Override
  public boolean tableExists(final QualifiedObjectName name) {
    return tableCache.getTable(name.getDatabaseName(), name.getObjectName(), false) != null;
  }

  @Override
  public Optional<TableSchema> getTableSchema(
      final SessionInfo session, final QualifiedObjectName name) {
    final String databaseName = name.getDatabaseName();
    final String tableName = name.getObjectName();

    final TsTable table = tableCache.getTable(databaseName, tableName, false);
    if (table == null) {
      return Optional.empty();
    }
    final List<ColumnSchema> columnSchemaList =
        table.getColumnList().stream()
            .map(
                o -> {
                  final ColumnSchema schema =
                      new ColumnSchema(
                          o.getColumnName(),
                          TypeFactory.getType(o.getDataType()),
                          false,
                          o.getColumnCategory());
                  schema.setProps(o.getProps());
                  return schema;
                })
            .collect(Collectors.toList());
    return Optional.of(
        TreeViewSchema.isTreeViewTable(table)
            ? new TreeDeviceViewSchema(table.getTableName(), columnSchemaList, table.getProps())
            : new TableSchema(table.getTableName(), columnSchemaList));
  }

  @Override
  public Type getOperatorReturnType(OperatorType operatorType, List<? extends Type> argumentTypes)
      throws OperatorNotFoundException {

    switch (operatorType) {
      case ADD:
        if (!CommonMetadataUtils.isTwoTypeCalculable(argumentTypes)
            || !AdditionResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(DataNodeQueryMessages.SHOULD_HAVE_TWO_NUMERIC_OPERANDS));
        }
        return AdditionResolver.checkConditions(argumentTypes).get();
      case SUBTRACT:
        if (!CommonMetadataUtils.isTwoTypeCalculable(argumentTypes)
            || !SubtractionResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(DataNodeQueryMessages.SHOULD_HAVE_TWO_NUMERIC_OPERANDS));
        }
        return SubtractionResolver.checkConditions(argumentTypes).get();
      case MULTIPLY:
        if (!CommonMetadataUtils.isTwoTypeCalculable(argumentTypes)
            || !MultiplicationResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(DataNodeQueryMessages.SHOULD_HAVE_TWO_NUMERIC_OPERANDS));
        }
        return MultiplicationResolver.checkConditions(argumentTypes).get();
      case DIVIDE:
        if (!CommonMetadataUtils.isTwoTypeCalculable(argumentTypes)
            || !DivisionResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(DataNodeQueryMessages.SHOULD_HAVE_TWO_NUMERIC_OPERANDS));
        }
        return DivisionResolver.checkConditions(argumentTypes).get();
      case MODULUS:
        if (!CommonMetadataUtils.isTwoTypeCalculable(argumentTypes)
            || !ModulusResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(DataNodeQueryMessages.SHOULD_HAVE_TWO_NUMERIC_OPERANDS));
        }
        return ModulusResolver.checkConditions(argumentTypes).get();
      case NEGATION:
        if (!CommonMetadataUtils.isOneNumericType(argumentTypes)
            && !CommonMetadataUtils.isTimestampType(argumentTypes.get(0))) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(DataNodeQueryMessages.SHOULD_HAVE_ONE_NUMERIC_OPERANDS));
        }
        return argumentTypes.get(0);
      case EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        if (!CommonMetadataUtils.isTwoTypeComparable(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException(
                  DataNodeQueryMessages.SHOULD_HAVE_TWO_COMPARABLE_OPERANDS));
        }
        return BOOLEAN;
      default:
        throw new OperatorNotFoundException(
            operatorType, argumentTypes, new UnsupportedOperationException());
    }
  }

  @Override
  public Type getFunctionReturnType(String functionName, List<? extends Type> argumentTypes) {
    return getFunctionType(functionName, argumentTypes);
  }

  public static Type getFunctionType(String functionName, List<? extends Type> argumentTypes) {

    // builtin scalar function
    if (TableBuiltinScalarFunction.DIFF.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isOneNumericType(argumentTypes)
          && !(argumentTypes.size() == 2
              && isNumericType(argumentTypes.get(0))
              && BOOLEAN.equals(argumentTypes.get(1)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_SUPPORTS_ONE_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_AND_ONE_BOOLEAN);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ROUND.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isOneSupportedMathNumericType(argumentTypes)
          && !CommonMetadataUtils.isTwoSupportedMathNumericType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_SUPPORTS_TWO_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.REPLACE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {

      if (!CommonMetadataUtils.isTwoCharType(argumentTypes)
          && !CommonMetadataUtils.isThreeCharType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.SUBSTRING
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 2
              && CommonMetadataUtils.isCharType(argumentTypes.get(0))
              && CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1)))
          && !(argumentTypes.size() == 3
              && CommonMetadataUtils.isCharType(argumentTypes.get(0))
              && CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1))
              && CommonMetadataUtils.isIntegerNumber(argumentTypes.get(2)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_FIRST_MUST_BE_TEXT_OR_STRING_DATA_TYPE_SECOND);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.LENGTH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))
              || CommonMetadataUtils.isObjectType(argumentTypes.get(0))))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_OR_BLOB_OR_OBJECT_DATA_TYPE);
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.UPPER.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && CommonMetadataUtils.isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.LOWER.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && CommonMetadataUtils.isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.TRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && CommonMetadataUtils.isCharType(argumentTypes.get(0)))
          && !(argumentTypes.size() == 2 && CommonMetadataUtils.isTwoCharType(argumentTypes))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_OR_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.LTRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && CommonMetadataUtils.isCharType(argumentTypes.get(0)))
          && !(argumentTypes.size() == 2 && CommonMetadataUtils.isTwoCharType(argumentTypes))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_OR_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.RTRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && CommonMetadataUtils.isCharType(argumentTypes.get(0)))
          && !(argumentTypes.size() == 2 && CommonMetadataUtils.isTwoCharType(argumentTypes))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_OR_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.REGEXP_LIKE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return BOOLEAN;
    } else if (TableBuiltinScalarFunction.STRPOS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.STARTS_WITH
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return BOOLEAN;
    } else if (TableBuiltinScalarFunction.ENDS_WITH
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return BOOLEAN;
    } else if (TableBuiltinScalarFunction.CONCAT.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() >= 2
          && argumentTypes.stream().allMatch(CommonMetadataUtils::isCharType))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_OR_MORE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.STRCMP.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE);
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.SIN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.COS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.TAN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ASIN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ACOS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ATAN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.SINH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.COSH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.TANH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.DEGREES
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.RADIANS
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ABS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.SIGN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.CEIL.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.FLOOR.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.EXP.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.LN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.LOG10.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.SQRT.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.PI.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.isEmpty())) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages.ACCEPTS_NO_ARGUMENT);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.E.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.isEmpty())) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages.ACCEPTS_NO_ARGUMENT);
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.DATE_BIN
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!CommonMetadataUtils.isTimestampType(argumentTypes.get(2))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THE_SECOND_AND_THIRD_MUST_BE_TIMESTAMP_DATA_TYPE);
      }
      return TIMESTAMP;
    } else if (TableBuiltinScalarFunction.FORMAT.getFunctionName().equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() < 2 || !CommonMetadataUtils.isCharType(argumentTypes.get(0))) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_FIRST_ARGUMENT_PATTERN_MUST_BE_TEXT_OR_STRING_TYPE);
      }
      return STRING;
    } else if (FAIL_FUNCTION_NAME.equalsIgnoreCase(functionName)) {
      return UNKNOWN;
    } else if (TableBuiltinScalarFunction.GREATEST.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.LEAST.getFunctionName().equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() < 2
          || !CommonMetadataUtils.areAllTypesSameAndComparable(argumentTypes)) {
        throw new SemanticException(
            DataNodeQueryMessages.SCALAR_FUNCTION
                + functionName.toLowerCase(Locale.ENGLISH)
                + DataNodeQueryMessages
                    .MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_ALL_TYPE_MUST_BE_THE_SAME);
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.BIT_COUNT.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.BITWISE_AND.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.BITWISE_OR.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.BITWISE_XOR
            .getFunctionName()
            .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 2
          || !(CommonMetadataUtils.isIntegerNumber(argumentTypes.get(0))
              && CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_INT32_OR_INT64_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.BITWISE_NOT
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isIntegerNumber(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_OR_INT64_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.BITWISE_LEFT_SHIFT
            .getFunctionName()
            .equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.BITWISE_RIGHT_SHIFT
            .getFunctionName()
            .equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.BITWISE_RIGHT_SHIFT_ARITHMETIC
            .getFunctionName()
            .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 2
          || !(CommonMetadataUtils.isIntegerNumber(argumentTypes.get(0))
              && CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_INT32_OR_INT64_DATA_TYPE,
                functionName));
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.TO_BASE64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.FROM_BASE64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isCharType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.TO_BASE64URL
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.FROM_BASE64URL
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isCharType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.TO_BASE32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.FROM_BASE32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isCharType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.SHA256.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.SHA512.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.SHA1.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.MD5.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.XXHASH64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.MURMUR3
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.TO_HEX.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1
          && (CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0))))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.FROM_HEX
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isCharType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.REVERSE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1
          || !(CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE_2,
                functionName));
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.HMAC_MD5.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.HMAC_SHA1.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.HMAC_SHA256.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.HMAC_SHA512
            .getFunctionName()
            .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 2
          || !(CommonMetadataUtils.isCharType(argumentTypes.get(0))
              || CommonMetadataUtils.isBlobType(argumentTypes.get(0)))
          || !CommonMetadataUtils.isCharType(argumentTypes.get(1))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_TEXT_STRING_OR_BLOB,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.TO_BIG_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT32.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_BIG_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.TO_BIG_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT64.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_BIG_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.TO_LITTLE_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT32.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_LITTLE_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.TO_LITTLE_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT64.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_LITTLE_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.TO_IEEE754_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !FLOAT.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_FLOAT_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_IEEE754_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return FLOAT;
    } else if (TableBuiltinScalarFunction.TO_IEEE754_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !DOUBLE.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_IEEE754_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.CRC32.getFunctionName().equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1
          || !(CommonMetadataUtils.isBlobType(argumentTypes.get(0))
              || CommonMetadataUtils.isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.SPOOKY_HASH_V2_32
            .getFunctionName()
            .equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.SPOOKY_HASH_V2_64
            .getFunctionName()
            .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1
          || !(CommonMetadataUtils.isBlobType(argumentTypes.get(0))
              || CommonMetadataUtils.isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.TO_BIG_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT32.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_BIG_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.TO_BIG_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT64.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_BIG_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.TO_LITTLE_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT32.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_LITTLE_ENDIAN_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.TO_LITTLE_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !INT64.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_LITTLE_ENDIAN_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.TO_IEEE754_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !FLOAT.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_FLOAT_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_IEEE754_32
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return FLOAT;
    } else if (TableBuiltinScalarFunction.TO_IEEE754_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !DOUBLE.equals(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.FROM_IEEE754_64
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE,
                functionName));
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.CRC32.getFunctionName().equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1
          || !(CommonMetadataUtils.isBlobType(argumentTypes.get(0))
              || CommonMetadataUtils.isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return INT64;
    } else if (TableBuiltinScalarFunction.SPOOKY_HASH_V2_32
            .getFunctionName()
            .equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.SPOOKY_HASH_V2_64
            .getFunctionName()
            .equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 1
          || !(CommonMetadataUtils.isBlobType(argumentTypes.get(0))
              || CommonMetadataUtils.isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE,
                functionName));
      }
      return BLOB;
    } else if (TableBuiltinScalarFunction.LPAD.getFunctionName().equalsIgnoreCase(functionName)
        || TableBuiltinScalarFunction.RPAD.getFunctionName().equalsIgnoreCase(functionName)) {
      if (argumentTypes.size() != 3
          || !CommonMetadataUtils.isBlobType(argumentTypes.get(0))
          || !CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1))
          || !CommonMetadataUtils.isBlobType(argumentTypes.get(2))) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                        .SCALAR_FUNCTION_S_ONLY_ACCEPTS_THREE_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_BLOB_TYPE
                    + DataNodeQueryMessages
                        .SECOND_ARGUMENT_MUST_BE_INT32_OR_INT64_TYPE_THIRD_ARGUMENT_MUST_BE_BLOB_TYPE,
                functionName));
      }
      return BLOB;
    }

    // builtin aggregation function
    // check argument type
    switch (functionName.toLowerCase(Locale.ENGLISH)) {
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.EXTREME:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT,
                  functionName));
        }

        if (!CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(0))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE,
                  functionName));
        }
        break;
      case SqlConstant.CORR:
      case SqlConstant.COVAR_POP:
      case SqlConstant.COVAR_SAMP:
      case SqlConstant.REGR_SLOPE:
      case SqlConstant.REGR_INTERCEPT:
        if (argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_2,
                  functionName.toUpperCase(),
                  argumentTypes.size()));
        }
        if (!CommonMetadataUtils.isNumericType(argumentTypes.get(0))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP,
                  functionName.toUpperCase()));
        }
        if (!CommonMetadataUtils.isNumericType(argumentTypes.get(1))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP,
                  functionName.toUpperCase()));
        }
        break;
      case SqlConstant.SKEWNESS:
      case SqlConstant.KURTOSIS:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_1,
                  functionName.toUpperCase(),
                  argumentTypes.size()));
        }
        if (!CommonMetadataUtils.isNumericType(argumentTypes.get(0))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP,
                  functionName.toUpperCase()));
        }
        break;
      case SqlConstant.MIN:
      case SqlConstant.MAX:
      case SqlConstant.MODE:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT,
                  functionName));
        }
        break;
      case SqlConstant.COUNT_IF:
        if (argumentTypes.size() != 1 || !CommonMetadataUtils.isBool(argumentTypes.get(0))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_BOOLEAN_EXPRESSION_AS_ARGUMENT,
                  functionName));
        }
        break;
      case SqlConstant.FIRST_AGGREGATION:
      case SqlConstant.LAST_AGGREGATION:
        if (argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_OR_TWO_ARGUMENTS,
                  functionName));
        } else if (!CommonMetadataUtils.isTimestampType(argumentTypes.get(1))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_ORDERABLE,
                  functionName));
        }
        break;
      case SqlConstant.FIRST_BY_AGGREGATION:
      case SqlConstant.LAST_BY_AGGREGATION:
        if (argumentTypes.size() != 3) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS,
                  functionName));
        }
        break;
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        if (argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_ARGUMENTS,
                  functionName));
        } else if (!argumentTypes.get(1).isOrderable()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_ORDERABLE,
                  functionName));
        }

        break;
      case SqlConstant.APPROX_COUNT_DISTINCT:
        if (argumentTypes.size() != 1 && argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_ARGUMENTS,
                  functionName));
        }

        if (argumentTypes.size() == 2
            && !CommonMetadataUtils.isSupportedMathNumericType(argumentTypes.get(1))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_NUMBERIC_TYPE_AND_DO_NOT_USE,
                  functionName));
        }
        break;
      case SqlConstant.APPROX_MOST_FREQUENT:
        if (argumentTypes.size() != 3) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_THREE_ARGUMENTS,
                  functionName));
        }
        break;
      case SqlConstant.APPROX_PERCENTILE:
        int argumentSize = argumentTypes.size();
        if (argumentSize != 2 && argumentSize != 3) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS,
                  functionName));
        }

        Type valueColumnType = argumentTypes.get(0);
        if (!isNumericType(valueColumnType)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT,
                  functionName));
        }

        Type percentageType = argumentTypes.get(argumentSize - 1);
        if (!isDecimalType(percentageType)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE,
                  functionName));
        }
        if (argumentSize == 3) {
          Type weightType = argumentTypes.get(1);
          if (!INT32.equals(weightType) && !CommonMetadataUtils.isUnknownType(weightType)) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.AGGREGATION_FUNCTIONS_S_DO_NOT_SUPPORT_WEIGHT_AS_S_TYPE,
                    functionName,
                    weightType.getDisplayName()));
          }
        }
        break;
      case SqlConstant.PERCENTILE:
        if (argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_ONLY_HAVE_TWO_ARGUMENTS_3D12DCFD,
                  functionName));
        }

        if (!isNumericType(argumentTypes.get(0))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP_97A6CA87,
                  functionName));
        }
        if (!isDecimalType(argumentTypes.get(1))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE_57033ADF,
                  functionName));
        }
        break;
      case SqlConstant.COUNT:
        break;
      default:
        // ignore
    }

    // get return type
    switch (functionName.toLowerCase(Locale.ENGLISH)) {
      case SqlConstant.COUNT:
      case SqlConstant.COUNT_ALL:
      case SqlConstant.COUNT_IF:
      case SqlConstant.APPROX_COUNT_DISTINCT:
        return INT64;
      case SqlConstant.FIRST_AGGREGATION:
      case SqlConstant.LAST_AGGREGATION:
      case SqlConstant.FIRST_BY_AGGREGATION:
      case SqlConstant.LAST_BY_AGGREGATION:
      case SqlConstant.EXTREME:
      case SqlConstant.MODE:
      case SqlConstant.MAX:
      case SqlConstant.MIN:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
      case SqlConstant.APPROX_PERCENTILE:
      case SqlConstant.PERCENTILE:
        return argumentTypes.get(0);
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
      case SqlConstant.CORR:
      case SqlConstant.COVAR_POP:
      case SqlConstant.COVAR_SAMP:
      case SqlConstant.REGR_SLOPE:
      case SqlConstant.REGR_INTERCEPT:
      case SqlConstant.SKEWNESS:
      case SqlConstant.KURTOSIS:
        return DOUBLE;
      case SqlConstant.APPROX_MOST_FREQUENT:
        return STRING;
      default:
        // ignore
    }

    // builtin window function
    // check argument type
    switch (functionName.toLowerCase(Locale.ENGLISH)) {
      case SqlConstant.NTILE:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT,
                  functionName));
        }
        break;
      case SqlConstant.NTH_VALUE:
        if (argumentTypes.size() != 2
            || !CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1))) {
          throw new SemanticException(
              DataNodeQueryMessages
                  .WINDOW_FUNCTION_NTH_VALUE_SHOULD_ONLY_HAVE_TWO_ARGUMENT_AND_SECOND_ARGUMENT_MUST_BE);
        }
        break;
      case SqlConstant.TABLE_FIRST_VALUE:
      case SqlConstant.TABLE_LAST_VALUE:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT,
                  functionName));
        }
      case SqlConstant.LEAD:
      case SqlConstant.LAG:
        if (argumentTypes.isEmpty() || argumentTypes.size() > 3) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_TO_THREE_ARGUMENT,
                  functionName));
        }
        if (argumentTypes.size() >= 2
            && !CommonMetadataUtils.isIntegerNumber(argumentTypes.get(1))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.WINDOW_FUNCTION_S_S_SECOND_ARGUMENT_MUST_BE_INTEGER_TYPE,
                  functionName));
        }
        break;
      default:
        // ignore
    }

    // get return type
    switch (functionName.toLowerCase(Locale.ENGLISH)) {
      case SqlConstant.RANK:
      case SqlConstant.DENSE_RANK:
      case SqlConstant.ROW_NUMBER:
      case SqlConstant.NTILE:
        return INT64;
      case SqlConstant.PERCENT_RANK:
      case SqlConstant.CUME_DIST:
        return DOUBLE;
      case SqlConstant.TABLE_FIRST_VALUE:
      case SqlConstant.TABLE_LAST_VALUE:
      case SqlConstant.NTH_VALUE:
      case SqlConstant.LEAD:
      case SqlConstant.LAG:
        return argumentTypes.get(0);
      default:
        // ignore
    }

    // User-defined scalar function
    if (TableUDFUtils.isScalarFunction(functionName)) {
      ScalarFunction scalarFunction = TableUDFUtils.getScalarFunction(functionName);
      FunctionArguments functionArguments =
          new FunctionArguments(
              argumentTypes.stream()
                  .map(UDFDataTypeTransformer::transformReadTypeToUDFDataType)
                  .collect(Collectors.toList()),
              Collections.emptyMap());
      try {
        ScalarFunctionAnalysis scalarFunctionAnalysis = scalarFunction.analyze(functionArguments);
        Type returnType =
            UDFDataTypeTransformer.transformUDFDataTypeToReadType(
                scalarFunctionAnalysis.getOutputDataType());
        if (returnType == ObjectType.OBJECT) {
          throw new SemanticException(
              DataNodeQueryMessages.OBJECT_TYPE_IS_NOT_SUPPORTED_AS_RETURN_TYPE);
        }
        return returnType;
      } catch (Exception e) {
        throw new SemanticException(
            DataNodeQueryMessages.INVALID_FUNCTION_PARAMETERS + e.getMessage());
      } finally {
        scalarFunction.beforeDestroy();
      }
    } else if (TableUDFUtils.isAggregateFunction(functionName)) {
      AggregateFunction aggregateFunction = TableUDFUtils.getAggregateFunction(functionName);
      FunctionArguments functionArguments =
          new FunctionArguments(
              argumentTypes.stream()
                  .map(UDFDataTypeTransformer::transformReadTypeToUDFDataType)
                  .collect(Collectors.toList()),
              Collections.emptyMap());
      try {
        AggregateFunctionAnalysis aggregateFunctionAnalysis =
            aggregateFunction.analyze(functionArguments);
        Type returnType =
            UDFDataTypeTransformer.transformUDFDataTypeToReadType(
                aggregateFunctionAnalysis.getOutputDataType());
        if (returnType == ObjectType.OBJECT) {
          throw new SemanticException(
              DataNodeQueryMessages.OBJECT_TYPE_IS_NOT_SUPPORTED_AS_RETURN_TYPE);
        }
        return returnType;
      } catch (Exception e) {
        throw new SemanticException(
            DataNodeQueryMessages.INVALID_FUNCTION_PARAMETERS + e.getMessage());
      } finally {
        aggregateFunction.beforeDestroy();
      }
    }

    throw new SemanticException(DataNodeQueryMessages.UNKNOWN_FUNCTION + functionName);
  }

  @Override
  public boolean isAggregationFunction(
      final SessionInfo session, final String functionName, final AccessControl accessControl) {
    return TableBuiltinAggregationFunction.getBuiltInAggregateFunctionName()
            .contains(functionName.toLowerCase(Locale.ENGLISH))
        || TableUDFUtils.isAggregateFunction(functionName);
  }

  @Override
  public Type getType(final TypeSignature signature) throws TypeNotFoundException {
    return typeManager.getType(signature);
  }

  @Override
  public boolean canCoerce(final Type from, final Type to) {
    return true;
  }

  @Override
  public Map<String, List<DeviceEntry>> indexScan(
      final QualifiedObjectName tableName,
      final List<Expression> expressionList,
      final List<String> attributeColumns,
      final MPPQueryContext context) {
    return TableDeviceSchemaFetcher.getInstance()
        .fetchDeviceSchemaForDataQuery(
            tableName.getDatabaseName(),
            tableName.getObjectName(),
            expressionList,
            attributeColumns,
            context);
  }

  @Override
  public Optional<TableSchema> validateTableHeaderSchema4TsFile(
      final String database,
      final TableSchema tableSchema,
      final MPPQueryContext context,
      final boolean allowCreateTable,
      final boolean isStrictTagColumn,
      final AtomicBoolean needDecode4DifferentTimeColumn)
      throws LoadAnalyzeTableColumnDisorderException {
    return TableHeaderSchemaValidator.getInstance()
        .validateTableHeaderSchema4TsFile(
            database,
            tableSchema,
            context,
            allowCreateTable,
            isStrictTagColumn,
            needDecode4DifferentTimeColumn);
  }

  @Override
  public void validateInsertNodeMeasurements(
      final String database,
      final InsertNodeMeasurementInfo measurementInfo,
      final MPPQueryContext context,
      final boolean allowCreateTable,
      final TableHeaderSchemaValidator.MeasurementValidator measurementValidator,
      final TableHeaderSchemaValidator.TagColumnHandler tagColumnHandler) {
    TableHeaderSchemaValidator.getInstance()
        .validateInsertNodeMeasurements(
            database,
            measurementInfo,
            context,
            allowCreateTable,
            measurementValidator,
            tagColumnHandler);
  }

  @Override
  public void validateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation, MPPQueryContext context) {
    TableDeviceSchemaValidator.getInstance().validateDeviceSchema(schemaValidation, context);
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      final List<DataPartitionQueryParam> dataPartitionQueryParams, final String userName) {
    return partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams, userName);
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(
      final String database, final List<IDeviceID> deviceIDList, final String userName) {
    return partitionFetcher.getOrCreateSchemaPartition(database, deviceIDList, userName);
  }

  @Override
  public SchemaPartition getSchemaPartition(
      final String database, final List<IDeviceID> deviceIDList) {
    return partitionFetcher.getSchemaPartition(database, deviceIDList);
  }

  @Override
  public SchemaPartition getSchemaPartition(final String database) {
    return partitionFetcher.getSchemaPartition(database, null);
  }

  @Override
  public DataPartition getDataPartition(
      String database, List<DataPartitionQueryParam> sgNameToQueryParamsMap) {
    return partitionFetcher.getDataPartition(
        Collections.singletonMap(database, sgNameToQueryParamsMap));
  }

  @Override
  public DataPartition getDataPartitionWithUnclosedTimeRange(
      String database, List<DataPartitionQueryParam> sgNameToQueryParamsMap) {
    return partitionFetcher.getDataPartitionWithUnclosedTimeRange(
        Collections.singletonMap(database, sgNameToQueryParamsMap));
  }
}
