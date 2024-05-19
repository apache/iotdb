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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.db.exception.metadata.table.TableNotExistsException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.TableModelSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;

public class TableMetadataImpl implements Metadata {

  private final TypeManager typeManager = new InternalTypeManager();

  private final DataNodeTableCache tableCache = DataNodeTableCache.getInstance();

  @Override
  public boolean tableExists(QualifiedObjectName name) {
    return tableCache.getTable(name.getDatabaseName(), name.getObjectName()) != null;
  }

  @Override
  public Optional<TableSchema> getTableSchema(SessionInfo session, QualifiedObjectName name) {
    TsTable table = tableCache.getTable(name.getDatabaseName(), name.getObjectName());
    if (table == null) {
      throw new SemanticException(
          new TableNotExistsException(name.getDatabaseName(), name.getObjectName()));
    }
    List<ColumnSchema> columnSchemaList =
        table.getColumnList().stream()
            .map(
                o ->
                    new ColumnSchema(
                        o.getColumnName(),
                        TypeFactory.getType(o.getDataType()),
                        false,
                        o.getColumnCategory()))
            .collect(Collectors.toList());
    return Optional.of(new TableSchema(table.getTableName(), columnSchemaList));
  }

  @Override
  public Type getOperatorReturnType(OperatorType operatorType, List<? extends Type> argumentTypes)
      throws OperatorNotFoundException {

    switch (operatorType) {
      case ADD:
      case SUBTRACT:
      case MULTIPLY:
      case DIVIDE:
      case MODULUS:
        if (!isTwoNumericType(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return DOUBLE;
      case NEGATION:
        if (!isOneNumericType(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have one numeric operands."));
        }
        return DOUBLE;
      case EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        if (!isTwoTypeComparable(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two comparable operands."));
        }
        return BOOLEAN;
      default:
        throw new OperatorNotFoundException(
            operatorType, argumentTypes, new UnsupportedOperationException());
    }
  }

  @Override
  public Type getFunctionReturnType(String functionName, List<? extends Type> argumentTypes) {

    // builtin scalar function
    if (BuiltinScalarFunction.DIFF.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isOneNumericType(argumentTypes)
          && !(argumentTypes.size() == 2
              && isNumericType(argumentTypes.get(0))
              && BOOLEAN.equals(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports one numeric data types [INT32, INT64, FLOAT, DOUBLE] and one boolean");
      }
      return argumentTypes.get(0);
    } else if (BuiltinScalarFunction.ROUND.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isOneNumericType(argumentTypes) && !isTwoNumericType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function"
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports two numeric data types [INT32, INT64, FLOAT, DOUBLE]");
      }
      return DOUBLE;
    } else if (BuiltinScalarFunction.REPLACE.getFunctionName().equalsIgnoreCase(functionName)) {

      if (!isTwoTextType(argumentTypes) && !isThreeTextType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function: "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports text data type.");
      }
      return TEXT;
    } else if (BuiltinScalarFunction.SUBSTRING.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 2
              && TEXT.equals(argumentTypes.get(0))
              && isNumericType(argumentTypes.get(1)))
          && !(argumentTypes.size() == 3
              && TEXT.equals(argumentTypes.get(0))
              && isNumericType(argumentTypes.get(1))
              && isNumericType(argumentTypes.get(2)))) {
        throw new SemanticException(
            "Scalar function"
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports text data type.");
      }
      return TEXT;
    }

    // builtin aggregation function
    // check argument type
    switch (functionName.toLowerCase()) {
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.EXTREME:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        if (!isOneNumericType(argumentTypes)) {
          throw new SemanticException(
              String.format(
                  "Aggregate functions [%s] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]",
                  functionName));
        }
        break;
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.TIME_DURATION:
      case SqlConstant.MODE:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  "Aggregate functions [%s] should only have one argument", functionName));
        }
        break;
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        if (argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  "Aggregate functions [%s] should only have two arguments", functionName));
        } else if (!argumentTypes.get(1).isOrderable()) {
          throw new SemanticException(
              String.format(
                  "Second argument of Aggregate functions [%s] should be orderable", functionName));
        }

        break;
      case SqlConstant.COUNT:
        break;
      default:
        // ignore
    }

    // get return type
    switch (functionName.toLowerCase()) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
      case SqlConstant.TIME_DURATION:
        return INT64;
      case SqlConstant.MIN_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.EXTREME:
      case SqlConstant.MODE:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        return argumentTypes.get(0);
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        return DOUBLE;
      default:
        // ignore
    }

    // TODO scalar UDF function

    // TODO UDAF

    throw new SemanticException("Unknown function: " + functionName);
  }

  @Override
  public boolean isAggregationFunction(
      SessionInfo session, String functionName, AccessControl accessControl) {
    return BuiltinAggregationFunction.getNativeFunctionNames()
        .contains(functionName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Type getType(TypeSignature signature) throws TypeNotFoundException {
    return typeManager.getType(signature);
  }

  @Override
  public boolean canCoerce(Type from, Type to) {
    return true;
  }

  @Override
  public List<DeviceEntry> indexScan(
      QualifiedObjectName tableName,
      List<Expression> expressionList,
      List<String> attributeColumns) {
    return TableModelSchemaFetcher.getInstance()
        .fetchDeviceSchema(
            tableName.getDatabaseName(),
            tableName.getObjectName(),
            expressionList,
            attributeColumns);
  }

  public static boolean isTwoNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 2
        && isNumericType(argumentTypes.get(0))
        && isNumericType(argumentTypes.get(1));
  }

  public static boolean isOneNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && isNumericType(argumentTypes.get(0));
  }

  public static boolean isOneBooleanType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && BOOLEAN.equals(argumentTypes.get(0));
  }

  public static boolean isOneTextType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && TEXT.equals(argumentTypes.get(0));
  }

  public static boolean isTwoTextType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 2
        && TEXT.equals(argumentTypes.get(0))
        && TEXT.equals(argumentTypes.get(1));
  }

  public static boolean isThreeTextType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 3
        && TEXT.equals(argumentTypes.get(0))
        && TEXT.equals(argumentTypes.get(1))
        && TEXT.equals(argumentTypes.get(2));
  }

  public static boolean isNumericType(Type type) {
    return DOUBLE.equals(type) || FLOAT.equals(type) || INT32.equals(type) || INT64.equals(type);
  }

  public static boolean isTwoTypeComparable(List<? extends Type> argumentTypes) {
    if (argumentTypes.size() != 2) {
      return false;
    }
    Type left = argumentTypes.get(0);
    Type right = argumentTypes.get(1);
    if (left.equals(right)) {
      return true;
    }

    // Boolean type and Binary Type can not be compared with other types
    return isNumericType(left) && isNumericType(right);
  }
}
