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

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.AdditionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.DivisionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.ModulusResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.MultiplicationResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.SubtractionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TableBuiltinScalarFunction;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class TableMetadataImpl implements Metadata {

  private final TypeManager typeManager = new InternalTypeManager();

  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();

  private final DataNodeTableCache tableCache = DataNodeTableCache.getInstance();

  @Override
  public boolean tableExists(QualifiedObjectName name) {
    return tableCache.getTable(name.getDatabaseName(), name.getObjectName()) != null;
  }

  @Override
  public Optional<TableSchema> getTableSchema(SessionInfo session, QualifiedObjectName name) {
    TsTable table = tableCache.getTable(name.getDatabaseName(), name.getObjectName());
    if (table == null) {
      return Optional.empty();
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
        if (!isTwoTypeCalculable(argumentTypes)
            || !AdditionResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return AdditionResolver.checkConditions(argumentTypes).get();
      case SUBTRACT:
        if (!isTwoTypeCalculable(argumentTypes)
            || !SubtractionResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return SubtractionResolver.checkConditions(argumentTypes).get();
      case MULTIPLY:
        if (!isTwoTypeCalculable(argumentTypes)
            || !MultiplicationResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return MultiplicationResolver.checkConditions(argumentTypes).get();
      case DIVIDE:
        if (!isTwoTypeCalculable(argumentTypes)
            || !DivisionResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return DivisionResolver.checkConditions(argumentTypes).get();
      case MODULUS:
        if (!isTwoTypeCalculable(argumentTypes)
            || !ModulusResolver.checkConditions(argumentTypes).isPresent()) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return ModulusResolver.checkConditions(argumentTypes).get();
      case NEGATION:
        if (!isOneNumericType(argumentTypes) && !isTimestampType(argumentTypes.get(0))) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have one numeric operands."));
        }
        return argumentTypes.get(0);
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
    return getFunctionType(functionName, argumentTypes);
  }

  public static Type getFunctionType(String functionName, List<? extends Type> argumentTypes) {

    // builtin scalar function
    if (TableBuiltinScalarFunction.DIFF.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isOneNumericType(argumentTypes)
          && !(argumentTypes.size() == 2
              && isNumericType(argumentTypes.get(0))
              && BOOLEAN.equals(argumentTypes.get(1)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports one numeric data types [INT32, INT64, FLOAT, DOUBLE] and one boolean");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ROUND.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isOneSupportedMathNumericType(argumentTypes)
          && !isTwoSupportedMathNumericType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports two numeric data types [INT32, INT64, FLOAT, DOUBLE]");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.REPLACE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {

      if (!isTwoCharType(argumentTypes) && !isThreeCharType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.SUBSTRING
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 2
              && isCharType(argumentTypes.get(0))
              && isIntegerNumber(argumentTypes.get(1)))
          && !(argumentTypes.size() == 3
              && isCharType(argumentTypes.get(0))
              && isIntegerNumber(argumentTypes.get(1))
              && isIntegerNumber(argumentTypes.get(2)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.LENGTH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be text or string data type.");
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.UPPER.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.LOWER.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isCharType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.TRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isCharType(argumentTypes.get(0)))
          && !(argumentTypes.size() == 2 && isTwoCharType(argumentTypes))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one or two arguments and they must be text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.LTRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isCharType(argumentTypes.get(0)))
          && !(argumentTypes.size() == 2 && isTwoCharType(argumentTypes))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one or two arguments and they must be text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.RTRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isCharType(argumentTypes.get(0)))
          && !(argumentTypes.size() == 2 && isTwoCharType(argumentTypes))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one or two arguments and they must be text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.REGEXP_LIKE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two arguments and they must be text or string data type.");
      }
      return BOOLEAN;
    } else if (TableBuiltinScalarFunction.STRPOS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two arguments and they must be text or string data type.");
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.STARTS_WITH
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two arguments and they must be text or string data type.");
      }
      return BOOLEAN;
    } else if (TableBuiltinScalarFunction.ENDS_WITH
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two arguments and they must be text or string data type.");
      }
      return BOOLEAN;
    } else if (TableBuiltinScalarFunction.CONCAT.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() >= 2
          && argumentTypes.stream().allMatch(TableMetadataImpl::isCharType))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two or more arguments and they must be text or string data type.");
      }
      return STRING;
    } else if (TableBuiltinScalarFunction.STRCMP.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isTwoCharType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two arguments and they must be text or string data type.");
      }
      return INT32;
    } else if (TableBuiltinScalarFunction.SIN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.COS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.TAN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ASIN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ACOS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ATAN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.SINH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.COSH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.TANH.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.DEGREES
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.RADIANS
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.ABS.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.SIGN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return argumentTypes.get(0);
    } else if (TableBuiltinScalarFunction.CEIL.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.FLOOR.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.EXP.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.LN.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.LOG10.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.SQRT.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.PI.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.isEmpty())) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " accepts no argument.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.E.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.isEmpty())) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " accepts no argument.");
      }
      return DOUBLE;
    } else if (TableBuiltinScalarFunction.DATE_BIN
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      if (!(argumentTypes.size() == 4 && isTimestampType(argumentTypes.get(2)))) {
        throw new SemanticException(
            "Scalar function "
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only accepts two or three arguments and the second and third must be TimeStamp data type.");
      }
      return TIMESTAMP;
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
        if (!isOneSupportedMathNumericType(argumentTypes)) {
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
    return TableBuiltinAggregationFunction.getNativeFunctionNames()
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
  public IPartitionFetcher getPartitionFetcher() {
    return ClusterPartitionFetcher.getInstance();
  }

  @Override
  public List<DeviceEntry> indexScan(
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
  public Optional<TableSchema> validateTableHeaderSchema(
      String database, TableSchema tableSchema, MPPQueryContext context, boolean allowCreateTable) {
    return TableHeaderSchemaValidator.getInstance()
        .validateTableHeaderSchema(database, tableSchema, context, allowCreateTable);
  }

  @Override
  public void validateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation, MPPQueryContext context) {
    TableDeviceSchemaValidator.getInstance().validateDeviceSchema(schemaValidation, context);
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      List<DataPartitionQueryParam> dataPartitionQueryParams, String userName) {
    return partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams, userName);
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(
      String database, List<IDeviceID> deviceIDList, String userName) {
    return partitionFetcher.getOrCreateSchemaPartition(
        PATH_ROOT + PATH_SEPARATOR + database, deviceIDList, userName);
  }

  @Override
  public SchemaPartition getSchemaPartition(String database, List<IDeviceID> deviceIDList) {
    return partitionFetcher.getSchemaPartition(PATH_ROOT + PATH_SEPARATOR + database, deviceIDList);
  }

  @Override
  public SchemaPartition getSchemaPartition(String database) {
    return partitionFetcher.getSchemaPartition(PATH_ROOT + PATH_SEPARATOR + database);
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

  @Override
  public boolean canUseStatistics(String functionName) {
    return TableBuiltinAggregationFunction.canUseStatistics(functionName);
  }

  public static boolean isTwoNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 2
        && isNumericType(argumentTypes.get(0))
        && isNumericType(argumentTypes.get(1));
  }

  public static boolean isOneNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && isNumericType(argumentTypes.get(0));
  }

  public static boolean isTwoSupportedMathNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 2
        && isSupportedMathNumericType(argumentTypes.get(0))
        && isSupportedMathNumericType(argumentTypes.get(1));
  }

  public static boolean isOneSupportedMathNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && isSupportedMathNumericType(argumentTypes.get(0));
  }

  public static boolean isOneBooleanType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && BOOLEAN.equals(argumentTypes.get(0));
  }

  public static boolean isOneCharType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && isCharType(argumentTypes.get(0));
  }

  public static boolean isTwoCharType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 2
        && isCharType(argumentTypes.get(0))
        && isCharType(argumentTypes.get(1));
  }

  public static boolean isThreeCharType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 3
        && isCharType(argumentTypes.get(0))
        && isCharType(argumentTypes.get(1))
        && isCharType(argumentTypes.get(2));
  }

  public static boolean isCharType(Type type) {
    return TEXT.equals(type) || StringType.STRING.equals(type);
  }

  public static boolean isBlobType(Type type) {
    return BlobType.BLOB.equals(type);
  }

  public static boolean isBool(Type type) {
    return BOOLEAN.equals(type);
  }

  public static boolean isSupportedMathNumericType(Type type) {
    return DOUBLE.equals(type) || FLOAT.equals(type) || INT32.equals(type) || INT64.equals(type);
  }

  public static boolean isNumericType(Type type) {
    return DOUBLE.equals(type)
        || FLOAT.equals(type)
        || INT32.equals(type)
        || INT64.equals(type)
        || TIMESTAMP.equals(type);
  }

  public static boolean isTimestampType(Type type) {
    return TIMESTAMP.equals(type);
  }

  public static boolean isIntegerNumber(Type type) {
    return INT32.equals(type) || INT64.equals(type);
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
    return (isNumericType(left) && isNumericType(right)) || (isCharType(left) && isCharType(right));
  }

  public static boolean isArithmeticType(Type type) {
    return INT32.equals(type)
        || INT64.equals(type)
        || FLOAT.equals(type)
        || DOUBLE.equals(type)
        || DATE.equals(type)
        || TIMESTAMP.equals(type);
  }

  public static boolean isTwoTypeCalculable(List<? extends Type> argumentTypes) {
    if (argumentTypes.size() != 2) {
      return false;
    }
    Type left = argumentTypes.get(0);
    Type right = argumentTypes.get(1);
    return isArithmeticType(left) && isArithmeticType(right);
  }
}
