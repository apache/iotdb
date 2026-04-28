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

package org.apache.iotdb.calc.plan.relational.metadata;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.exception.table.TableNotExistsException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class CommonMetadataUtils {
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

  public static boolean isObjectType(Type type) {
    return ObjectType.OBJECT.equals(type);
  }

  public static boolean isBlobType(Type type) {
    return BLOB.equals(type);
  }

  public static boolean isBool(Type type) {
    return BOOLEAN.equals(type);
  }

  public static boolean isDecimalType(Type type) {
    return DOUBLE.equals(type) || FLOAT.equals(type);
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

  public static boolean isUnknownType(Type type) {
    return UNKNOWN.equals(type);
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
    return (isNumericType(left) && isNumericType(right))
        || (isCharType(left) && isCharType(right))
        || (isUnknownType(left) && (isNumericType(right) || isCharType(right)))
        || ((isNumericType(left) || isCharType(left)) && isUnknownType(right));
  }

  public static boolean areAllTypesSameAndComparable(List<? extends Type> argumentTypes) {
    if (argumentTypes == null || argumentTypes.isEmpty()) {
      return true;
    }
    Type firstType = argumentTypes.get(0);
    if (!firstType.isComparable()) {
      return false;
    }
    return argumentTypes.stream().allMatch(type -> type.equals(firstType));
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
    if ((isUnknownType(left) && isArithmeticType(right))
        || (isUnknownType(right) && isArithmeticType(left))) {
      return true;
    }
    return isArithmeticType(left) && isArithmeticType(right);
  }

  public static void throwTableNotExistsException(final String database, final String tableName) {
    throw new SemanticException(new TableNotExistsException(database, tableName));
  }

  public static void throwColumnNotExistsException(final Object columnName) {
    throw new SemanticException(
        new IoTDBException(
            String.format("Column '%s' cannot be resolved.", columnName),
            TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode()));
  }
}
