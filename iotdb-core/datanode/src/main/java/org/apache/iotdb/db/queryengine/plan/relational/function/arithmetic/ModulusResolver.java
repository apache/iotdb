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

package org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class ModulusResolver {

  private static final Map<Type, Map<Type, Type>> CONDITION_MAP = new HashMap<>();
  private static final Map<TSDataType, Map<TSDataType, TSDataType>> CONDITION_MAP_TREE =
      new HashMap<>();

  static {
    addCondition(INT32, INT32, INT32);
    addCondition(INT32, INT64, INT64);
    addCondition(INT32, FLOAT, FLOAT);
    addCondition(INT32, DOUBLE, DOUBLE);
    addCondition(INT32, UNKNOWN, INT32);

    addCondition(INT64, INT32, INT64);
    addCondition(INT64, INT64, INT64);
    addCondition(INT64, FLOAT, FLOAT);
    addCondition(INT64, DOUBLE, DOUBLE);
    addCondition(INT64, UNKNOWN, INT64);

    addCondition(FLOAT, INT32, FLOAT);
    addCondition(FLOAT, INT64, FLOAT);
    addCondition(FLOAT, FLOAT, FLOAT);
    addCondition(FLOAT, DOUBLE, DOUBLE);
    addCondition(FLOAT, UNKNOWN, FLOAT);

    addCondition(DOUBLE, INT32, DOUBLE);
    addCondition(DOUBLE, INT64, DOUBLE);
    addCondition(DOUBLE, FLOAT, DOUBLE);
    addCondition(DOUBLE, DOUBLE, DOUBLE);
    addCondition(DOUBLE, UNKNOWN, DOUBLE);

    addCondition(UNKNOWN, INT32, INT32);
    addCondition(UNKNOWN, INT64, INT64);
    addCondition(UNKNOWN, FLOAT, FLOAT);
    addCondition(UNKNOWN, DOUBLE, DOUBLE);

    addConditionTS(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32);
    addConditionTS(TSDataType.INT32, TSDataType.INT64, TSDataType.INT64);
    addConditionTS(TSDataType.INT32, TSDataType.FLOAT, TSDataType.FLOAT);
    addConditionTS(TSDataType.INT32, TSDataType.DOUBLE, TSDataType.DOUBLE);
    addConditionTS(TSDataType.INT32, TSDataType.UNKNOWN, TSDataType.INT32);

    addConditionTS(TSDataType.INT64, TSDataType.INT32, TSDataType.INT64);
    addConditionTS(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64);
    addConditionTS(TSDataType.INT64, TSDataType.FLOAT, TSDataType.FLOAT);
    addConditionTS(TSDataType.INT64, TSDataType.DOUBLE, TSDataType.DOUBLE);
    addConditionTS(TSDataType.INT64, TSDataType.UNKNOWN, TSDataType.INT64);

    addConditionTS(TSDataType.FLOAT, TSDataType.INT32, TSDataType.FLOAT);
    addConditionTS(TSDataType.FLOAT, TSDataType.INT64, TSDataType.FLOAT);
    addConditionTS(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT);
    addConditionTS(TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.DOUBLE);
    addConditionTS(TSDataType.FLOAT, TSDataType.UNKNOWN, TSDataType.FLOAT);

    addConditionTS(TSDataType.DOUBLE, TSDataType.INT32, TSDataType.DOUBLE);
    addConditionTS(TSDataType.DOUBLE, TSDataType.INT64, TSDataType.DOUBLE);
    addConditionTS(TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.DOUBLE);
    addConditionTS(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE);
    addConditionTS(TSDataType.DOUBLE, TSDataType.UNKNOWN, TSDataType.DOUBLE);

    addConditionTS(TSDataType.UNKNOWN, TSDataType.INT32, TSDataType.INT32);
    addConditionTS(TSDataType.UNKNOWN, TSDataType.INT64, TSDataType.INT64);
    addConditionTS(TSDataType.UNKNOWN, TSDataType.FLOAT, TSDataType.FLOAT);
    addConditionTS(TSDataType.UNKNOWN, TSDataType.DOUBLE, TSDataType.DOUBLE);
  }

  private static void addCondition(Type condition1, Type condition2, Type result) {
    CONDITION_MAP.computeIfAbsent(condition1, k -> new HashMap<>()).put(condition2, result);
  }

  private static void addConditionTS(TSDataType left, TSDataType right, TSDataType result) {
    CONDITION_MAP_TREE.computeIfAbsent(left, k -> new HashMap<>()).put(right, result);
  }

  public static Optional<Type> checkConditions(List<? extends Type> argumentTypes) {
    return Optional.ofNullable(
        CONDITION_MAP
            .getOrDefault(argumentTypes.get(0), Collections.emptyMap())
            .getOrDefault(argumentTypes.get(1), null));
  }

  public static Optional<TSDataType> inferType(TSDataType leftType, TSDataType rightType) {
    return Optional.ofNullable(
        CONDITION_MAP_TREE
            .getOrDefault(leftType, Collections.emptyMap())
            .getOrDefault(rightType, null));
  }
}
