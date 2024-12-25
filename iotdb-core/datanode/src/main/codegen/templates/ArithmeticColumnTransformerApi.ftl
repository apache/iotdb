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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/column/binary/ArithmeticColumnTransformerApi.java" />
package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.tsfile.read.common.type.Type;

import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.AdditionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.DivisionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.ModulusResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.MultiplicationResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.SubtractionResolver;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.DoubleNegationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.FloatNegationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.IntNegationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.LongNegationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.TimestampNegationColumnTransformer;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

public class ArithmeticColumnTransformerApi {
  <#list mathematicalOperator.binaryOperators as operator>

  public static ColumnTransformer get${operator.name}Transformer(
      ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    switch (leftTransformer.getType().getTypeEnum()) {
      case INT32:
        return getInt${operator.name}Transformer(leftTransformer, rightTransformer, zoneId);
      case INT64:
        return getLong${operator.name}Transformer(leftTransformer, rightTransformer, zoneId);
      case FLOAT:
        return getFloat${operator.name}Transformer(leftTransformer, rightTransformer, zoneId);
      case DOUBLE:
        return getDouble${operator.name}Transformer(leftTransformer, rightTransformer, zoneId);
      <#if operator.name == "Addition" || operator.name == "Subtraction">
      case DATE:
        return getDate${operator.name}Transformer(leftTransformer, rightTransformer, zoneId);
      case TIMESTAMP:
        return getTimestamp${operator.name}Transformer(leftTransformer, rightTransformer, zoneId);
      </#if>
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }
  </#list>

  public static ColumnTransformer getNegationTransformer(ColumnTransformer columnTransformer) {
    switch (columnTransformer.getType().getTypeEnum()) {
      case INT32:
        return new IntNegationColumnTransformer(
            columnTransformer.getType(), columnTransformer);
      case INT64:
        return new LongNegationColumnTransformer(
            columnTransformer.getType(), columnTransformer);
      case FLOAT:
        return new FloatNegationColumnTransformer(
            columnTransformer.getType(), columnTransformer);
      case DOUBLE:
        return new DoubleNegationColumnTransformer(
            columnTransformer.getType(), columnTransformer);
      case TIMESTAMP:
        return new TimestampNegationColumnTransformer(
            columnTransformer.getType(), columnTransformer);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }
  <#-- Parting line -->
  <#list mathematicalOperator.binaryOperators as operator>
  <#list mathematicalDataType.types as first>
    <#assign firstType =  first.type?replace("Type","")>
  <#-- The getTransformer method without Date and Timesatmp -->
  <#if firstType != "Date" && firstType != "Timestamp">

  private static ColumnTransformer get${firstType}${operator.name}Transformer(
      ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    List<? extends Type> argumentTypes =
    Arrays.asList(leftTransformer.getType(), rightTransformer.getType());
    switch (rightTransformer.getType().getTypeEnum()) {
      case INT32:
        return new ${firstType}${operator.name}IntColumnTransformer(
            ${operator.name}Resolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer);
      case INT64:
        return new ${firstType}${operator.name}LongColumnTransformer(
            ${operator.name}Resolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer);
      case FLOAT:
        return new ${firstType}${operator.name}FloatColumnTransformer(
            ${operator.name}Resolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer);
      case DOUBLE:
        return new ${firstType}${operator.name}DoubleColumnTransformer(
            ${operator.name}Resolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer);
      <#if operator.name == "Addition" && (firstType == "Int" || firstType == "Long")>
      case DATE:
        return new ${firstType}${operator.name}DateColumnTransformer(
            ${operator.name}Resolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      case TIMESTAMP:
        return new ${firstType}${operator.name}TimestampColumnTransformer(
            ${operator.name}Resolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      </#if>
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }
  </#if>
  </#list>
  </#list>

  private static ColumnTransformer getDateAdditionTransformer(
      ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    List<? extends Type> argumentTypes =
    Arrays.asList(leftTransformer.getType(), rightTransformer.getType());
    switch (rightTransformer.getType().getTypeEnum()) {
      case INT32:
        return new DateAdditionIntColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      case INT64:
        return new DateAdditionLongColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  private static ColumnTransformer getTimestampAdditionTransformer(
      ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    List<? extends Type> argumentTypes =
    Arrays.asList(leftTransformer.getType(), rightTransformer.getType());
    switch (rightTransformer.getType().getTypeEnum()) {
      case INT32:
        return new TimestampAdditionIntColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      case INT64:
        return new TimestampAdditionLongColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  private static ColumnTransformer getDateSubtractionTransformer(
      ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    List<? extends Type> argumentTypes =
    Arrays.asList(leftTransformer.getType(), rightTransformer.getType());
    switch (rightTransformer.getType().getTypeEnum()) {
      case INT32:
        return new DateSubtractionIntColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      case INT64:
        return new DateSubtractionLongColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  private static ColumnTransformer getTimestampSubtractionTransformer(
      ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    List<? extends Type> argumentTypes =
    Arrays.asList(leftTransformer.getType(), rightTransformer.getType());
    switch (rightTransformer.getType().getTypeEnum()) {
      case INT32:
        return new TimestampSubtractionIntColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      case INT64:
        return new TimestampSubtractionLongColumnTransformer(
            AdditionResolver.checkConditions(argumentTypes).get(),
            leftTransformer,
            rightTransformer,
            zoneId);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }
}
