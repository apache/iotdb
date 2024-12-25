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
<#list mathematicalOperator.binaryOperators as operator>
<#list mathematicalDataType.types as first>
<#list mathematicalDataType.types as second>
<#--Parting line-->
<#assign className = "${first.type?replace('Type','')}${operator.name}${second.type?replace('Type','')}ColumnTransformer">

<#--Main Part-->
<#if (first.instance == "DATE" || first.instance == "TIMESTAMP") && (second.instance == "INT" || second.instance == "LONG")><#if operator.name == "Addition" || operator.name == "Subtraction">
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/column/binary/${className}.java" />
<#--Date + int || Date + long || Timestamp + int || Timestamp + long-->
<#--Date - int || Date - long || Timestamp - int || Timestamp - long-->
package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.DateUtils;

import java.time.ZoneId;

<#if first.instance == "DATE">
import java.time.format.DateTimeParseException;

import static org.apache.iotdb.rpc.TSStatusCode.DATE_OUT_OF_RANGE;
</#if>
<#if first.dataType == "int" || second.dataType == "int" || first.dataType == "long" || second.dataType == "long">
import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;
</#if>

public class ${className} extends BinaryColumnTransformer {

  private static ZoneId zoneId;

  public ${className}(
    Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    super(returnType, leftTransformer, rightTransformer);
    this.zoneId = zoneId;
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        returnType.write${first.dataType?cap_first}(
            builder,
            transform(
                leftTransformer.getType().get${first.dataType?cap_first}(leftColumn, i),
                rightTransformer.getType().get${second.dataType?cap_first}(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection){
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i) && selection[i]) {
        returnType.write${first.dataType?cap_first}(
            builder,
            transform(
                leftTransformer.getType().get${first.dataType?cap_first}(leftColumn, i),
                rightTransformer.getType().get${second.dataType?cap_first}(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public static ${first.dataType} transform(${first.dataType} left, ${second.dataType} right) {
    <#switch operator.name>
    <#case "Addition">
    <#if first.instance == "DATE">
    <#--Date + int || Date + long-->
    try{
      long timestamp =
          Math.addExact(
              DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(left,zoneId)), right);
      return DateUtils.parseDateExpressionToInt(
          DateTimeUtils.convertToLocalDate(timestamp, zoneId));
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("long ${operator.name} overflow: %s + %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }catch (DateTimeParseException e) {
      throw new IoTDBRuntimeException(
          "Year must be between 1000 and 9999.",
          DATE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    <#else>
    <#--Timestamp + int || Timestamp + long-->
    try{
      return Math.addExact(left, right);
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(String.format("long ${operator.name} overflow: %s + %s", left, right),NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),true);
    }
    </#if>
    <#break>
    <#case "Subtraction">
    <#if first.instance == "DATE">
    <#--Date - int || Date - long-->
    try{
      long timestamp =
          Math.subtractExact(
              DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(left, zoneId)), right);
      return DateUtils.parseDateExpressionToInt(
          DateTimeUtils.convertToLocalDate(timestamp, zoneId));
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("long ${operator.name} overflow: %s - %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }catch (DateTimeParseException e) {
      throw new IoTDBRuntimeException(
          "Year must be between 1000 and 9999.",
          DATE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    <#else>
    <#--Timestamp - int || Timestamp - long-->
    try{
      return Math.subtractExact(left, right);
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("long ${operator.name} overflow: %s - %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    </#if>
    <#break>
    </#switch>
  }
}
</#if>
<#elseif (second.instance == "DATE" || second.instance =="TIMESTAMP") && (first.instance == "INT" || first.instance == "LONG")>
  <#if operator.name == "Addition">
<#--int + Date || long + Date || int + Timestamp || long + Timestamp-->
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/column/binary/${className}.java" />
package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.DateUtils;

import java.time.ZoneId;

<#if first.dataType == "int" || second.dataType == "int" || first.dataType == "long" || second.dataType == "long">
import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;
</#if>

public class ${className} extends BinaryColumnTransformer {

  private static ZoneId zoneId;

  public ${className}(
    Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer, ZoneId zoneId) {
    super(returnType, leftTransformer, rightTransformer);
    this.zoneId = zoneId;
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        returnType.write${second.dataType?cap_first}(
            builder,
            transform(
                leftTransformer.getType().get${first.dataType?cap_first}(leftColumn, i),
                rightTransformer.getType().get${second.dataType?cap_first}(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection){
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i) && selection[i]) {
        returnType.write${second.dataType?cap_first}(
            builder,
            transform(
                leftTransformer.getType().get${first.dataType?cap_first}(leftColumn, i),
                rightTransformer.getType().get${second.dataType?cap_first}(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public static ${second.dataType} transform(${first.dataType} left, ${second.dataType} right) {
    <#if second.instance == "DATE">
    try{
      long timestamp = Math.addExact(left,DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(right, zoneId)));
      return DateUtils.parseDateExpressionToInt(DateTimeUtils.convertToLocalDate(timestamp, zoneId));
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("long ${operator.name} overflow: %s + %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    <#else>
    try{
      return Math.addExact(left, right);
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("long ${operator.name} overflow: %s + %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    </#if>
  }
}
</#if>
<#elseif first.instance != "DATE" && first.instance != "TIMESTAMP" && second.instance != "DATE" && second.instance != "TIMESTAMP">
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/column/binary/${className}.java" />
<#--int、long、float、double with + - * / %-->
<#--assign resultType-->
<#if first.dataType == "double" || second.dataType == "double">
  <#assign resultType = "double" />
<#elseif first.dataType == "float" || second.dataType == "float">
  <#assign resultType = "float" />
<#elseif first.dataType == "long" || second.dataType == "long">
  <#assign resultType = "long" />
<#else>
  <#assign resultType = "int" />
</#if>
<#--Parting line-->
package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

<#if operator.name == "Division" || operator.name == "Modulus">
import static org.apache.iotdb.rpc.TSStatusCode.DIVISION_BY_ZERO;
</#if>
<#if first.dataType == "int" || second.dataType == "int" || first.dataType == "long" || second.dataType == "long">
import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;
</#if>

public class ${className} extends BinaryColumnTransformer {
  public ${className}(
    Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        returnType.write${resultType?cap_first}(
            builder,
            transform(
                leftTransformer.getType().get${first.dataType?cap_first}(leftColumn, i),
                rightTransformer.getType().get${second.dataType?cap_first}(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection){
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i) && selection[i]) {
        returnType.write${resultType?cap_first}(
            builder,
            transform(
                leftTransformer.getType().get${first.dataType?cap_first}(leftColumn, i),
                rightTransformer.getType().get${second.dataType?cap_first}(rightColumn, i)));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public static ${resultType} transform(${first.dataType} left, ${second.dataType} right) {
    <#if (first.dataType == "int" || first.dataType == "long") && (second.dataType == "int" || second.dataType =="long")>
    <#switch operator.name>
    <#case "Addition">
    try{
      return Math.addExact(left, right);
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("${resultType} ${operator.name} overflow: %s + %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          false);
    }
    <#break>
    <#case "Subtraction">
    try{
      return Math.subtractExact(left, right);
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("${resultType} ${operator.name} overflow: %s - %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          false);
    }
    <#break>
    <#case "Multiplication">
    try{
      return Math.multiplyExact(left, right);
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException(
          String.format("${resultType} ${operator.name} overflow: %s * %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    <#break>
    <#case "Division">
    try{
      if (left == <#if first.dataType == "int">Integer.MIN_VALUE<#elseif first.dataType == "long">Long.MIN_VALUE</#if> && right == -1) {
        throw new IoTDBRuntimeException(
          String.format("${resultType} overflow: %s / %s", left, right),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
      }
      return left / right;
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException("Division by zero",DIVISION_BY_ZERO.getStatusCode(),true);
    }
    <#break>
    <#case "Modulus">
    try{
      return left % right;
    }catch (ArithmeticException e){
      throw new IoTDBRuntimeException("Division by zero",DIVISION_BY_ZERO.getStatusCode(),true);
    }
    <#break>
    </#switch>
    <#else>
    return left ${operator.symbol} right;
    </#if>
  }
}
</#if>
<#--Parting line-->
</#list>
</#list>
</#list>