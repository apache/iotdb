<@pp.dropOutputFile />
<#list mathematicalOperator.binaryOperators as operator>
<#list decimalDataTypes.types as first>
<#list decimalDataTypes.types as second>
<#assign className = "${first.dataType?cap_first}${operator.name}${second.dataType?cap_first}ColumnTransformer">
<#if first.dataType == "double" || second.dataType == "double">
  <#assign resultType = "double" />
<#elseif first.dataType == "float" || second.dataType == "float">
  <#assign resultType = "float" />
<#elseif first.dataType == "long" || second.dataType == "long">
  <#assign resultType = "long" />
<#else>
  <#assign resultType = "int" />
</#if>
<@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/column/binary/${className}.java" />
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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

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
  protected void checkType() {
    if (!leftTransformer.isReturnTypeNumeric() || !rightTransformer.isReturnTypeNumeric()) {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  protected ${resultType} transform(${first.dataType} left, ${second.dataType} right) {
    <#if (first.dataType == "int" || first.dataType == "long") && (second.dataType == "int" || second.dataType =="long")>
    <#switch operator.name>
    <#case "Addition">
    return Math.addExact(left, right);
    <#break>
    <#case "Subtraction">
    return Math.subtractExact(left, right);
    <#break>
    <#case "Multiplication">
    return Math.multiplyExact(left, right);
    <#break>
    <#case "Division">
    try{
      if (left == <#if first.dataType == "int">Integer.MIN_VALUE<#elseif first.dataType == "long">Long.MIN_VALUE</#if> && right == -1) {
        throw new ArithmeticException("overflow");
      }
      return left / right;
    }catch (ArithmeticException e){
      throw new ArithmeticException("Division by zero");
    }
    <#break>
    <#case "Modulus">
    try{
      return left % right;
    }catch (ArithmeticException e){
      throw new ArithmeticException("Modulus by zero");
    }
    <#break>
    </#switch>
    <#else>
    return left ${operator.symbol} right;
    </#if>
  }
}
</#list>
</#list>
</#list>