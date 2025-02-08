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
<#list mathematicalDataType.types as type>
  <#assign newType = type.type?replace("Type","")>
  <#assign className = "${newType}NegationColumnTransformer">
<#if newType != "Date">
  <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/transformation/dag/column/unary/${className}.java" />
package org.apache.iotdb.db.queryengine.transformation.dag.column.unary;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;

public class ${className} extends UnaryColumnTransformer {

  public ${className}(
    Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType, childColumnTransformer);
  }

  @Override
  protected void doTransform(
      Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        returnType.write${type.dataType?cap_first}(
            columnBuilder, transform(childColumnTransformer.getType().get${type.dataType?cap_first}(column, i)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        returnType.write${type.dataType?cap_first}(
            columnBuilder, transform(childColumnTransformer.getType().get${type.dataType?cap_first}(column, i)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public static ${type.dataType} transform(${type.dataType} value){
    <#if type.dataType == "int" || type.dataType == "long">
    if(value == <#if type.dataType == "int">Integer<#else>Long</#if>.MIN_VALUE){
      throw new IoTDBRuntimeException(String.format("The %s is out of range of ${type.dataType}.", value), NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(), true);
    }
    </#if>
    return -value;
  }
}
</#if>
</#list>
