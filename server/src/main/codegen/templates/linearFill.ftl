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
<@pp.dropOutputFile />

<#list decimalDataTypes.types as type>

  <#assign className = "${type.dataType?cap_first}LinearFill">
  <@pp.changeOutputFile name="/org/apache/iotdb/db/mpp/execution/operator/process/fill/linear/${className}.java" />
package org.apache.iotdb.db.mpp.execution.operator.process.fill.linear;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.${type.column};
import org.apache.iotdb.tsfile.read.common.block.column.${type.column}Builder;

import java.util.Optional;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
public class ${className} extends LinearFill {

  // previous value
  private ${type.dataType} previousValue;
  // next non-null value whose time is closest to the current TsBlock's endTime
  private ${type.dataType} nextValue;

  private ${type.dataType} nextValueInCurrentColumn;

  @Override
  void fillValue(Column column, int index, Object array) {
    ((${type.dataType}[]) array)[index] = column.get${type.dataType?cap_first}(index);
  }

  @Override
  void fillValue(Object array, int index) {
    ((${type.dataType}[]) array)[index] = getFilledValue();
  }

  @Override
  Object createValueArray(int size) {
    return new ${type.dataType}[size];
  }

  @Override
  Column createNullValueColumn() {
    return ${type.column}Builder.NULL_VALUE_BLOCK;
  }

  @Override
  Column createFilledValueColumn() {
    ${type.dataType} filledValue = getFilledValue();
    return new ${type.column}(1, Optional.empty(), new ${type.dataType}[] {filledValue});
  }

  @Override
  Column createFilledValueColumn(Object array, boolean[] isNull, boolean hasNullValue, int size) {
    if (hasNullValue) {
      return new ${type.column}(size, Optional.of(isNull), (${type.dataType}[]) array);
    } else {
      return new ${type.column}(size, Optional.empty(), (${type.dataType}[]) array);
    }
  }

  @Override
  void updatePreviousValue(Column column, int index) {
    previousValue = column.get${type.dataType?cap_first}(index);
  }

  @Override
  void updateNextValue(Column nextValueColumn, int index) {
    this.nextValue = nextValueColumn.get${type.dataType?cap_first}(index);
  }

  @Override
  void updateNextValueInCurrentColumn(Column nextValueColumn, int index) {
    this.nextValueInCurrentColumn = nextValueColumn.get${type.dataType?cap_first}(index);
  }

  @Override
  void updateNextValueInCurrentColumn() {
    this.nextValueInCurrentColumn = this.nextValue;
  }

  private ${type.dataType} getFilledValue() {
    <#if type.dataType == "double">
    return (previousValue + nextValueInCurrentColumn) / 2.0;
    <#elseif type.dataType == "float">
    return (previousValue + nextValueInCurrentColumn) / 2.0f;
    <#else>
    return (previousValue + nextValueInCurrentColumn) / 2;
    </#if>
  }
}

</#list>
