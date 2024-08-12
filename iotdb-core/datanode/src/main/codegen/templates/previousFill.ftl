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

<#list allDataTypes.types as type>

  <#assign className = "${type.dataType?cap_first}PreviousFill">
  <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/execution/operator/process/fill/previous/${className}.java" />
package org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous;

import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFillFilter;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.${type.column};
import org.apache.tsfile.read.common.block.column.${type.column}Builder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
<#if type.dataType == "Binary">
  import org.apache.tsfile.utils.Binary;
</#if>

import java.util.Optional;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
public class ${className} implements IFill {

  // previous value
  private ${type.dataType} value;
  // previous time
  private long previousTime;
  // whether previous value is null
  private boolean previousIsNull = true;

  private final IFillFilter filter;

  public ${className}(IFillFilter filter) {
    this.filter = filter;
  }

  @Override
  public Column fill(Column timeColumn, Column valueColumn) {
    int size = valueColumn.getPositionCount();
    // if this valueColumn is empty, just return itself;
    if (size == 0) {
      return valueColumn;
    }
    // if this valueColumn doesn't have any null value, record the last value, and then return
    // itself.
    if (!valueColumn.mayHaveNull()) {
      previousIsNull = false;
      // update the value using last non-null value
      previousTime = timeColumn.getLong(size - 1);
      value = valueColumn.get${type.dataType?cap_first}(size - 1);
      return valueColumn;
    }
    // if its values are all null
    if (valueColumn instanceof RunLengthEncodedColumn) {
      if (previousIsNull) {
        return new RunLengthEncodedColumn(${type.column}Builder.NULL_VALUE_BLOCK, size);
      } else if (filter.needFill(timeColumn.getLong(size - 1), previousTime)) {
        return new RunLengthEncodedColumn(
            new ${type.column}(1, Optional.empty(), new ${type.dataType}[] {value}), size);
      }
    }

    ${type.dataType}[] array = new ${type.dataType}[size];
    boolean[] isNull = new boolean[size];
    // have null value
    boolean hasNullValue = false;
    for (int i = 0; i < size; i++) {
      if (valueColumn.isNull(i)) {
        if (previousIsNull || !filter.needFill(timeColumn.getLong(i), previousTime)) {
          isNull[i] = true;
          hasNullValue = true;
        } else {
          array[i] = value;
        }
      } else {
        array[i] = valueColumn.get${type.dataType?cap_first}(i);
        previousTime = timeColumn.getLong(i);
        value = array[i];
        previousIsNull = false;
      }
    }
    if (hasNullValue) {
      return new ${type.column}(size, Optional.of(isNull), array);
    } else {
      return new ${type.column}(size, Optional.empty(), array);
    }
  }
}
</#list>
