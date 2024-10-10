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
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
<@pp.dropOutputFile />

<#list allDataTypes.types as type>

  <#assign className = "${type.dataType?cap_first}ConstantFill">
  <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/execution/operator/process/fill/constant/${className}.java" />
package org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant;

import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;
import org.apache.tsfile.read.common.block.column.${type.column};
import org.apache.tsfile.block.column.Column;
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

  // fill value
  private final ${type.dataType} value;
  // used for constructing RunLengthEncodedColumn, size of it must be 1
  private final ${type.dataType}[] valueArray;

  public ${className}(${type.dataType} value) {
    this.value = value;
    this.valueArray = new ${type.dataType}[] {value};
  }

  @Override
  public Column fill(Column timeColumn, Column valueColumn) {
    int size = valueColumn.getPositionCount();
    // if this valueColumn doesn't have any null value, or it's empty, just return itself;
    if (!valueColumn.mayHaveNull() || size == 0) {
      return valueColumn;
    }
    // if its values are all null
    if (valueColumn instanceof RunLengthEncodedColumn) {
      return new RunLengthEncodedColumn(new ${type.column}(1, Optional.empty(), valueArray), size);
    } else {
      ${type.dataType}[] array = new ${type.dataType}[size];
      for (int i = 0; i < size; i++) {
        if (valueColumn.isNull(i)) {
          array[i] = value;
        } else {
          array[i] = valueColumn.get${type.dataType?cap_first}(i);
        }
      }
      return new ${type.column}(size, Optional.empty(), array);
    }
  }

  @Override
  public void reset() {
    // do nothing
  }
}

</#list>
