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

<#list allDataTypes.types as dataType>
    <#list compareTypes.types as compareType>

        <#if compareType.compareType == "variation">
            <#if dataType.dataType == "boolean">
                <#continue>
            </#if>
            <#if dataType.dataType == "Binary">
                <#continue>
            </#if>
        </#if>

        <#assign className = "${compareType.compareType?cap_first}Event${dataType.dataType?cap_first}Window">
        <@pp.changeOutputFile name="/org/apache/iotdb/db/mpp/execution/operator/window/${className}.java" />

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class ${className} extends Event${dataType.dataType?cap_first}Window {

  public ${className}(EventWindowParameter eventWindowParameter) {
    super(eventWindowParameter);
  }

  @Override
  public boolean satisfy(Column column, int index) {
    if (!initializedEventValue) {
      return true;
    }
    if(column.isNull(index)){
      return valueIsNull;
    }
    <#if compareType.compareType == "equal">
        <#if dataType.dataType == "Binary">
    return !valueIsNull&&column.get${dataType.dataType?cap_first}(index).equals(eventValue);
        <#else>
    return !valueIsNull&&column.get${dataType.dataType?cap_first}(index) == eventValue;
        </#if>
    <#else>
    return !valueIsNull&&Math.abs(column.get${dataType.dataType?cap_first}(index) - eventValue) <= eventWindowParameter.getDelta();
    </#if>
  }
}

    </#list>
</#list>