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

        <#assign className = "${compareType.compareType?cap_first}${dataType.dataType?cap_first}WindowManager">
        <#assign windowName = "${compareType.compareType?cap_first}${dataType.dataType?cap_first}Window">
        <@pp.changeOutputFile name="/org/apache/iotdb/db/mpp/execution/operator/window/${className}.java" />

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
<#if dataType.dataType == "Binary">
import org.apache.iotdb.tsfile.utils.Binary;
</#if>

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
public class ${className} extends VariationWindowManager {

  public ${className}(
      VariationWindowParameter variationWindowParameter, boolean ascending) {
    super(ascending);
    variationWindow = new ${windowName}(variationWindowParameter);
  }

  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {
    if (!needSkip) {
      return inputTsBlock;
    }

    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    Column controlColumn = variationWindow.getControlColumn(inputTsBlock);
    TimeColumn timeColumn = inputTsBlock.getTimeColumn();
    int i = 0, size = inputTsBlock.getPositionCount();
    ${dataType.dataType} previousValue = ((${windowName}) variationWindow).getPreviousHeadValue();
    boolean previousValueIsNull = ((${windowName}) variationWindow).valueIsNull();
    for (; i < size; i++) {
      // condition must be initialized when isNull is false
      boolean condition = false;
      boolean isNull = controlColumn.isNull(i);
      <#if compareType.compareType == "equal">
          <#if dataType.dataType == "Binary">
      if(!isNull) condition = !controlColumn.get${dataType.dataType?cap_first}(i).equals(previousValue);
          <#else>
      if(!isNull) condition = controlColumn.get${dataType.dataType?cap_first}(i) != previousValue;
          </#if>
      <#else>
      if(!isNull) condition = Math.abs(controlColumn.get${dataType.dataType?cap_first}(i) - previousValue)
          > variationWindow.getDelta();
      </#if>
      if(isIgnoringNull()){
        if (!isNull && condition) {
            break;
        }else if(isNull){
            continue;
        }
      }else{
        if((isNull&&!previousValueIsNull)||!isNull&&previousValueIsNull||(!isNull&&condition)){
            break;
        }
      }

      long currentTime = timeColumn.getLong(i);
      // judge whether we need update startTime
      if (variationWindow.getStartTime() > currentTime) {
        variationWindow.setStartTime(currentTime);
      }
      // judge whether we need update endTime
      if (variationWindow.getEndTime() < currentTime) {
        variationWindow.setEndTime(currentTime);
      }
    }
    // we can create a new window beginning at index i of inputTsBlock
    if (i < size) {
      needSkip = false;
    }
    return inputTsBlock.subTsBlock(i);
  }
}

    </#list>
</#list>