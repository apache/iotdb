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

<#list allDataTypes.types as type>

    <#assign className = "AbstractVariation${type.dataType?cap_first}Window">
    <@pp.changeOutputFile name="/org/apache/iotdb/db/mpp/execution/operator/window/${className}.java" />
package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
<#if type.dataType == "Binary">
import org.apache.iotdb.tsfile.utils.Binary;
</#if>

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
public abstract class ${className} extends AbstractVariationWindow {

  protected ${type.dataType} headValue;

  private ${type.dataType} previousValue;

  public ${className}(VariationWindowParameter variationWindowParameter) {
    super(variationWindowParameter);
  }

  @Override
  public void updatePreviousValue() {
    previousValue = headValue;
  }

  @Override
  public void mergeOnePoint(Column[] controlTimeAndValueColumn, int index) {
    long currentTime = controlTimeAndValueColumn[1].getLong(index);
    // judge whether we need update startTime
    if (startTime > currentTime) {
      startTime = currentTime;
    }
    // judge whether we need update endTime
    if (endTime < currentTime) {
      endTime = currentTime;
    }
    // judge whether we need initialize eventValue
    if (!initializedHeadValue) {
      startTime = currentTime;
      endTime = currentTime;
      if(controlTimeAndValueColumn[0].isNull(index)){
        valueIsNull = true;
      }else{
        valueIsNull = false;
        headValue = controlTimeAndValueColumn[0].get${type.dataType?cap_first}(index);
      }
      initializedHeadValue = true;
    }
  }

  public ${type.dataType} getHeadValue() {
    return headValue;
  }

  public ${type.dataType} getPreviousHeadValue() {
    return previousValue;
  }
}

</#list>