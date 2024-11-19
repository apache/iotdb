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

package org.apache.iotdb.udf.api.customizer.parameter;

import org.apache.iotdb.udf.api.type.Type;

import java.util.List;
import java.util.Map;

public class FunctionParameters {
  private final List<Type> childExpressionDataTypes;
  private final Map<String, String> systemAttributes;

  public FunctionParameters(
      List<Type> childExpressionDataTypes, Map<String, String> systemAttributes) {
    this.childExpressionDataTypes = childExpressionDataTypes;
    this.systemAttributes = systemAttributes;
  }

  public List<Type> getChildExpressionDataTypes() {
    return childExpressionDataTypes;
  }

  public int getChildExpressionsSize() {
    return childExpressionDataTypes.size();
  }

  public Type getDataType(int index) {
    return childExpressionDataTypes.get(index);
  }

  public boolean hasSystemAttribute(String attributeKey) {
    return systemAttributes.containsKey(attributeKey);
  }

  public Map<String, String> getSystemAttributes() {
    return systemAttributes;
  }
}
