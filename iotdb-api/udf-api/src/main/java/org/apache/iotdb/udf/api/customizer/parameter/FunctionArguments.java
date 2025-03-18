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

/**
 * FunctionArguments is used to provide the information of the function parameters to the UDF
 * implementation. It contains the data types of the child expressions, system attributes, etc.
 */
public class FunctionArguments {
  private final List<Type> argumentTypes;
  private final Map<String, String> systemAttributes;

  public FunctionArguments(
      List<Type> childExpressionDataTypes, Map<String, String> systemAttributes) {
    this.argumentTypes = childExpressionDataTypes;
    this.systemAttributes = systemAttributes;
  }

  /**
   * Get the data types of the arguments.
   *
   * @return a list of data types of the arguments
   */
  public List<Type> getArgumentTypes() {
    return argumentTypes;
  }

  /**
   * Get the number of the arguments.
   *
   * @return the number of the arguments
   */
  public int getArgumentsSize() {
    return argumentTypes.size();
  }

  /**
   * Get the data type of the input child expression at the specified index.
   *
   * @param index column index
   * @return the data type of the input child expression at the specified index
   */
  public Type getDataType(int index) {
    return argumentTypes.get(index);
  }

  /**
   * Check if the system attribute exists.
   *
   * @param attributeKey the key of the system attribute
   * @return true if the system attribute exists, false otherwise
   */
  public boolean hasSystemAttribute(String attributeKey) {
    return systemAttributes.containsKey(attributeKey);
  }

  /**
   * Get all the system attributes.
   *
   * @return a map of the system attributes
   */
  public Map<String, String> getSystemAttributes() {
    return systemAttributes;
  }
}
