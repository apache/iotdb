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

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 *
 * <p>This class is used to parse the parameters in the UDF entered by the user.
 *
 * <p>The input parameters of UDF have two parts. The first part is the paths (measurements) of the
 * time series that the UDF needs to process, and the second part is the key-value pair attributes
 * for customization. Only the second part can be empty.
 *
 * <p>Note that the user must enter the paths (measurements) part before entering the attributes
 * part.
 */
public class UDFParameters {

  private final List<String> childExpressions;
  private final List<Type> childExpressionDataTypes;
  private final Map<String, String> userAttributes;
  private final Map<String, String> systemAttributes;

  public UDFParameters(
      List<String> childExpressions,
      List<Type> childExpressionDataTypes,
      Map<String, String> userAttributes) {
    this(childExpressions, childExpressionDataTypes, userAttributes, new HashMap<>());
  }

  public UDFParameters(
      List<String> childExpressions,
      List<Type> childExpressionDataTypes,
      Map<String, String> userAttributes,
      Map<String, String> systemAttributes) {
    this.childExpressions = childExpressions;
    this.childExpressionDataTypes = childExpressionDataTypes;
    this.userAttributes = userAttributes;
    this.systemAttributes = systemAttributes;
  }

  public List<String> getChildExpressions() {
    return childExpressions;
  }

  public Map<String, String> getAttributes() {
    return userAttributes;
  }

  public List<Type> getDataTypes() {
    return childExpressionDataTypes;
  }

  public int getChildExpressionsSize() {
    return childExpressions.size();
  }

  public Type getDataType(int index) {
    return childExpressionDataTypes.get(index);
  }

  public boolean hasAttribute(String attributeKey) {
    return userAttributes.containsKey(attributeKey);
  }

  public String getString(String key) {
    return userAttributes.get(key);
  }

  public Boolean getBoolean(String key) {
    String value = userAttributes.get(key);
    return value == null ? null : Boolean.parseBoolean(value);
  }

  public Integer getInt(String key) {
    String value = userAttributes.get(key);
    return value == null ? null : Integer.parseInt(value);
  }

  public Long getLong(String key) {
    String value = userAttributes.get(key);
    return value == null ? null : Long.parseLong(value);
  }

  public Float getFloat(String key) {
    String value = userAttributes.get(key);
    return value == null ? null : Float.parseFloat(value);
  }

  public Double getDouble(String key) {
    String value = userAttributes.get(key);
    return value == null ? null : Double.parseDouble(value);
  }

  public String getStringOrDefault(String key, String defaultValue) {
    String value = userAttributes.get(key);
    return value == null ? defaultValue : value;
  }

  public boolean getBooleanOrDefault(String key, boolean defaultValue) {
    String value = userAttributes.get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public int getIntOrDefault(String key, int defaultValue) {
    String value = userAttributes.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public long getLongOrDefault(String key, long defaultValue) {
    String value = userAttributes.get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public float getFloatOrDefault(String key, float defaultValue) {
    String value = userAttributes.get(key);
    return value == null ? defaultValue : Float.parseFloat(value);
  }

  public double getDoubleOrDefault(String key, double defaultValue) {
    String value = userAttributes.get(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }

  public boolean hasSystemAttribute(String attributeKey) {
    return systemAttributes.containsKey(attributeKey);
  }

  public Map<String, String> getSystemAttributes() {
    return systemAttributes;
  }

  public String getSystemString(String key) {
    return systemAttributes.get(key);
  }

  public Boolean getSystemBoolean(String key) {
    String value = systemAttributes.get(key);
    return value == null ? null : Boolean.parseBoolean(value);
  }

  public Integer getSystemInt(String key) {
    String value = systemAttributes.get(key);
    return value == null ? null : Integer.parseInt(value);
  }

  public Long getSystemLong(String key) {
    String value = systemAttributes.get(key);
    return value == null ? null : Long.parseLong(value);
  }

  public Float getSystemFloat(String key) {
    String value = systemAttributes.get(key);
    return value == null ? null : Float.parseFloat(value);
  }

  public Double getSystemDouble(String key) {
    String value = systemAttributes.get(key);
    return value == null ? null : Double.parseDouble(value);
  }

  public String getSystemStringOrDefault(String key, String defaultValue) {
    String value = systemAttributes.get(key);
    return value == null ? defaultValue : value;
  }

  public boolean getSystemBooleanOrDefault(String key, boolean defaultValue) {
    String value = systemAttributes.get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public int getSystemIntOrDefault(String key, int defaultValue) {
    String value = systemAttributes.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public long getSystemLongOrDefault(String key, long defaultValue) {
    String value = systemAttributes.get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public float getSystemFloatOrDefault(String key, float defaultValue) {
    String value = systemAttributes.get(key);
    return value == null ? defaultValue : Float.parseFloat(value);
  }

  public double getSystemDoubleOrDefault(String key, double defaultValue) {
    String value = systemAttributes.get(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }
}
