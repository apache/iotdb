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

package org.apache.iotdb.pipe.api.customizer.paramater;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.config.ConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.config.ProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.type.Type;

import java.util.List;
import java.util.Map;

/**
 * Used in {@link PipeProcessor#beforeStart(PipeParameters, ProcessorRuntimeConfiguration)} and
 * {@link PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)}.
 *
 * <p>This class is used to parse the parameters in WITH PROCESSOR and WITH CONNECTOR when creating
 * a pipe.
 */
public class PipeParameters {

  private final List<String> childExpressions;
  private final List<Type> childExpressionDataTypes;
  private final Map<String, String> attributes;

  public PipeParameters(
      List<String> childExpressions,
      List<Type> childExpressionDataTypes,
      Map<String, String> attributes) {
    this.childExpressions = childExpressions;
    this.childExpressionDataTypes = childExpressionDataTypes;
    this.attributes = attributes;
  }

  public List<String> getChildExpressions() {
    return childExpressions;
  }

  public Map<String, String> getAttribute() {
    return attributes;
  }

  public List<Type> getDataTypes() {
    return childExpressionDataTypes;
  }

  public Type getDataType(int index) {
    return childExpressionDataTypes.get(index);
  }

  public int getChildExpressionsSize() {
    return childExpressions.size();
  }

  public boolean hasAttribute(String key) {
    return attributes.containsKey(key);
  }

  public String getString(String key) {
    return attributes.get(key);
  }

  public Boolean getBoolean(String key) {
    String value = attributes.get(key);
    return value == null ? null : Boolean.parseBoolean(value);
  }

  public Integer getInt(String key) {
    String value = attributes.get(key);
    return value == null ? null : Integer.parseInt(value);
  }

  public Long getLong(String key) {
    String value = attributes.get(key);
    return value == null ? null : Long.parseLong(value);
  }

  public Float getFloat(String key) {
    String value = attributes.get(key);
    return value == null ? null : Float.parseFloat(value);
  }

  public Double getDouble(String key) {
    String value = attributes.get(key);
    return value == null ? null : Double.parseDouble(value);
  }

  public String getStringOrDefault(String key, String defaultValue) {
    String value = attributes.get(key);
    return value == null ? defaultValue : value;
  }

  public boolean getBooleanOrDefault(String key, boolean defaultValue) {
    String value = attributes.get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public int getIntOrDefault(String key, int defaultValue) {
    String value = attributes.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public long getLongOrDefault(String key, long defaultValue) {
    String value = attributes.get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public float getFloatOrDefault(String key, float defaultValue) {
    String value = attributes.get(key);
    return value == null ? defaultValue : Float.parseFloat(value);
  }

  public double getDoubleOrDefault(String key, double defaultValue) {
    String value = attributes.get(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }
}
