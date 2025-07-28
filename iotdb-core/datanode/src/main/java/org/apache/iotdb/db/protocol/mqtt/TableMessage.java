/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.protocol.mqtt;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;

/** Message parsing into a table */
public class TableMessage extends Message {

  private String database;

  private String table;

  private List<String> tagKeys;

  private List<Object> tagValues;

  private List<String> attributeKeys;

  private List<Object> attributeValues;

  private List<String> fields;

  private List<TSDataType> dataTypes;

  private List<Object> values;

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<String> getTagKeys() {
    return tagKeys;
  }

  public void setTagKeys(List<String> tagKeys) {
    this.tagKeys = tagKeys;
  }

  public List<Object> getTagValues() {
    return tagValues;
  }

  public void setTagValues(List<Object> tagValues) {
    this.tagValues = tagValues;
  }

  public List<String> getAttributeKeys() {
    return attributeKeys;
  }

  public void setAttributeKeys(List<String> attributeKeys) {
    this.attributeKeys = attributeKeys;
  }

  public List<Object> getAttributeValues() {
    return attributeValues;
  }

  public void setAttributeValues(List<Object> attributeValues) {
    this.attributeValues = attributeValues;
  }

  public List<String> getFields() {
    return fields;
  }

  public void setFields(List<String> fields) {
    this.fields = fields;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<Object> getValues() {
    return values;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

  @Override
  public String toString() {
    return "TableMessage{"
        + "database='"
        + database
        + '\''
        + ", table='"
        + table
        + '\''
        + ", tagKeys="
        + tagKeys
        + ", tagValues="
        + tagValues
        + ", attributeKeys="
        + attributeKeys
        + ", attributeValues="
        + attributeValues
        + ", fields="
        + fields
        + ", dataTypes="
        + dataTypes
        + ", values="
        + values
        + ", timestamp="
        + timestamp
        + '}';
  }
}
