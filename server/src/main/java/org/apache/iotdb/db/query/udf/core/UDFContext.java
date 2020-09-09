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

package org.apache.iotdb.db.query.udf.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class UDFContext {

  private final String name;
  private final Map<String, String> attributes;
  private final List<String> attributeKeysInOriginalOrder;
  private List<Path> paths;
  private List<TSDataType> dataTypes;

  private String columnParameterPart;
  private String column;

  public UDFContext(String name) {
    this.name = name;
    attributes = new HashMap<>();
    attributeKeysInOriginalOrder = new ArrayList<>();
    paths = new ArrayList<>();
  }

  public UDFContext(String name, Map<String, String> attributes,
      List<String> attributeKeysInOriginalOrder, List<Path> paths) {
    this.name = name;
    this.attributes = attributes;
    this.attributeKeysInOriginalOrder = attributeKeysInOriginalOrder;
    this.paths = paths;
  }

  public void addAttribute(String key, String value) {
    attributes.put(key, value);
    attributeKeysInOriginalOrder.add(key);
  }

  public void addPath(Path path) {
    paths.add(path);
  }

  public void setPaths(List<Path> paths) {
    this.paths = paths;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public List<String> getAttributeKeysInOriginalOrder() {
    return attributeKeysInOriginalOrder;
  }

  public List<Path> getPaths() {
    return paths;
  }

  public List<TSDataType> getDataTypes() throws MetadataException {
    if (dataTypes == null) {
      dataTypes = new ArrayList<>();
      for (Path path : paths) {
        dataTypes.add(IoTDB.metaManager.getSeriesType(path.getFullPath()));
      }
    }
    return dataTypes;
  }

  public String getColumnParameterPart() {
    if (columnParameterPart == null) {
      StringBuilder builder = new StringBuilder(paths.get(0).getFullPath());
      for (int i = 1; i < paths.size(); ++i) {
        builder.append(", ").append(paths.get(i).getFullPath());
      }
      for (int i = 0; i < attributeKeysInOriginalOrder.size(); ++i) {
        String key = attributeKeysInOriginalOrder.get(0);
        builder.append(", ").append("\"").append(key).append("\"=\"").append(attributes.get(key))
            .append("\"");
      }
      columnParameterPart = builder.toString();
    }
    return columnParameterPart;
  }

  public String getColumn() {
    if (column == null) {
      column = name + "(" + getColumnParameterPart() + ")";
    }
    return column;
  }

  public String getColumnForReaderDeduplication(int index) {
    return getColumn() + paths.get(index).getFullPath();
  }
}
