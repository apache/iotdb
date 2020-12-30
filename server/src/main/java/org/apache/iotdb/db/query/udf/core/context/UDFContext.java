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

package org.apache.iotdb.db.query.udf.core.context;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDFContext {

  private final String name;
  private final Map<String, String> attributes;

  private List<PartialPath> paths;
  private List<TSDataType> dataTypes;

  private String columnParameterPart;
  private String column;

  public UDFContext(String name) {
    this.name = name;
    attributes = new LinkedHashMap<>();
    paths = new ArrayList<>();
  }

  public UDFContext(String name, Map<String, String> attributes, List<PartialPath> paths) {
    this.name = name;
    this.attributes = attributes;
    this.paths = paths;
  }

  public void addAttribute(String key, String value) {
    attributes.put(key, value);
  }

  public void addPath(PartialPath path) {
    paths.add(path);
  }

  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public List<PartialPath> getPaths() {
    return paths;
  }

  public List<TSDataType> getDataTypes() throws MetadataException {
    if (dataTypes == null) {
      dataTypes = new ArrayList<>();
      for (PartialPath path : paths) {
        dataTypes.add(IoTDB.metaManager.getSeriesType(path));
      }
    }
    return dataTypes;
  }

  /**
   * Generates the column name of the udf query.
   */
  public String getColumnName() {
    if (column == null) {
      column = name + "(" + getColumnNameParameterPart() + ")";
    }
    return column;
  }

  /**
   * Generates the parameter part of the udf column name.
   * <p>
   * Example:
   * <br>Full column name   -> udf(root.sg.d.s1, root.sg.d.s1, 'key1'='value1', 'key2'='value2')
   * <br>The parameter part -> root.sg.d.s1, root.sg.d.s1, 'key1'='value1', 'key2'='value2'
   */
  private String getColumnNameParameterPart() {
    if (columnParameterPart == null) {
      StringBuilder builder = new StringBuilder();
      if (!paths.isEmpty()) {
        builder.append(paths.get(0).getFullPath());
        for (int i = 1; i < paths.size(); ++i) {
          builder.append(", ").append(paths.get(i).getFullPath());
        }
      }
      if (!attributes.isEmpty()) {
        if (!paths.isEmpty()) {
          builder.append(", ");
        }
        Iterator<Entry<String, String>> iterator = attributes.entrySet().iterator();
        Entry<String, String> entry = iterator.next();
        builder.append("\"").append(entry.getKey()).append("\"=\"").append(entry.getValue())
            .append("\"");
        while (iterator.hasNext()) {
          entry = iterator.next();
          builder.append(", ").append("\"").append(entry.getKey()).append("\"=\"")
              .append(entry.getValue()).append("\"");
        }
      }
      columnParameterPart = builder.toString();
    }
    return columnParameterPart;
  }
}
