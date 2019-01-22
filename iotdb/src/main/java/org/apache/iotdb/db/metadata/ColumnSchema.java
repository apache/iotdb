/**
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
package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class ColumnSchema implements Serializable {

  private static final long serialVersionUID = -8257474930341487207L;

  public String name;
  public TSDataType dataType;
  public TSEncoding encoding;
  private Map<String, String> args;

  /**
   * constructor of ColumnSchema.
   *
   * @param name name
   * @param dataType time series data type
   * @param encoding time series data encoding type
   */
  public ColumnSchema(String name, TSDataType dataType, TSEncoding encoding) {
    this.name = name;
    this.dataType = dataType;
    this.encoding = encoding;
    this.args = new HashMap<>();
  }

  public void putKeyValueToArgs(String key, String value) {
    this.args.put(key, value);
  }

  public String getValueFromArgs(String key) {
    return args.get(key);
  }

  public String getName() {
    return name;
  }

  public TSDataType geTsDataType() {
    return dataType;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public Map<String, String> getArgsMap() {
    return args;
  }

  public void setArgsMap(Map<String, String> argsMap) {
    this.args = argsMap;
  }

}
