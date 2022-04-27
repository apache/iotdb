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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;

public class TypeProvider {

  private final Map<String, TSDataType> typeMap;

  public TypeProvider() {
    this.typeMap = new HashMap<>();
  }

  public TSDataType getType(String path) {
    TSDataType type = typeMap.get(path);
    if (type == null) {
      throw new StatementAnalyzeException(String.format("no data type found for path: %s", path));
    }
    return type;
  }

  public void setType(String path, TSDataType dataType) {
    if (typeMap.containsKey(path) && typeMap.get(path) != dataType) {
      throw new StatementAnalyzeException(
          String.format("inconsistent data type for path: %s", path));
    }
    this.typeMap.put(path, dataType);
  }
}
