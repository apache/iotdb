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

package org.apache.iotdb.cli.fs.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlRow {

  private final Map<String, String> values;

  public SqlRow(Map<String, String> values) {
    this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
  }

  public static SqlRow of(String... keyValues) {
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("SqlRow keyValues must contain pairs");
    }
    Map<String, String> values = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      values.put(keyValues[i], keyValues[i + 1]);
    }
    return new SqlRow(values);
  }

  public static List<SqlRow> list(SqlRow... rows) {
    return new ArrayList<>(Arrays.asList(rows));
  }

  public String get(String column) {
    return values.get(column);
  }

  public Map<String, String> asMap() {
    return values;
  }
}
