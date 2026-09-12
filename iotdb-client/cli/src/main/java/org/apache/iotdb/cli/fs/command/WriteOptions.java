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

package org.apache.iotdb.cli.fs.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Schema and input/output settings shared by the write parser and executor. */
public final class WriteOptions {
  private final String table;
  private final List<Column> columns;
  private final String input;
  private final String output;
  private final boolean verbose;
  private final Map<String, String> encodings;
  private final Map<String, String> compressions;

  WriteOptions(
      String table,
      List<Column> columns,
      String input,
      String output,
      boolean verbose,
      Map<String, String> encodings,
      Map<String, String> compressions) {
    this.table = table;
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.input = input;
    this.output = output;
    this.verbose = verbose;
    this.encodings = Collections.unmodifiableMap(new LinkedHashMap<>(encodings));
    this.compressions = Collections.unmodifiableMap(new LinkedHashMap<>(compressions));
  }

  public String getTable() {
    return table;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public String getInput() {
    return input;
  }

  public String getOutput() {
    return output;
  }

  public boolean isStdin() {
    return "-".equals(input);
  }

  public boolean isVerbose() {
    return verbose;
  }

  public Map<String, String> getEncodings() {
    return encodings;
  }

  public Map<String, String> getCompressions() {
    return compressions;
  }

  public static final class Column {
    private final String name;
    private final String type;
    private final String category;

    Column(String name, String type, String category) {
      this.name = name;
      this.type = type;
      this.category = category;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public String getCategory() {
      return category;
    }
  }
}
