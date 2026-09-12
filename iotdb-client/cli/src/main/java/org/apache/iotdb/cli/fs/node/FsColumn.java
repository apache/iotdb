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

package org.apache.iotdb.cli.fs.node;

import java.util.Locale;

/** The logical column schema, independent of JDBC's display labels. */
public final class FsColumn {
  private final String name;
  private final String category;
  private final String dataType;
  private final String encoding;
  private final String compression;

  public FsColumn(String name, String category, String dataType) {
    this(name, category, dataType, null, null);
  }

  public FsColumn(
      String name, String category, String dataType, String encoding, String compression) {
    this.name = name;
    this.category = category == null ? "FIELD" : category.toUpperCase(Locale.ROOT);
    this.dataType = dataType == null ? "STRING" : dataType.toUpperCase(Locale.ROOT);
    this.encoding = encoding;
    this.compression = compression;
  }

  public String getName() {
    return name;
  }

  public String getCategory() {
    return category;
  }

  public String getDataType() {
    return dataType;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getCompression() {
    return compression;
  }
}
