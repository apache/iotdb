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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.cli.fs.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable TsFile-Cli compatible options for row reads. */
public final class ReadOptions {
  private final String format;
  private final String device;
  private final String table;
  private final List<String> columns;
  private final long limit;
  private final long offset;
  private final Long start;
  private final Long end;
  private final List<String> tagFilters;
  private final String tagMatch;

  public ReadOptions(
      String format,
      String device,
      String table,
      List<String> columns,
      long limit,
      long offset,
      Long start,
      Long end,
      List<String> tagFilters,
      String tagMatch) {
    this.format = format;
    this.device = device;
    this.table = table;
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.limit = limit;
    this.offset = offset;
    this.start = start;
    this.end = end;
    this.tagFilters = Collections.unmodifiableList(new ArrayList<>(tagFilters));
    this.tagMatch = tagMatch;
  }

  public String getFormat() {
    return format;
  }

  public String getDevice() {
    return device;
  }

  public String getTable() {
    return table;
  }

  public List<String> getColumns() {
    return columns;
  }

  public long getLimit() {
    return limit;
  }

  public long getOffset() {
    return offset;
  }

  public Long getStart() {
    return start;
  }

  public Long getEnd() {
    return end;
  }

  public List<String> getTagFilters() {
    return tagFilters;
  }

  public String getTagMatch() {
    return tagMatch;
  }
}
