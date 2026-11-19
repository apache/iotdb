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

import org.apache.iotdb.cli.i18n.FsReadMessages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
  private final List<String> aggregates;
  private final List<Window> windows;

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
    this(
        format,
        device,
        table,
        columns,
        limit,
        offset,
        start,
        end,
        tagFilters,
        tagMatch,
        Collections.emptyList(),
        Collections.emptyList());
  }

  private ReadOptions(
      String format,
      String device,
      String table,
      List<String> columns,
      long limit,
      long offset,
      Long start,
      Long end,
      List<String> tagFilters,
      String tagMatch,
      List<String> aggregates,
      List<Window> windows) {
    this.aggregates = Collections.unmodifiableList(new ArrayList<>(aggregates));
    this.windows = Collections.unmodifiableList(new ArrayList<>(windows));
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

  public ReadOptions withStatistics(String aggregates, String windows) {
    List<Window> ranges = parseWindows(windows);
    if (!ranges.isEmpty() && (start != null || end != null)) throw invalidWindows();
    return new ReadOptions(
        format,
        device,
        table,
        columns,
        limit,
        offset,
        start,
        end,
        tagFilters,
        tagMatch,
        aggregates.isEmpty() ? Collections.emptyList() : Arrays.asList(aggregates.split(",")),
        ranges);
  }

  public List<String> getAggregates() {
    return aggregates;
  }

  public List<Window> getWindows() {
    return windows;
  }

  public static List<Window> parseWindows(String value) {
    List<Window> result = new ArrayList<>();
    if (value.isEmpty()) return result;
    String[] items = value.split(",", -1);
    if (items.length > 256) throw invalidWindows();
    try {
      for (String item : items) {
        if (!item.matches("-?[0-9]+:-?[0-9]+")) throw invalidWindows();
        String[] bounds = item.split(":");
        long start = Long.parseLong(bounds[0]), end = Long.parseLong(bounds[1]);
        if (start > end) throw invalidWindows();
        result.add(new Window(start, end));
      }
    } catch (NumberFormatException e) {
      throw invalidWindows();
    }
    return result;
  }

  private static IllegalArgumentException invalidWindows() {
    return new IllegalArgumentException(
        FsReadMessages
            .EXCEPTION_EXPECTED_1_256_INCLUSIVE_EPOCH_WINDOWS_START_END_SEPARATED_BY_COMMAS_START_MUST_NOT_EXCEED_END_AND_WINDOWS_CANNOT_BE_COMBINED_WITH_START_END_DCB04A3E);
  }

  public static final class Window {
    private final long start;
    private final long end;

    private Window(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    public boolean contains(long time) {
      return time >= start && time <= end;
    }
  }

  public static ReadOptions defaults() {
    return new ReadOptions(
        "table",
        "",
        "",
        Collections.emptyList(),
        -1,
        0,
        null,
        null,
        Collections.emptyList(),
        "all");
  }

  public String timePredicate() {
    if (!windows.isEmpty()) {
      List<Window> sorted = new ArrayList<>(windows);
      sorted.sort(Comparator.comparingLong(Window::getStart));
      List<Window> merged = new ArrayList<>();
      long lower = sorted.get(0).start, upper = sorted.get(0).end;
      for (int i = 1; i < sorted.size(); i++) {
        Window next = sorted.get(i);
        if (upper == Long.MAX_VALUE || next.start <= upper + 1) {
          upper = Math.max(upper, next.end);
        } else {
          merged.add(new Window(lower, upper));
          lower = next.start;
          upper = next.end;
        }
      }
      merged.add(new Window(lower, upper));
      List<String> terms = new ArrayList<>();
      for (Window window : merged) {
        String term = "time >= " + window.start + " AND time <= " + window.end;
        terms.add(merged.size() == 1 ? term : "(" + term + ")");
      }
      return " WHERE " + String.join(" OR ", terms);
    }
    List<String> terms = new ArrayList<>();
    if (start != null) terms.add("time >= " + start);
    if (end != null) terms.add("time <= " + end);
    return terms.isEmpty() ? "" : " WHERE " + String.join(" AND ", terms);
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
