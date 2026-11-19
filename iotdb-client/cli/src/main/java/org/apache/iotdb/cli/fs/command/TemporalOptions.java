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

import org.apache.iotdb.cli.i18n.FsTemporalMessages;

import java.util.Collections;

/** Explicit entity-local semantics shared by native temporal commands. */
public final class TemporalOptions {
  public enum Mode {
    GAPS,
    SESSIONS,
    EXTREME
  }

  private final Mode mode;
  private final ReadOptions read;
  private final String field;
  private final boolean maximum;
  private final long gap;

  public TemporalOptions(Mode mode, ReadOptions read, String field, String extremum, long gap) {
    if (field == null
        || field.isEmpty()
        || read.getLimit() < -1
        || read.getOffset() < 0
        || (read.getStart() != null && read.getEnd() != null && read.getStart() > read.getEnd())
        || (mode == Mode.SESSIONS && (gap <= 0 || !read.getWindows().isEmpty()))
        || (mode == Mode.EXTREME && !"min".equals(extremum) && !"max".equals(extremum)))
      throw invalid();
    this.mode = mode;
    this.read = read;
    this.field = field;
    this.maximum = "max".equals(extremum);
    this.gap = gap;
  }

  public static TemporalOptions from(FilesystemCommand command) {
    long gap = 0;
    try {
      if (command.hasOption("--gap-ms")) gap = Long.parseLong(command.optionValue("--gap-ms", ""));
    } catch (NumberFormatException e) {
      throw invalid();
    }
    return new TemporalOptions(
        Mode.valueOf(command.getType().name()),
        command.getReadOptions(),
        command.optionValue("--field", ""),
        command.optionValue("--mode", ""),
        gap);
  }

  public static IllegalArgumentException invalid() {
    return new IllegalArgumentException(
        FsTemporalMessages
            .EXCEPTION_EXPECTED_GAPS_SESSIONS_EXTREME_WITH_FIELD_SESSIONS_REQUIRES_POSITIVE_GAP_MS_AND_NO_WINDOWS_EXTREME_REQUIRES_MODE_MIN_MAX_BOUNDS_ARE_INCLUSIVE_WINDOWS_EXCLUDE_START_END_AND_OUTPUT_LIMIT_OFFSET_MUST_BE_NONNEGATIVE_BACA632C);
  }

  public Mode getMode() {
    return mode;
  }

  public ReadOptions getRead() {
    return read;
  }

  public String getField() {
    return field;
  }

  public boolean isMaximum() {
    return maximum;
  }

  public long getGap() {
    return gap;
  }

  /** Gaps need the actual preceding non-null observation, even outside all output windows. */
  public ReadOptions scanOptions() {
    if (mode != Mode.GAPS) return read;
    Long end = read.getEnd();
    if (!read.getWindows().isEmpty())
      end = read.getWindows().stream().mapToLong(ReadOptions.Window::getEnd).max().getAsLong();
    return new ReadOptions(
        read.getFormat(),
        "",
        "",
        Collections.emptyList(),
        -1,
        0,
        null,
        end,
        read.getTagFilters(),
        read.getTagMatch());
  }
}
