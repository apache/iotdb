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

import org.apache.iotdb.cli.i18n.FsWindowMessages;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public final class ReductionOptions {
  public static final List<String> METRICS =
      Collections.unmodifiableList(
          Arrays.asList(
              "count",
              "sum",
              "avg",
              "min",
              "max",
              "first",
              "last",
              "delta",
              "elapsed_ms",
              "rate_per_ms"));
  public final String field, groupBy, rankBy, order;
  public final List<String> metrics;
  public final long top;
  public final ReadOptions read;
  public final WindowGrid grid;

  private ReductionOptions(FilesystemCommand c) {
    read = c.getReadOptions();
    grid = WindowGrid.from(c);
    field = c.optionValue("--field", "");
    groupBy = c.optionValue("--group-by", "entity");
    rankBy = c.optionValue("--rank-by", "");
    order = c.optionValue("--order", "desc");
    metrics = Arrays.asList(c.optionValue("--metrics", "count,first,last,delta").split(",", -1));
    try {
      top = Long.parseLong(c.optionValue("--top", Long.toString(Long.MAX_VALUE)));
    } catch (NumberFormatException e) {
      throw invalid();
    }
    if (field.isEmpty()
        || !Arrays.asList("entity", "pooled").contains(groupBy)
        || !Arrays.asList("asc", "desc").contains(order)
        || !METRICS.containsAll(metrics)
        || new HashSet<>(metrics).size() != metrics.size()
        || (!rankBy.isEmpty() && !metrics.contains(rankBy))
        || top <= 0
        || read.getOffset() < 0
        || read.getLimit() < -1
        || (c.hasOption("--top") && rankBy.isEmpty())
        || (c.hasOption("--order") && rankBy.isEmpty())) throw invalid();
  }

  public static ReductionOptions from(FilesystemCommand c) {
    return new ReductionOptions(c);
  }

  public static IllegalArgumentException invalid() {
    return new IllegalArgumentException(
        FsWindowMessages
            .EXCEPTION_INVALID_REDUCTION_REQUIRE_A_FINITE_NUMERIC_FIELD_GROUP_BY_ENTITY_POOLED_METRICS_COUNT_SUM_AVG_MIN_MAX_FIRST_LAST_DELTA_ELAPSED_MS_RATE_PER_MS_RANK_BY_MUST_BE_A_SELECTED_METRIC_ORDER_ASC_DESC_POSITIVE_TOP_AT_MOST_65536_NONEMPTY_ENTITY_WINDOW_GROUPS_3742D176);
  }

  public ReadOptions scanOptions() {
    if (grid.origin == null) return read;
    return new ReadOptions(
            read.getFormat(),
            "",
            "",
            Collections.emptyList(),
            -1,
            0,
            null,
            null,
            read.getTagFilters(),
            read.getTagMatch())
        .withStatistics("", grid.ranges());
  }
}
