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

package org.apache.iotdb.db.queryengine.statistics;

import java.util.Collections;
import java.util.Map;

public class SpecifiedInfoMergerFactory {

  @FunctionalInterface
  public interface SpecifiedInfoMerger {
    Map<String, String> merge(Map<String, String> first, Map<String, String> second);
  }

  private static final SpecifiedInfoMerger DEFAULT_MERGER =
      (first, second) -> {
        return Collections.emptyMap();
      };

  private static final SpecifiedInfoMerger LONG_MERGER =
      (first, second) -> {
        first.replaceAll(
            (k, v) -> Long.toString(Long.parseLong(v) + Long.parseLong(second.get(k))));
        return first;
      };

  public static SpecifiedInfoMerger getMerger(String operatorType) {
    switch (operatorType) {
      case "TreeSortOperator":
      case "TreeMergeSortOperator":
      case "TableSortOperator":
      case "TableMergeSortOperator":
      case "FilterAndProjectOperator":
        return LONG_MERGER;
      default:
        return DEFAULT_MERGER;
    }
  }
}
