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

package org.apache.iotdb.calc.execution.operator.process.rowpattern.matcher;

import org.apache.iotdb.calc.i18n.CalcMessages;

import static java.util.Objects.requireNonNull;

public class MatchResult {
  public static final MatchResult NO_MATCH =
      new MatchResult(false, ArrayView.EMPTY, ArrayView.EMPTY);

  private final boolean matched;
  private final ArrayView labels;
  private final ArrayView exclusions;

  public MatchResult(boolean matched, ArrayView labels, ArrayView exclusions) {
    this.matched = matched;
    this.labels = requireNonNull(labels, CalcMessages.EXCEPTION_LABELS_IS_NULL_F4FBBECE);
    this.exclusions =
        requireNonNull(exclusions, CalcMessages.EXCEPTION_EXCLUSIONS_IS_NULL_336ED5E7);
  }

  public boolean isMatched() {
    return matched;
  }

  public ArrayView getLabels() {
    return labels;
  }

  public ArrayView getExclusions() {
    return exclusions;
  }
}
