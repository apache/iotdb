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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrAnchor.Type.PARTITION_END;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrAnchor.Type.PARTITION_START;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrQuantifier.oneOrMore;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrQuantifier.zeroOrMore;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrQuantifier.zeroOrOne;

public class Patterns {
  private Patterns() {}

  public static IrLabel label(String name) {
    return new IrLabel(name);
  }

  public static IrRowPattern empty() {
    return new IrEmpty();
  }

  public static IrRowPattern excluded(IrRowPattern pattern) {
    return new IrExclusion(pattern);
  }

  public static IrRowPattern start() {
    return new IrAnchor(PARTITION_START);
  }

  public static IrRowPattern end() {
    return new IrAnchor(PARTITION_END);
  }

  public static IrRowPattern plusQuantified(IrRowPattern pattern, boolean greedy) {
    return new IrQuantified(pattern, oneOrMore(greedy));
  }

  public static IrRowPattern starQuantified(IrRowPattern pattern, boolean greedy) {
    return new IrQuantified(pattern, zeroOrMore(greedy));
  }

  public static IrRowPattern questionMarkQuantified(IrRowPattern pattern, boolean greedy) {
    return new IrQuantified(pattern, zeroOrOne(greedy));
  }

  public static IrRowPattern rangeQuantified(
      IrRowPattern pattern, int atLeast, Optional<Integer> atMost, boolean greedy) {
    return new IrQuantified(pattern, new IrQuantifier(atLeast, atMost, greedy));
  }

  public static IrRowPattern alternation(IrRowPattern... parts) {
    return new IrAlternation(ImmutableList.copyOf(parts));
  }

  public static IrRowPattern concatenation(IrRowPattern... parts) {
    return new IrConcatenation(ImmutableList.copyOf(parts));
  }

  public static IrRowPattern permutation(IrRowPattern... parts) {
    return new IrPermutation(ImmutableList.copyOf(parts));
  }
}
