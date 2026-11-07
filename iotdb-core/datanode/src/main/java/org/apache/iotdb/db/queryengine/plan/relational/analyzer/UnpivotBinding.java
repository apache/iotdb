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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;

/** Resolved, query-local UNPIVOT schema; source expressions are evaluated before expansion. */
public final class UnpivotBinding {
  private final List<Integer> retained;
  private final List<List<Expression>> sources;
  private final List<List<Integer>> sourceFieldIndexes;
  private final List<String> names;
  private final List<Type> valueTypes;

  public UnpivotBinding(
      List<Integer> retained,
      List<List<Expression>> sources,
      List<List<Integer>> sourceFieldIndexes,
      List<String> names,
      List<Type> valueTypes) {
    this.retained = ImmutableList.copyOf(retained);
    this.sources =
        sources.stream().map(ImmutableList::copyOf).collect(ImmutableList.toImmutableList());
    this.sourceFieldIndexes =
        sourceFieldIndexes.stream()
            .map(ImmutableList::copyOf)
            .collect(ImmutableList.toImmutableList());
    this.names = ImmutableList.copyOf(names);
    this.valueTypes = ImmutableList.copyOf(valueTypes);
  }

  public List<Integer> getRetained() {
    return retained;
  }

  public List<List<Expression>> getSources() {
    return sources;
  }

  /** Input field index for a source, or -1 when the source is an explicit expression. */
  public List<List<Integer>> getSourceFieldIndexes() {
    return sourceFieldIndexes;
  }

  public List<String> getNames() {
    return names;
  }

  public List<Type> getValueTypes() {
    return valueTypes;
  }
}
