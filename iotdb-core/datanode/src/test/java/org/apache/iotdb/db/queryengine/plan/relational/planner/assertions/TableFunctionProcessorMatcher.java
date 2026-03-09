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

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.node;

public class TableFunctionProcessorMatcher implements Matcher {
  private final String name;
  private final List<String> properOutputs;
  private final List<String> requiredSymbols;
  private final TableFunctionHandle handle;

  private TableFunctionProcessorMatcher(
      String name,
      List<String> properOutputs,
      List<String> requiredSymbols,
      TableFunctionHandle handle) {
    this.name = requireNonNull(name, "name is null");
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
    this.handle = handle;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof TableFunctionProcessorNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    TableFunctionProcessorNode tableFunctionProcessorNode = (TableFunctionProcessorNode) node;
    if (!name.equals(tableFunctionProcessorNode.getName())) {
      return NO_MATCH;
    }
    if (properOutputs.size() != tableFunctionProcessorNode.getProperOutputs().size()) {
      return NO_MATCH;
    }

    List<SymbolReference> expectedRequired =
        requiredSymbols.stream().map(symbolAliases::get).collect(toImmutableList());
    List<SymbolReference> actualRequired =
        tableFunctionProcessorNode.getRequiredSymbols().stream()
            .map(Symbol::toSymbolReference)
            .collect(toImmutableList());
    if (!expectedRequired.equals(actualRequired)) {
      return NO_MATCH;
    }
    if (!handle.equals(tableFunctionProcessorNode.getTableFunctionHandle())) {
      return NO_MATCH;
    }
    ImmutableMap.Builder<String, SymbolReference> properOutputsMapping = ImmutableMap.builder();
    for (int i = 0; i < properOutputs.size(); i++) {
      properOutputsMapping.put(
          properOutputs.get(i),
          tableFunctionProcessorNode.getProperOutputs().get(i).toSymbolReference());
    }

    return match(
        SymbolAliases.builder()
            .putAll(symbolAliases)
            .putAll(properOutputsMapping.buildOrThrow())
            .build());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .omitNullValues()
        .add("name", name)
        .add("properOutputs", properOutputs)
        .add("requiredSymbols", requiredSymbols)
        .add("handle", handle)
        .toString();
  }

  public static class Builder {
    private String name;
    private List<String> properOutputs = ImmutableList.of();
    private List<String> requiredSymbols = ImmutableList.of();
    private TableFunctionHandle handle;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder properOutputs(String... properOutputs) {
      this.properOutputs = ImmutableList.copyOf(properOutputs);
      return this;
    }

    public Builder requiredSymbols(String... requiredSymbols) {
      this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
      return this;
    }

    public Builder handle(TableFunctionHandle handle) {
      this.handle = handle;
      return this;
    }

    public TableFunctionProcessorMatcher build() {
      return new TableFunctionProcessorMatcher(name, properOutputs, requiredSymbols, handle);
    }
  }
}
