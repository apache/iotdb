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
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.node;

public class TableFunctionProcessorMatcher implements Matcher {
  private final String name;
  private final List<String> properOutputs;
  private final List<String> requiredSymbols;
  private final Map<String, ArgumentValue> arguments;

  private TableFunctionProcessorMatcher(
      String name,
      List<String> properOutputs,
      List<String> requiredSymbols,
      Map<String, ArgumentValue> arguments) {
    this.name = requireNonNull(name, "name is null");
    this.properOutputs = ImmutableList.copyOf(properOutputs);
    this.requiredSymbols = ImmutableList.copyOf(requiredSymbols);
    this.arguments = ImmutableMap.copyOf(arguments);
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
    for (Map.Entry<String, ArgumentValue> entry : arguments.entrySet()) {
      String argumentName = entry.getKey();
      Argument actual = tableFunctionProcessorNode.getArguments().get(argumentName);
      if (actual == null) {
        return NO_MATCH;
      }
      ArgumentValue expected = entry.getValue();
      if (expected instanceof ScalarArgumentValue) {
        if (!(actual instanceof ScalarArgument)) {
          return NO_MATCH;
        }
        ScalarArgumentValue expectedScalar = (ScalarArgumentValue) expected;
        ScalarArgument actualScalar = (ScalarArgument) actual;
        if (!Objects.equals(expectedScalar.value, actualScalar.getValue())) {
          return NO_MATCH;
        }

      } else {
        if (!(actual instanceof TableArgument)) {
          return NO_MATCH;
        }
        TableArgumentValue expectedTableArgument = (TableArgumentValue) expected;
        TableArgument actualTableArgument = (TableArgument) actual;
        if (expectedTableArgument.rowSemantics != actualTableArgument.isRowSemantics()) {
          // check row semantic
          return NO_MATCH;
        }
        if (expectedTableArgument.passThroughColumns) {
          // check pass through columns
          Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification =
              tableFunctionProcessorNode.getPassThroughSpecification();
          if (!passThroughSpecification.isPresent()
              || !passThroughSpecification.get().isDeclaredAsPassThrough()) {
            return NO_MATCH;
          }
          Set<SymbolReference> expectedPassThrough =
              expectedTableArgument.passThroughSymbol.stream()
                  .map(symbolAliases::get)
                  .collect(toImmutableSet());
          Set<SymbolReference> actualPassThrough =
              passThroughSpecification.get().getColumns().stream()
                  .map(TableFunctionNode.PassThroughColumn::getSymbol)
                  .map(Symbol::toSymbolReference)
                  .collect(toImmutableSet());

          if (!expectedPassThrough.equals(actualPassThrough)) {
            return NO_MATCH;
          }
        }
        if (expectedTableArgument.specification.isPresent()
            && tableFunctionProcessorNode.getDataOrganizationSpecification().isPresent()) {
          // check data organization
          DataOrganizationSpecification expectedDataOrganization =
              expectedTableArgument.specification.get().getExpectedValue(symbolAliases);
          DataOrganizationSpecification actualDataOrganization =
              tableFunctionProcessorNode.getDataOrganizationSpecification().get();
          if (!expectedDataOrganization.equals(actualDataOrganization)) {
            return NO_MATCH;
          }
        }
      }
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
        .add("arguments", arguments)
        .toString();
  }

  public interface ArgumentValue {}

  public static class Builder {
    private String name;
    private List<String> properOutputs = ImmutableList.of();
    private List<String> requiredSymbols = ImmutableList.of();
    private final ImmutableMap.Builder<String, ArgumentValue> arguments = ImmutableMap.builder();

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

    public TableFunctionProcessorMatcher.Builder addScalarArgument(String name, Object value) {
      this.arguments.put(name, new ScalarArgumentValue(value));
      return this;
    }

    public TableFunctionProcessorMatcher.Builder addTableArgument(
        String name, TableArgumentValue.Builder tableArgument) {
      this.arguments.put(name, tableArgument.build());
      return this;
    }

    public TableFunctionProcessorMatcher build() {
      return new TableFunctionProcessorMatcher(
          name, properOutputs, requiredSymbols, arguments.build());
    }
  }

  public static class ScalarArgumentValue implements ArgumentValue {
    protected Object value;

    public ScalarArgumentValue(Object value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return toStringHelper(this).add("value", value).toString();
    }
  }

  public static class TableArgumentValue implements ArgumentValue {
    protected boolean rowSemantics;
    protected boolean passThroughColumns;
    protected Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
    protected Set<String> passThroughSymbol;

    public TableArgumentValue(
        boolean rowSemantics,
        boolean passThroughColumns,
        Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
        Set<String> passThroughSymbol) {
      this.rowSemantics = rowSemantics;
      this.passThroughColumns = passThroughColumns;
      this.specification = specification;
      this.passThroughSymbol = passThroughSymbol;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .omitNullValues()
          .add("rowSemantics", rowSemantics)
          .add("passThroughColumns", passThroughColumns)
          .add("specification", specification)
          .add("passThroughSymbol", passThroughSymbol)
          .toString();
    }

    public static class Builder {
      private boolean rowSemantics;
      private boolean passThroughColumns;
      private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification =
          Optional.empty();
      private Set<String> passThroughSymbols = ImmutableSet.of();

      private Builder() {}

      public static Builder tableArgument() {
        return new Builder();
      }

      public Builder rowSemantics() {
        this.rowSemantics = true;
        return this;
      }

      public Builder specification(
          ExpectedValueProvider<DataOrganizationSpecification> specification) {
        this.specification = Optional.of(specification);
        return this;
      }

      public Builder passThroughSymbols(String... symbols) {
        this.passThroughColumns = true;
        this.passThroughSymbols = ImmutableSet.copyOf(symbols);
        return this;
      }

      private TableArgumentValue build() {
        return new TableArgumentValue(
            rowSemantics, passThroughColumns, specification, passThroughSymbols);
      }
    }
  }
}
