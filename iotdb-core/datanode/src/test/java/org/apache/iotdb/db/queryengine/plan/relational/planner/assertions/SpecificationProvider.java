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

import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class SpecificationProvider implements ExpectedValueProvider<DataOrganizationSpecification> {
  private final List<SymbolAlias> partitionBy;
  private final List<SymbolAlias> orderBy;
  private final Map<SymbolAlias, SortOrder> orderings;

  SpecificationProvider(
      List<SymbolAlias> partitionBy,
      List<SymbolAlias> orderBy,
      Map<SymbolAlias, SortOrder> orderings) {
    this.partitionBy = ImmutableList.copyOf(requireNonNull(partitionBy, "partitionBy is null"));
    this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
    this.orderings = ImmutableMap.copyOf(requireNonNull(orderings, "orderings is null"));
  }

  @Override
  public DataOrganizationSpecification getExpectedValue(SymbolAliases aliases) {
    Optional<OrderingScheme> orderingScheme = Optional.empty();
    if (!orderBy.isEmpty()) {
      orderingScheme =
          Optional.of(
              new OrderingScheme(
                  orderBy.stream().map(alias -> alias.toSymbol(aliases)).collect(toImmutableList()),
                  orderings.entrySet().stream()
                      .collect(
                          toImmutableMap(
                              entry -> entry.getKey().toSymbol(aliases), Map.Entry::getValue))));
    }

    return new DataOrganizationSpecification(
        partitionBy.stream().map(alias -> alias.toSymbol(aliases)).collect(toImmutableList()),
        orderingScheme);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionBy", this.partitionBy)
        .add("orderBy", this.orderBy)
        .add("orderings", this.orderings)
        .toString();
  }
}
