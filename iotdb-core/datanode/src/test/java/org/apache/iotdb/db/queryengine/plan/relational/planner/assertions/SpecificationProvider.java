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
