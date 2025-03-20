package org.apache.iotdb.db.queryengine.plan.relational.utils;

import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DataOrganizationSpecification {
  private final List<Symbol> partitionBy;
  private final Optional<OrderingScheme> orderingScheme;

  public DataOrganizationSpecification(
      List<Symbol> partitionBy, Optional<OrderingScheme> orderingScheme) {
    requireNonNull(partitionBy, "partitionBy is null");
    requireNonNull(orderingScheme, "orderingScheme is null");

    this.partitionBy = ImmutableList.copyOf(partitionBy);
    this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
  }

  public List<Symbol> getPartitionBy() {
    return partitionBy;
  }

  public Optional<OrderingScheme> getOrderingScheme() {
    return orderingScheme;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionBy, orderingScheme);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    DataOrganizationSpecification other = (DataOrganizationSpecification) obj;

    return Objects.equals(this.partitionBy, other.partitionBy)
        && Objects.equals(this.orderingScheme, other.orderingScheme);
  }
}
