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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableArgumentAnalysis {
  private final String argumentName;
  private final Optional<QualifiedName> name;
  private final Relation relation;
  private final Optional<List<Expression>> partitionBy; // it is allowed to partition by empty list
  private final Optional<OrderBy> orderBy;
  private final boolean pruneWhenEmpty;
  private final boolean rowSemantics;
  private final boolean passThroughColumns;

  private TableArgumentAnalysis(
      String argumentName,
      Optional<QualifiedName> name,
      Relation relation,
      Optional<List<Expression>> partitionBy,
      Optional<OrderBy> orderBy,
      boolean pruneWhenEmpty,
      boolean rowSemantics,
      boolean passThroughColumns) {
    this.argumentName = requireNonNull(argumentName, "argumentName is null");
    this.name = requireNonNull(name, "name is null");
    this.relation = requireNonNull(relation, "relation is null");
    this.partitionBy =
        requireNonNull(partitionBy, "partitionBy is null").map(ImmutableList::copyOf);
    this.orderBy = requireNonNull(orderBy, "orderBy is null");
    this.pruneWhenEmpty = pruneWhenEmpty;
    this.rowSemantics = rowSemantics;
    this.passThroughColumns = passThroughColumns;
  }

  public String getArgumentName() {
    return argumentName;
  }

  public Optional<QualifiedName> getName() {
    return name;
  }

  public Relation getRelation() {
    return relation;
  }

  public Optional<List<Expression>> getPartitionBy() {
    return partitionBy;
  }

  public Optional<OrderBy> getOrderBy() {
    return orderBy;
  }

  public boolean isPruneWhenEmpty() {
    return pruneWhenEmpty;
  }

  public boolean isRowSemantics() {
    return rowSemantics;
  }

  public boolean isPassThroughColumns() {
    return passThroughColumns;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String argumentName;
    private Optional<QualifiedName> name = Optional.empty();
    private Relation relation;
    private Optional<List<Expression>> partitionBy = Optional.empty();
    private Optional<OrderBy> orderBy = Optional.empty();
    private boolean pruneWhenEmpty;
    private boolean rowSemantics;
    private boolean passThroughColumns;

    private Builder() {}

    @CanIgnoreReturnValue
    public Builder withArgumentName(String argumentName) {
      this.argumentName = argumentName;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withName(QualifiedName name) {
      this.name = Optional.of(name);
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withRelation(Relation relation) {
      this.relation = relation;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withPartitionBy(List<Expression> partitionBy) {
      this.partitionBy = Optional.of(partitionBy);
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withOrderBy(OrderBy orderBy) {
      this.orderBy = Optional.of(orderBy);
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withPruneWhenEmpty(boolean pruneWhenEmpty) {
      this.pruneWhenEmpty = pruneWhenEmpty;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withRowSemantics(boolean rowSemantics) {
      this.rowSemantics = rowSemantics;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder withPassThroughColumns(boolean passThroughColumns) {
      this.passThroughColumns = passThroughColumns;
      return this;
    }

    public TableArgumentAnalysis build() {
      return new TableArgumentAnalysis(
          argumentName,
          name,
          relation,
          partitionBy,
          orderBy,
          pruneWhenEmpty,
          rowSemantics,
          passThroughColumns);
    }
  }
}
