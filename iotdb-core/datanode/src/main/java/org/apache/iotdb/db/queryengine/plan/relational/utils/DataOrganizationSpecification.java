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
