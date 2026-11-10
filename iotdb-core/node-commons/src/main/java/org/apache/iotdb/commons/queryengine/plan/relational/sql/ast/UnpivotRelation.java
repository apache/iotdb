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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class UnpivotRelation extends QueryBody {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UnpivotRelation.class);

  // A selector is represented by an empty expressions list and a present excluded list.
  // An empty excluded list selects all input columns.
  public record Item(
      List<Expression> expressions,
      Optional<List<Identifier>> excluded,
      Optional<Identifier> alias) {
    public Item {
      expressions = ImmutableList.copyOf(expressions);
      excluded = excluded.map(ImmutableList::copyOf);
    }
  }

  private final Relation input;
  private final List<Identifier> valueColumns;
  private final Identifier nameColumn;
  private final List<Item> items;
  private final boolean includeNulls;
  private final boolean simplified;
  private final boolean explicitInto;

  public UnpivotRelation(
      NodeLocation location,
      Relation input,
      List<Identifier> valueColumns,
      Identifier nameColumn,
      List<Item> items,
      boolean includeNulls,
      boolean simplified,
      boolean explicitInto) {
    super(location);
    this.input = Objects.requireNonNull(input);
    this.valueColumns = ImmutableList.copyOf(valueColumns);
    this.nameColumn = Objects.requireNonNull(nameColumn);
    this.items = ImmutableList.copyOf(items);
    this.includeNulls = includeNulls;
    this.simplified = simplified;
    this.explicitInto = explicitInto;
  }

  public Relation getInput() {
    return input;
  }

  public List<Identifier> getValueColumns() {
    return valueColumns;
  }

  public Identifier getNameColumn() {
    return nameColumn;
  }

  public List<Item> getItems() {
    return items;
  }

  public boolean isIncludeNulls() {
    return includeNulls;
  }

  public boolean isSimplified() {
    return simplified;
  }

  public boolean hasExplicitInto() {
    return explicitInto;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitUnpivotRelation(this, context);
  }

  @Override
  public List<Node> getChildren() {
    List<Node> children = new ArrayList<>();
    children.add(input);
    children.add(nameColumn);
    children.addAll(valueColumns);
    for (Item item : items) {
      children.addAll(item.expressions());
      item.excluded().ifPresent(children::addAll);
      item.alias().ifPresent(children::add);
    }
    return ImmutableList.copyOf(children);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }

  @Override
  public String toString() {
    return "UnpivotRelation{" + input + ", " + items + "}";
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof UnpivotRelation)) {
      return false;
    }
    UnpivotRelation that = (UnpivotRelation) other;
    return input.equals(that.input)
        && valueColumns.equals(that.valueColumns)
        && nameColumn.equals(that.nameColumn)
        && items.equals(that.items)
        && includeNulls == that.includeNulls
        && simplified == that.simplified
        && explicitInto == that.explicitInto;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        input, valueColumns, nameColumn, items, includeNulls, simplified, explicitInto);
  }
}
