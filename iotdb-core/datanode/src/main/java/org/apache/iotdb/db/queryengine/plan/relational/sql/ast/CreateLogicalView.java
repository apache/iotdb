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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateLogicalView extends CreateTable {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CreateLogicalView.class);

  private final Query query;
  private final boolean replace;

  public CreateLogicalView(
      final @Nullable NodeLocation location,
      final QualifiedName name,
      final List<ColumnDefinition> elements,
      final @Nullable String charsetName,
      final @Nullable String comment,
      final List<Property> properties,
      final Query query,
      final boolean replace) {
    super(location, name, elements, false, charsetName, comment, properties);
    this.query = requireNonNull(query, "query is null");
    this.replace = replace;
  }

  public Query getQuery() {
    return query;
  }

  public boolean isReplace() {
    return replace;
  }

  @Override
  public <R, C> R accept(final IAstVisitor<R, C> visitor, final C context) {
    return ((AstVisitor<R, C>) visitor).visitCreateLogicalView(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder()
        .addAll(getElements())
        .addAll(getProperties())
        .add(query)
        .build();
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(query, ((CreateLogicalView) o).query)
        && replace == ((CreateLogicalView) o).replace;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), query, replace);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", getName())
        .add("elements", getElements())
        .add("ifNotExists", isIfNotExists())
        .add("charsetName", getCharsetName())
        .add("properties", getProperties())
        .add("query", query)
        .add("replace", replace)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += ramBytesUsedExcludingInstanceSize();
    size += query.ramBytesUsed();
    return size;
  }
}
