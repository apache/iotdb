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

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTable extends Statement {
  private final QualifiedName name;
  private final List<ColumnDefinition> elements;

  private final boolean ifNotExists;

  @Nullable private final String charsetName;
  private final List<Property> properties;

  public CreateTable(
      final NodeLocation location,
      final QualifiedName name,
      final List<ColumnDefinition> elements,
      final boolean ifNotExists,
      final @Nullable String charsetName,
      final List<Property> properties) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
    this.ifNotExists = ifNotExists;
    this.charsetName = charsetName;
    this.properties = requireNonNull(properties, "properties is null");
  }

  public QualifiedName getName() {
    return name;
  }

  public List<ColumnDefinition> getElements() {
    return elements;
  }

  public List<Property> getProperties() {
    return properties;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public Optional<String> getCharsetName() {
    return Optional.ofNullable(charsetName);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTable(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder().addAll(elements).addAll(properties).build();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateTable that = (CreateTable) o;
    return ifNotExists == that.ifNotExists
        && Objects.equals(name, that.name)
        && Objects.equals(elements, that.elements)
        && Objects.equals(charsetName, that.charsetName)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, elements, ifNotExists, charsetName, properties);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("elements", elements)
        .add("ifNotExists", ifNotExists)
        .add("charsetName", charsetName)
        .add("properties", properties)
        .toString();
  }
}
