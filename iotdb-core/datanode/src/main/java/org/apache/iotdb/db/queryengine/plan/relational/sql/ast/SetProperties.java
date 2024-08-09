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

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetProperties extends Statement {

  public enum Type {
    TABLE,
    MATERIALIZED_VIEW
  }

  private final Type type;
  private final QualifiedName name;
  private final List<Property> properties;
  private final boolean ifExists;

  public SetProperties(
      final Type type,
      final QualifiedName name,
      final List<Property> properties,
      final boolean ifExists) {
    super(null);
    this.type = requireNonNull(type, "type is null");
    this.name = requireNonNull(name, "name is null");
    this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    this.ifExists = ifExists;
  }

  public SetProperties(
      final @Nonnull NodeLocation location,
      final Type type,
      final QualifiedName name,
      final List<Property> properties,
      final boolean ifExists) {
    super(requireNonNull(location, "location is null"));
    this.type = requireNonNull(type, "type is null");
    this.name = requireNonNull(name, "name is null");
    this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    this.ifExists = ifExists;
  }

  public Type getType() {
    return type;
  }

  public QualifiedName getName() {
    return name;
  }

  public List<Property> getProperties() {
    return properties;
  }

  public boolean ifExists() {
    return ifExists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSetProperties(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, properties, ifExists);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final SetProperties o = (SetProperties) obj;
    return type == o.type
        && ifExists == o.ifExists
        && Objects.equals(name, o.name)
        && Objects.equals(properties, o.properties);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("name", name)
        .add("properties", properties)
        .add("ifExists", ifExists)
        .toString();
  }
}
