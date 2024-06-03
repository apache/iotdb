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

package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RenameTable extends Statement {
  private final QualifiedName source;
  private final Identifier target;

  public RenameTable(QualifiedName source, Identifier target) {
    super(null);
    this.source = requireNonNull(source, "source name is null");
    this.target = requireNonNull(target, "target name is null");
  }

  public RenameTable(NodeLocation location, QualifiedName source, Identifier target) {
    super(requireNonNull(location, "location is null"));
    this.source = requireNonNull(source, "source name is null");
    this.target = requireNonNull(target, "target name is null");
  }

  public QualifiedName getSource() {
    return source;
  }

  public Identifier getTarget() {
    return target;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRenameTable(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    RenameTable o = (RenameTable) obj;
    return Objects.equals(source, o.source) && Objects.equals(target, o.target);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("source", source).add("target", target).toString();
  }
}
