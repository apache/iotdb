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

import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowPipes extends Statement {

  private final String pipeName;
  private final boolean hasWhereClause;

  public ShowPipes(final @Nullable String pipeName, final boolean hasWhereClause) {
    super(null);
    this.pipeName = pipeName;
    this.hasWhereClause = hasWhereClause;
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean hasWhereClause() {
    return hasWhereClause;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowPipes(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, hasWhereClause);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ShowPipes other = (ShowPipes) obj;
    return Objects.equals(pipeName, other.pipeName)
        && Objects.equals(hasWhereClause, other.hasWhereClause);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", pipeName)
        .add("hasWhereClause", hasWhereClause)
        .toString();
  }
}
