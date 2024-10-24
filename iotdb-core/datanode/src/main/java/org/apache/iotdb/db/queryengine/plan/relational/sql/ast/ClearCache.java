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

import org.apache.iotdb.commons.schema.cache.CacheClearOptions;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ClearCache extends Statement {

  private final boolean onCluster;
  private final Set<CacheClearOptions> options;

  public ClearCache(final boolean onCluster, final Set<CacheClearOptions> options) {
    super(null);
    this.onCluster = onCluster;
    this.options = options;
  }

  public boolean isOnCluster() {
    return onCluster;
  }

  public Set<CacheClearOptions> getOptions() {
    return options;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitClearCache(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(onCluster);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || (getClass() != obj.getClass())) {
      return false;
    }

    final ClearCache other = (ClearCache) obj;
    return this.onCluster == other.onCluster;
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
