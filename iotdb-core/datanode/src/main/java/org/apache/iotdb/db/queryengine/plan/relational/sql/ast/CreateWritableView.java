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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;

import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class CreateWritableView extends CreateTable {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CreateWritableView.class);

  private final QualifiedName originalName;
  private final boolean replace;
  private final Map<String, String> viewColumnToSourceColumnMap;
  private final Map<String, String> viewColumnCommentMap;

  // Schema cascade is in properties, skip here

  public CreateWritableView(
      final @Nullable NodeLocation location,
      final QualifiedName name,
      final @Nullable String comment,
      final List<Property> properties,
      final QualifiedName originalName,
      final boolean replace,
      final Map<String, String> viewColumnCommentMap,
      final Map<String, String> viewColumnToSourceColumnMap) {
    super(location, name, Collections.emptyList(), false, null, comment, properties);
    this.originalName = originalName;
    this.replace = replace;
    this.viewColumnCommentMap = viewColumnCommentMap;
    this.viewColumnToSourceColumnMap = viewColumnToSourceColumnMap;
  }

  public QualifiedName getOriginalName() {
    return originalName;
  }

  public boolean isReplace() {
    return replace;
  }

  public Map<String, String> getViewColumnCommentMap() {
    return viewColumnCommentMap;
  }

  public Map<String, String> getViewColumnToSourceColumnMap() {
    return viewColumnToSourceColumnMap;
  }

  @Override
  public <R, C> R accept(final IAstVisitor<R, C> visitor, final C context) {
    return ((AstVisitor<R, C>) visitor).visitCreateWritableView(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(originalName, ((CreateWritableView) o).originalName)
        && replace == ((CreateWritableView) o).replace
        && Objects.equals(viewColumnCommentMap, ((CreateWritableView) o).viewColumnCommentMap)
        && Objects.equals(
            viewColumnToSourceColumnMap, ((CreateWritableView) o).viewColumnToSourceColumnMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), originalName, replace, viewColumnCommentMap, viewColumnToSourceColumnMap);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", getName())
        .add("elements", getElements())
        .add("ifNotExists", isIfNotExists())
        .add("charsetName", getCharsetName())
        .add("properties", getProperties())
        .add("originalName", originalName)
        .add("replace", replace)
        .add("viewColumnCommentMap", viewColumnCommentMap)
        .add("viewColumnToSourceColumnMap", viewColumnToSourceColumnMap)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += ramBytesUsedExcludingInstanceSize();
    size += originalName == null ? 0L : originalName.ramBytesUsed();
    size += RamUsageEstimator.sizeOfMap(viewColumnCommentMap);
    size += RamUsageEstimator.sizeOfMap(viewColumnToSourceColumnMap);
    return size;
  }
}
