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

package org.apache.iotdb.calc.transformation.dag.column;

import org.apache.iotdb.calc.transformation.dag.column.leaf.NullColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isNumericType;

public abstract class ColumnTransformer {

  protected final Type returnType;

  protected final ColumnCache columnCache;

  // Compatibility with the evaluateWithSelection()/getColumn() API. Never used as a full cache.
  private ColumnCache selectedColumnCache;

  protected int referenceCount;

  protected ColumnTransformer(Type returnType) {
    this.returnType = returnType;
    this.columnCache = new ColumnCache();
    referenceCount = 0;
  }

  public void tryEvaluate() {
    selectedColumnCache = null;
    if (!columnCache.hasCached()) {
      evaluate();
    }
  }

  /**
   * @deprecated Use {@link #getColumnWithSelection(boolean[])} to retain the result directly.
   */
  @Deprecated
  public void tryEvaluateWithSelection(boolean[] selection) {
    selectedColumnCache = null;
    if (!columnCache.hasCached()) {
      evaluateWithSelection(selection);
    }
  }

  public Column getColumn() {
    if (selectedColumnCache != null) {
      return selectedColumnCache.getColumn();
    }
    return columnCache.getColumn();
  }

  /**
   * Returns a column whose selected rows are evaluated, without consuming or replacing the shared
   * full-column cache. Unselected rows are unspecified. Callers must retain the returned column and
   * only read selected rows. Partial results belong to the caller, so different CASE branches can
   * use different masks without invalidating one another's columns.
   */
  public Column getColumnWithSelection(boolean[] selection) {
    Column fullColumn = columnCache.peekColumn();
    return fullColumn != null ? fullColumn : evaluateSelection(selection);
  }

  public void addReferenceCount() {
    referenceCount++;
  }

  public void initializeColumnCache(Column column) {
    selectedColumnCache = null;
    columnCache.cacheColumn(column, referenceCount);
  }

  public int getColumnCachePositionCount() {
    if (selectedColumnCache != null) {
      return selectedColumnCache.getPositionCount();
    }
    return columnCache.getPositionCount();
  }

  public Type getType() {
    return returnType;
  }

  public boolean isReturnTypeNumeric() {
    if (returnType == null) {
      return true;
    }
    return isNumericType(returnType);
  }

  /**
   * Return whether the types of two ColumnTransformer are equal. If one ColumnTransformer is {@link
   * NullColumnTransformer} (Only the Type of NullColumnTransformer is null), return {@code true}.
   */
  public static boolean typeEquals(ColumnTransformer a, ColumnTransformer b) {
    if (a.getType() == null || b.getType() == null) {
      return true;
    }
    return a.getType().getTypeEnum().equals(b.getType().getTypeEnum());
  }

  /**
   * Return whether the type of this ColumnTransformer is equal to certain type. If this
   * ColumnTransformer is {@link NullColumnTransformer}, return {@code true}.
   */
  public boolean typeEquals(TypeEnum typeEnum) {
    return returnType == null || returnType.getTypeEnum().equals(typeEnum);
  }

  /**
   * Return whether the type of this ColumnTransformer is not equal to certain type. If this
   * ColumnTransformer is {@link NullColumnTransformer}, return {@code true}.
   */
  public boolean typeNotEquals(TypeEnum typeEnum) {
    return returnType == null || !returnType.getTypeEnum().equals(typeEnum);
  }

  /** Responsible for the calculation. */
  protected abstract void evaluate();

  /**
   * @deprecated Use {@link #getColumnWithSelection(boolean[])} to retain the result directly.
   */
  @Deprecated
  public void evaluateWithSelection(boolean[] selection) {
    Column result = getColumnWithSelection(selection);
    selectedColumnCache = new ColumnCache();
    selectedColumnCache.cacheColumn(result, referenceCount);
  }

  protected abstract Column evaluateSelection(boolean[] selection);

  protected abstract void checkType();

  public void close() {
    columnCache.clear();
    selectedColumnCache = null;
  }

  /**
   * Clears ordinary column caches in this subgraph. Input-specific retained results, such as DIFF's
   * complete result, remain valid so another request for the same input cannot advance state again.
   * This method does not mark a new input: before replacing leaf inputs, call {@link
   * #clearCacheForNewInput()} on every node collected by {@link #collectTransformers(Iterable)}.
   * Subclasses should define child edges via {@link #forEachChild(Consumer)} instead of overriding
   * this method.
   *
   * <p>Kept non-final so existing Mockito-based tests can stub it.
   */
  public void clearCache() {
    selectedColumnCache = null;
    this.columnCache.clear();
    forEachChild(ColumnTransformer::clearCache);
  }

  /**
   * Clears only this node's cached column before initializing a new input block. Call this on every
   * node collected by {@link #collectTransformers(Iterable)} before evaluating any expressions for
   * the new block, when no sibling expression can still need a cached column.
   */
  public final void clearCacheForNewInput() {
    selectedColumnCache = null;
    this.columnCache.clear();
    resetForNewInput();
  }

  /** Releases input-specific state while preserving state carried between blocks, such as DIFF. */
  protected void resetForNewInput() {
    // Stateless transformers only need their shared cache cleared.
  }

  /**
   * Collects all nodes reachable from the evaluation phase's roots, including CASE descendants,
   * deduplicating by identity. Collect once after constructing the DAG and retain the flat array so
   * new-input cache clearing needs no graph traversal or visited-set allocation.
   */
  public static ColumnTransformer[] collectTransformers(Iterable<ColumnTransformer> roots) {
    List<ColumnTransformer> transformers = new ArrayList<>();
    Set<ColumnTransformer> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (ColumnTransformer root : roots) {
      root.collectTransformers(transformers, visited);
    }
    return transformers.toArray(new ColumnTransformer[0]);
  }

  private void collectTransformers(
      List<ColumnTransformer> transformers, Set<ColumnTransformer> visited) {
    if (visited.add(this)) {
      transformers.add(this);
      forEachChild(child -> child.collectTransformers(transformers, visited));
    }
  }

  /** Defines the child edges used for cache clearing and input lifecycle management. */
  protected abstract void forEachChild(Consumer<ColumnTransformer> action);
}
