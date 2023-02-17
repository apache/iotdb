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

package org.apache.iotdb.db.mpp.transformation.dag.column;

import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

public abstract class ColumnTransformer {

  protected final Type returnType;

  protected final ColumnCache columnCache;

  protected int referenceCount;

  protected ColumnTransformer(Type returnType) {
    this.returnType = returnType;
    this.columnCache = new ColumnCache();
    referenceCount = 0;
  }

  public void tryEvaluate() {
    if (!columnCache.hasCached()) {
      evaluate();
    }
  }

  public Column getColumn() {
    return columnCache.getColumn();
  }

  public void addReferenceCount() {
    referenceCount++;
  }

  public void initializeColumnCache(Column column) {
    columnCache.cacheColumn(column, referenceCount);
  }

  public int getColumnCachePositionCount() {
    return columnCache.getPositionCount();
  }

  public Type getType() {
    return returnType;
  }

  public boolean isReturnTypeNumeric() {
    if (returnType == null) {
      return true;
    }
    TypeEnum typeEnum = returnType.getTypeEnum();
    return typeEnum.equals(TypeEnum.INT32)
        || typeEnum.equals(TypeEnum.INT64)
        || typeEnum.equals(TypeEnum.FLOAT)
        || typeEnum.equals(TypeEnum.DOUBLE);
  }

  /**
   * Return whether the types of two ColumnTransformer are equal. If one ColumnTransformer is {@link
   * NullColumnTransformer} (Only the Type of NullColumnTransformer is null), return {@code true}
   */
  public static boolean typeEquals(ColumnTransformer a, ColumnTransformer b) {
    if (a.getType() == null || b.getType() == null) {
      return true;
    }
    return a.getType().getTypeEnum().equals(b.getType().getTypeEnum());
  }

  /**
   * Return whether the type of this ColumnTransformer is equal to certain type. If this
   * ColumnTransformer is {@link NullColumnTransformer}, return {@code true}
   */
  public boolean typeEquals(TypeEnum typeEnum) {
    return returnType == null || returnType.getTypeEnum().equals(typeEnum);
  }

  /**
   * Return whether the type of this ColumnTransformer is not equal to certain type. If this
   * ColumnTransformer is {@link NullColumnTransformer}, return {@code true}
   */
  public boolean typeNotEquals(TypeEnum typeEnum) {
    return returnType == null || !returnType.getTypeEnum().equals(typeEnum);
  }

  /** Responsible for the calculation */
  protected abstract void evaluate();

  protected abstract void checkType();

  public void close() {
    // do nothing
  }
}
