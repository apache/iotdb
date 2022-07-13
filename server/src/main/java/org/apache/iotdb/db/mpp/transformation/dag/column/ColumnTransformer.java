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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.type.Type;

public abstract class ColumnTransformer {

  protected final Expression expression;

  protected final Type returnType;

  protected final ColumnCache columnCache;

  protected boolean hasEvaluated;

  public ColumnTransformer(Expression expression, Type returnType, ColumnCache columnCache) {
    this.expression = expression;
    this.returnType = returnType;
    this.columnCache = columnCache;
    hasEvaluated = false;
  }

  public void tryEvaluate() {
    if (!hasEvaluated) {
      evaluate();
      hasEvaluated = true;
    }
  }

  public Column getColumn() {
    return columnCache.getColumn();
  }

  public Type getType() {
    return returnType;
  }

  /** Responsible for the calculation */
  public abstract void evaluate();

  public abstract void reset();
}
