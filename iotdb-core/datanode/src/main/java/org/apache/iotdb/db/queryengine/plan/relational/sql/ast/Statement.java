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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

public abstract class Statement extends Node {

  protected Statement(final @Nullable NodeLocation location) {
    super(location);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitStatement(this, context);
  }

  /**
   * Checks whether this statement should be split into multiple sub-statements based on the given
   * async requirement. Used to limit resource consumption during statement analysis, etc.
   *
   * @param requireAsync whether async execution is required
   * @return true if the statement should be split, false otherwise. Default implementation returns
   *     false.
   */
  public boolean shouldSplit() {
    return false;
  }

  /**
   * Splits the current statement into multiple sub-statements. Used to limit resource consumption
   * during statement analysis, etc.
   *
   * @return the list of sub-statements. Default implementation returns empty list.
   */
  public List<? extends Statement> getSubStatements() {
    return Collections.emptyList();
  }
}
