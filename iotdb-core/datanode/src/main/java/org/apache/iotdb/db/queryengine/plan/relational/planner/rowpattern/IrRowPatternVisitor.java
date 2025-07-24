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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import javax.annotation.Nullable;

public abstract class IrRowPatternVisitor<R, C> {
  public R process(IrRowPattern rowPattern) {
    return process(rowPattern, null);
  }

  public R process(IrRowPattern rowPattern, @Nullable C context) {
    return rowPattern.accept(this, context);
  }

  protected R visitIrRowPattern(IrRowPattern rowPattern, C context) {
    return null;
  }

  protected R visitIrAlternation(IrAlternation node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrConcatenation(IrConcatenation node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrQuantified(IrQuantified node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrAnchor(IrAnchor node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrEmpty(IrEmpty node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrExclusion(IrExclusion node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrPermutation(IrPermutation node, C context) {
    return visitIrRowPattern(node, context);
  }

  protected R visitIrLabel(IrLabel node, C context) {
    return visitIrRowPattern(node, context);
  }
}
