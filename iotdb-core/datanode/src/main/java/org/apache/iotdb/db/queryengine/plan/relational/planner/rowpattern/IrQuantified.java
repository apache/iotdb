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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IrQuantified extends IrRowPattern {
  private final IrRowPattern pattern;
  private final IrQuantifier quantifier;

  public IrQuantified(IrRowPattern pattern, IrQuantifier quantifier) {
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.quantifier = requireNonNull(quantifier, "quantifier is null");
  }

  public IrRowPattern getPattern() {
    return pattern;
  }

  public IrQuantifier getQuantifier() {
    return quantifier;
  }

  @Override
  public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context) {
    return visitor.visitIrQuantified(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    IrQuantified o = (IrQuantified) obj;
    return Objects.equals(pattern, o.pattern) && Objects.equals(quantifier, o.quantifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pattern, quantifier);
  }

  @Override
  public String toString() {
    return pattern.toString() + quantifier;
  }

  public static void serialize(IrQuantified pattern, ByteBuffer byteBuffer) {
    IrRowPattern.serialize(pattern.pattern, byteBuffer);
    IrQuantifier.serialize(pattern.quantifier, byteBuffer);
  }

  public static void serialize(IrQuantified pattern, DataOutputStream stream) throws IOException {
    IrRowPattern.serialize(pattern.pattern, stream);
    IrQuantifier.serialize(pattern.quantifier, stream);
  }

  public static IrQuantified deserialize(ByteBuffer byteBuffer) {
    IrRowPattern pattern = IrRowPattern.deserialize(byteBuffer);
    IrQuantifier quantifier = IrQuantifier.deserialize(byteBuffer);
    return new IrQuantified(pattern, quantifier);
  }
}
