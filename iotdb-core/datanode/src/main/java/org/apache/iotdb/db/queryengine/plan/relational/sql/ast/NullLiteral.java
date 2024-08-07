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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class NullLiteral extends Literal {

  public NullLiteral() {
    super(null);
  }

  public NullLiteral(NodeLocation location) {
    super(requireNonNull(location, "location is null"));
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNullLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.NULL_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {}

  public NullLiteral(ByteBuffer byteBuffer) {
    super(null);
  }

  @Override
  public Object getTsValue() {
    return null;
  }
}
