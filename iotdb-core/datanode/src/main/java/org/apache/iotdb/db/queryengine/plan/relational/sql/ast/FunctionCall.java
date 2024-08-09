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
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FunctionCall extends Expression {
  private final QualifiedName name;
  private final boolean distinct;
  private final List<Expression> arguments;

  public FunctionCall(QualifiedName name, List<Expression> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.distinct = false;
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(QualifiedName name, boolean distinct, List<Expression> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.distinct = distinct;
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(NodeLocation location, QualifiedName name, List<Expression> arguments) {
    this(location, name, false, arguments);
  }

  public FunctionCall(
      NodeLocation location, QualifiedName name, boolean distinct, List<Expression> arguments) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.distinct = distinct;
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFunctionCall(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.addAll(arguments);
    return nodes.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    FunctionCall o = (FunctionCall) obj;
    return Objects.equals(name, o.name)
        && Objects.equals(distinct, o.distinct)
        && Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, distinct, arguments);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    FunctionCall otherFunction = (FunctionCall) other;

    return name.equals(otherFunction.name) && distinct == otherFunction.distinct;
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.FUNCTION_CALL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    this.name.serialize(stream);
    ReadWriteIOUtils.write(this.distinct, stream);
    ReadWriteIOUtils.write(arguments.size(), stream);
    for (Expression argument : arguments) {
      Expression.serialize(argument, stream);
    }
  }

  public FunctionCall(ByteBuffer byteBuffer) {
    super(null);
    this.name = QualifiedName.deserialize(byteBuffer);
    this.distinct = ReadWriteIOUtils.readBool(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    this.arguments = new ArrayList<>(size);
    while (size-- > 0) {
      arguments.add(Expression.deserialize(byteBuffer));
    }
  }
}
