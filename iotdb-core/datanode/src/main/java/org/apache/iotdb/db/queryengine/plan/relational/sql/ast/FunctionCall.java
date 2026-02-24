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
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FunctionCall extends Expression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FunctionCall.class);

  private final QualifiedName name;
  private final Optional<Window> window;
  private final boolean distinct;
  private final Optional<ProcessingMode> processingMode;
  private final List<Expression> arguments;
  private final Optional<NullTreatment> nullTreatment;

  public FunctionCall(QualifiedName name, List<Expression> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.window = Optional.empty();
    this.nullTreatment = Optional.empty();
    this.distinct = false;
    this.processingMode = Optional.empty();
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(QualifiedName name, boolean distinct, List<Expression> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.window = Optional.empty();
    this.nullTreatment = Optional.empty();
    this.distinct = distinct;
    this.processingMode = Optional.empty();
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(
      QualifiedName name, Optional<ProcessingMode> processingMode, List<Expression> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.window = Optional.empty();
    this.nullTreatment = Optional.empty();
    this.distinct = false;
    this.processingMode = requireNonNull(processingMode, "processingMode is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(
      QualifiedName name,
      boolean distinct,
      Optional<ProcessingMode> processingMode,
      List<Expression> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.window = Optional.empty();
    this.nullTreatment = Optional.empty();
    this.distinct = distinct;
    this.processingMode = requireNonNull(processingMode, "processingMode is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(NodeLocation location, QualifiedName name, List<Expression> arguments) {
    this(location, name, false, Optional.empty(), arguments);
  }

  public FunctionCall(
      NodeLocation location,
      QualifiedName name,
      boolean distinct,
      Optional<ProcessingMode> processingMode,
      List<Expression> arguments) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.window = Optional.empty();
    this.nullTreatment = Optional.empty();
    this.distinct = distinct;
    this.processingMode = requireNonNull(processingMode, "processingMode is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public FunctionCall(
      NodeLocation location,
      QualifiedName name,
      Optional<Window> window,
      Optional<NullTreatment> nullTreatment,
      boolean distinct,
      List<Expression> arguments) {
    super(requireNonNull(location, "location is null"));

    this.name = name;
    this.window = window;
    this.nullTreatment = nullTreatment;
    this.distinct = distinct;
    this.processingMode = Optional.empty();
    this.arguments = arguments;
  }

  public FunctionCall(
      NodeLocation location,
      QualifiedName name,
      Optional<Window> window,
      Optional<NullTreatment> nullTreatment,
      boolean distinct,
      Optional<ProcessingMode> processingMode,
      List<Expression> arguments) {
    super(requireNonNull(location, "location is null"));

    this.name = name;
    this.window = window;
    this.nullTreatment = nullTreatment;
    this.distinct = distinct;
    this.processingMode = requireNonNull(processingMode, "processingMode is null");
    this.arguments = arguments;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public Optional<ProcessingMode> getProcessingMode() {
    return processingMode;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  public Optional<Window> getWindow() {
    return window;
  }

  public Optional<NullTreatment> getNullTreatment() {
    return nullTreatment;
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
        && Objects.equals(processingMode, o.processingMode)
        && Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, distinct, processingMode, arguments);
  }

  public enum NullTreatment {
    IGNORE,
    RESPECT
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    FunctionCall otherFunction = (FunctionCall) other;

    return name.equals(otherFunction.name)
        && distinct == otherFunction.distinct
        && processingMode.equals(otherFunction.processingMode);
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.FUNCTION_CALL;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    this.name.serialize(buffer);
    ReadWriteIOUtils.write(this.distinct, buffer);
    ReadWriteIOUtils.write(arguments.size(), buffer);
    for (Expression argument : arguments) {
      Expression.serialize(argument, buffer);
    }

    if (nullTreatment.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, buffer);
      ReadWriteIOUtils.write((byte) nullTreatment.get().ordinal(), buffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, buffer);
    }

    if (window.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, buffer);
      // Window type
      if (window.get() instanceof WindowReference) {
        ReadWriteIOUtils.write((byte) 0, buffer);
      } else {
        ReadWriteIOUtils.write((byte) 1, buffer);
      }
      window.get().serialize(buffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, buffer);
    }

    if (processingMode.isPresent()) {
      ReadWriteIOUtils.write(true, buffer);
      ProcessingMode mode = processingMode.get();
      ReadWriteIOUtils.write(mode.getMode().name(), buffer);
    } else {
      ReadWriteIOUtils.write(false, buffer);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    this.name.serialize(stream);
    ReadWriteIOUtils.write(this.distinct, stream);
    ReadWriteIOUtils.write(arguments.size(), stream);
    for (Expression argument : arguments) {
      Expression.serialize(argument, stream);
    }

    if (nullTreatment.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write((byte) nullTreatment.get().ordinal(), stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }

    if (window.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, stream);
      // Window type
      if (window.get() instanceof WindowReference) {
        ReadWriteIOUtils.write((byte) 0, stream);
      } else {
        ReadWriteIOUtils.write((byte) 1, stream);
      }
      window.get().serialize(stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }

    if (processingMode.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      ProcessingMode mode = processingMode.get();
      ReadWriteIOUtils.write(mode.getMode().name(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
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

    if (ReadWriteIOUtils.readByte(byteBuffer) == 1) {
      this.nullTreatment =
          Optional.of(NullTreatment.values()[ReadWriteIOUtils.readByte(byteBuffer)]);
    } else {
      this.nullTreatment = Optional.empty();
    }

    if (ReadWriteIOUtils.readByte(byteBuffer) == 1) {
      // Window type
      if (ReadWriteIOUtils.readByte(byteBuffer) == 0) {
        this.window = Optional.of(new WindowReference(byteBuffer));
      } else {
        this.window = Optional.of(new WindowSpecification(byteBuffer));
      }
    } else {
      this.window = Optional.empty();
    }

    boolean hasProcessingMode = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasProcessingMode) {
      String modeName = ReadWriteIOUtils.readString(byteBuffer);
      ProcessingMode.Mode mode = ProcessingMode.Mode.valueOf(modeName);
      this.processingMode = Optional.of(new ProcessingMode(null, mode));
    } else {
      this.processingMode = Optional.empty();
    }
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += name == null ? 0L : name.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(arguments);
    size += 3 * AstMemoryEstimationHelper.OPTIONAL_INSTANCE_SIZE;
    if (window.isPresent()) {
      Window windowValue = window.get();
      if (windowValue instanceof Node) {
        size += ((Node) windowValue).ramBytesUsed();
      }
    }
    size +=
        AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(processingMode.orElse(null));
    return size;
  }
}
