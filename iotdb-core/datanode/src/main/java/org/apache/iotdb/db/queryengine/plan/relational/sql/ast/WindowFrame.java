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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowFrame extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(WindowFrame.class);

  public enum Type {
    RANGE,
    ROWS,
    GROUPS
  }

  private final Type type;
  private final FrameBound start;
  private final Optional<FrameBound> end;

  public WindowFrame(NodeLocation location, Type type, FrameBound start, Optional<FrameBound> end) {
    super(requireNonNull(location));
    this.type = requireNonNull(type, "type is null");
    this.start = requireNonNull(start, "start is null");
    this.end = requireNonNull(end, "end is null");
  }

  public Type getType() {
    return type;
  }

  public FrameBound getStart() {
    return start;
  }

  public Optional<FrameBound> getEnd() {
    return end;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWindowFrame(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(start);
    end.ifPresent(nodes::add);
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
    WindowFrame o = (WindowFrame) obj;
    return type == o.type && Objects.equals(start, o.start) && Objects.equals(end, o.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, start, end);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("type", type).add("start", start).add("end", end).toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    WindowFrame otherNode = (WindowFrame) other;
    return type == otherNode.type;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write((byte) type.ordinal(), buffer);
    start.serialize(buffer);
    if (end.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, buffer);
      end.get().serialize(buffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, buffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write((byte) type.ordinal(), stream);
    start.serialize(stream);
    if (end.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, stream);
      end.get().serialize(stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }
  }

  public WindowFrame(ByteBuffer byteBuffer) {
    super(null);
    type = Type.values()[ReadWriteIOUtils.readByte(byteBuffer)];
    start = new FrameBound(byteBuffer);

    if (ReadWriteIOUtils.readByte(byteBuffer) == 1) {
      end = Optional.of(new FrameBound(byteBuffer));
    } else {
      end = Optional.empty();
    }
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(start);
    size += AstMemoryEstimationHelper.OPTIONAL_INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(end.orElse(null));
    return size;
  }
}
