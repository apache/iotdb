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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class LinearFillNode extends FillNode {
  private final Symbol helperColumn;

  @Nullable private final List<Symbol> groupingKeys;

  public LinearFillNode(
      PlanNodeId id, PlanNode child, Symbol helperColumn, List<Symbol> groupingKeys) {
    super(id, child);
    this.helperColumn = helperColumn;
    this.groupingKeys = groupingKeys;
  }

  public Symbol getHelperColumn() {
    return helperColumn;
  }

  public Optional<List<Symbol>> getGroupingKeys() {
    return Optional.ofNullable(groupingKeys);
  }

  @Override
  public PlanNode clone() {
    return new LinearFillNode(id, null, helperColumn, groupingKeys);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLinearFill(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_LINEAR_FILL_NODE.serialize(byteBuffer);
    Symbol.serialize(helperColumn, byteBuffer);
    if (groupingKeys == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(groupingKeys.size(), byteBuffer);
      for (Symbol symbol : groupingKeys) {
        Symbol.serialize(symbol, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_LINEAR_FILL_NODE.serialize(stream);
    Symbol.serialize(helperColumn, stream);
    if (groupingKeys == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(groupingKeys.size(), stream);
      for (Symbol symbol : groupingKeys) {
        Symbol.serialize(symbol, stream);
      }
    }
  }

  public static LinearFillNode deserialize(ByteBuffer byteBuffer) {
    Symbol helperColumn = Symbol.deserialize(byteBuffer);
    boolean hasValue = ReadWriteIOUtils.readBool(byteBuffer);
    List<Symbol> groupingKeys = null;
    if (hasValue) {
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      groupingKeys = new ArrayList<>(size);
      while (size-- > 0) {
        groupingKeys.add(Symbol.deserialize(byteBuffer));
      }
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LinearFillNode(planNodeId, null, helperColumn, groupingKeys);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new LinearFillNode(
        id, Iterables.getOnlyElement(newChildren), helperColumn, groupingKeys);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LinearFillNode that = (LinearFillNode) o;
    return Objects.equals(helperColumn, that.helperColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), helperColumn);
  }

  @Override
  public String toString() {
    return "LinearFillNode-" + this.getPlanNodeId();
  }
}
