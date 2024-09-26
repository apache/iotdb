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
import org.apache.tsfile.utils.TimeDuration;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PreviousFillNode extends FillNode {

  @Nullable private final TimeDuration timeDuration;

  @Nullable private final Symbol helperColumn;

  public PreviousFillNode(
      PlanNodeId id, PlanNode child, TimeDuration timeDuration, Symbol helperColumn) {
    super(id, child);
    this.timeDuration = timeDuration;
    this.helperColumn = helperColumn;
  }

  public Optional<TimeDuration> getTimeDuration() {
    return Optional.ofNullable(timeDuration);
  }

  public Optional<Symbol> getHelperColumn() {
    return Optional.ofNullable(helperColumn);
  }

  @Override
  public PlanNode clone() {
    return new PreviousFillNode(id, null, timeDuration, helperColumn);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPreviousFill(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_PREVIOUS_FILL_NODE.serialize(byteBuffer);
    if (timeDuration == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      timeDuration.serialize(byteBuffer);
    }
    if (helperColumn == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      Symbol.serialize(helperColumn, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_PREVIOUS_FILL_NODE.serialize(stream);
    if (timeDuration == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      timeDuration.serialize(stream);
    }
    if (helperColumn == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      Symbol.serialize(helperColumn, stream);
    }
  }

  public static PreviousFillNode deserialize(ByteBuffer byteBuffer) {
    boolean isNull = ReadWriteIOUtils.readBool(byteBuffer);
    TimeDuration timeDuration = null;
    if (!isNull) {
      timeDuration = TimeDuration.deserialize(byteBuffer);
    }
    isNull = ReadWriteIOUtils.readBool(byteBuffer);
    Symbol helperColumn = null;
    if (!isNull) {
      helperColumn = Symbol.deserialize(byteBuffer);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new PreviousFillNode(planNodeId, null, timeDuration, helperColumn);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new PreviousFillNode(
        id, Iterables.getOnlyElement(newChildren), timeDuration, helperColumn);
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
    PreviousFillNode that = (PreviousFillNode) o;
    return Objects.equals(timeDuration, that.timeDuration)
        && Objects.equals(helperColumn, that.helperColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeDuration, helperColumn);
  }

  @Override
  public String toString() {
    return "PreviousFillNode-" + this.getPlanNodeId();
  }
}
