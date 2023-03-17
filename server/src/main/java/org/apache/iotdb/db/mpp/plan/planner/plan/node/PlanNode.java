/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** The base class of query logical plan nodes, which is used to compose logical query plan. */
public abstract class PlanNode implements IConsensusRequest {

  private final Logger logger = LoggerFactory.getLogger(PlanNode.class);

  protected static final int NO_CHILD_ALLOWED = 0;
  protected static final int ONE_CHILD = 1;
  protected static final int CHILD_COUNT_NO_LIMIT = -1;

  private PlanNodeId id;

  protected PlanNode(PlanNodeId id) {
    requireNonNull(id, "id is null");
    this.id = id;
  }

  public PlanNodeId getPlanNodeId() {
    return id;
  }

  public void setPlanNodeId(PlanNodeId id) {
    this.id = id;
  }

  public abstract List<PlanNode> getChildren();

  public abstract void addChild(PlanNode child);

  @Override
  public abstract PlanNode clone();

  /**
   * Create sub node which has exactly the same function of origin node, only its children is a part
   * of it, which is composed by the [startIndex, endIndex) of origin children list.
   *
   * @param subNodeId the sub node id
   * @param startIndex the start Index of origin children
   * @param endIndex the endIndex Index of origin children
   */
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    throw new UnsupportedOperationException(
        String.format("Can't create subNode for %s", this.getClass().toString()));
  }

  public PlanNode cloneWithChildren(List<PlanNode> children) {
    if (!(children == null
        || allowedChildCount() == CHILD_COUNT_NO_LIMIT
        || children.size() == allowedChildCount())) {
      throw new IllegalArgumentException(
          String.format(
              "Child count is not correct for PlanNode. Expected: %d, Value: %d",
              allowedChildCount(), getChildrenCount(children)));
    }
    PlanNode node = clone();
    if (children != null) {
      children.forEach(node::addChild);
    }
    return node;
  }

  private int getChildrenCount(List<PlanNode> children) {
    return children == null ? 0 : children.size();
  }

  public abstract int allowedChildCount();

  public abstract List<String> getOutputColumnNames();

  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPlan(this, context);
  }

  public void serialize(ByteBuffer byteBuffer) {
    serializeAttributes(byteBuffer);
    id.serialize(byteBuffer);
    List<PlanNode> planNodes = getChildren();
    if (planNodes == null) {
      ReadWriteIOUtils.write(0, byteBuffer);
    } else {
      ReadWriteIOUtils.write(planNodes.size(), byteBuffer);
      for (PlanNode planNode : planNodes) {
        planNode.serialize(byteBuffer);
      }
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    serializeAttributes(stream);
    id.serialize(stream);
    List<PlanNode> planNodes = getChildren();
    if (planNodes == null) {
      ReadWriteIOUtils.write(0, stream);
    } else {
      ReadWriteIOUtils.write(planNodes.size(), stream);
      for (PlanNode planNode : planNodes) {
        planNode.serialize(stream);
      }
    }
  }

  /**
   * Deserialize via {@link
   * org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType#deserialize(ByteBuffer)}
   */
  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      logger.error("Unexpected error occurs when serializing writePlanNode.", e);
      throw new SerializationRunTimeException(e);
    }
  }

  protected abstract void serializeAttributes(ByteBuffer byteBuffer);

  protected abstract void serializeAttributes(DataOutputStream stream) throws IOException;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlanNode planNode = (PlanNode) o;
    return Objects.equals(id, planNode.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
