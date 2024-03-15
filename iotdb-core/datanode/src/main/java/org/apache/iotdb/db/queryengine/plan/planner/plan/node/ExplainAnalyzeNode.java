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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ExplainAnalyzeNode extends SingleChildProcessNode {
  private final boolean verbose;

  private final long queryId;
  private final long timeout;

  public ExplainAnalyzeNode(
      PlanNodeId id, PlanNode child, boolean verbose, long queryId, long timeout) {
    super(id, child);
    this.verbose = verbose;
    this.queryId = queryId;
    this.timeout = timeout;
  }

  @Override
  public PlanNode clone() {
    return new ExplainAnalyzeNode(getPlanNodeId(), child, verbose, queryId, timeout);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitExplainAnalyze(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.singletonList(ColumnHeaderConstant.EXPLAIN_ANALYZE);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.EXPLAIN_ANALYZE.serialize(byteBuffer);
    ReadWriteIOUtils.write(verbose, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.EXPLAIN_ANALYZE.serialize(stream);
    ReadWriteIOUtils.write(verbose, stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(timeout, stream);
  }

  public static ExplainAnalyzeNode deserialize(ByteBuffer byteBuffer) {
    boolean verbose = ReadWriteIOUtils.readBool(byteBuffer);
    long queryId = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    long timeout = ReadWriteIOUtils.readLong(byteBuffer);
    return new ExplainAnalyzeNode(planNodeId, null, verbose, queryId, timeout);
  }

  public boolean isVerbose() {
    return verbose;
  }

  public long getQueryId() {
    return queryId;
  }

  public long getTimeout() {
    return timeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ExplainAnalyzeNode)) return false;
    ExplainAnalyzeNode that = (ExplainAnalyzeNode) o;
    return verbose == that.verbose && queryId == that.queryId && timeout == that.timeout;
  }

  @Override
  public int hashCode() {
    return super.hashCode()
        + Boolean.hashCode(verbose)
        + Long.hashCode(queryId)
        + Long.hashCode(timeout);
  }
}
