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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

// TODO:[OBJECT] WAL serde
public class FileNode {

  private final boolean isEOF;

  private final long offset;

  private byte[] content;

  private String filePath;

  public FileNode(boolean isEOF, long offset, byte[] content) {
    this.isEOF = isEOF;
    this.offset = offset;
    this.content = content;
  }

  public boolean isEOF() {
    return isEOF;
  }

  public byte[] getContent() {
    return content;
  }

  public long getOffset() {
    return offset;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getFilePath() {
    return filePath;
  }
  //
  //  @Override
  //  public void serializeToWAL(IWALByteBufferView buffer) {}
  //
  //  @Override
  //  public int serializedSize() {
  //    return 0;
  //  }
  //
  //  @Override
  //  public SearchNode merge(List<SearchNode> searchNodes) {
  //    return null;
  //  }
  //
  //  @Override
  //  public ProgressIndex getProgressIndex() {
  //    return null;
  //  }
  //
  //  @Override
  //  public void setProgressIndex(ProgressIndex progressIndex) {}
  //
  //  @Override
  //  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
  //    return List.of();
  //  }
  //
  //  @Override
  //  public TRegionReplicaSet getRegionReplicaSet() {
  //    return null;
  //  }
  //
  //  @Override
  //  public List<PlanNode> getChildren() {
  //    return List.of();
  //  }
  //
  //  @Override
  //  public void addChild(PlanNode child) {}
  //
  //  @Override
  //  public PlanNode clone() {
  //    return null;
  //  }
  //
  //  @Override
  //  public int allowedChildCount() {
  //    return 0;
  //  }
  //
  //  @Override
  //  public List<String> getOutputColumnNames() {
  //    return List.of();
  //  }
  //
  //  @Override
  //  protected void serializeAttributes(ByteBuffer byteBuffer) {}
  //
  //  @Override
  //  protected void serializeAttributes(DataOutputStream stream) throws IOException {}
}
