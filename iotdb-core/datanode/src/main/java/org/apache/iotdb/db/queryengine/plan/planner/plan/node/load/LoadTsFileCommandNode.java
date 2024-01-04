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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;

public class LoadTsFileCommandNode extends WritePlanNode implements ComparableConsensusRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileCommandNode.class);
  private final LoadCommand command;
  private final String uuid;
  private ProgressIndex progressIndex;

  // for load locally
  private final boolean isLoadedLocally;
  private String tsFilePath;
  private TsFileResource tsFileResource;
  private boolean isDeleteAfterLoad;

  public LoadTsFileCommandNode(
      PlanNodeId id, LoadCommand command, String uuid, boolean isGeneratedByPipe) {
    this(id, command, uuid, isGeneratedByPipe, false, null, null, false);
  }

  public LoadTsFileCommandNode(
      PlanNodeId id,
      LoadCommand command,
      String uuid,
      boolean isGeneratedByPipe,
      boolean isLoadedLocally,
      String tsFilePath,
      TsFileResource tsFileResource,
      boolean isDeleteAfterLoad) {
    super(id);
    this.command = command;
    this.uuid = uuid;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.isLoadedLocally = isLoadedLocally;
    this.tsFilePath = tsFilePath;
    this.tsFileResource = tsFileResource;
    this.isDeleteAfterLoad = isDeleteAfterLoad;
  }

  public LoadCommand getCommand() {
    return command;
  }

  public String getUuid() {
    return uuid;
  }

  public boolean getIsGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  public boolean isLoadedLocally() {
    return isLoadedLocally;
  }

  public TsFileResource getTsFileResource() throws IOException {
    if (tsFileResource == null) {
      tsFileResource = new TsFileResource(new File(tsFilePath));
      tsFileResource.deserialize();
    }
    return tsFileResource;
  }

  public boolean isDeleteAfterLoad() {
    return isDeleteAfterLoad;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLoadTsFileCommand(this, context);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    throw new NotImplementedException(
        String.format(
            "getRegionReplicaSet of %s is not implemented",
            LoadTsFileCommandNode.class.getSimpleName()));
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    // do nothing
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException(
        String.format(
            "clone of %s is not implemented", LoadTsFileCommandNode.class.getSimpleName()));
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    try {
      ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      DataOutputStream stream = new DataOutputStream(byteOutputStream);
      serializeAttributes(stream);
      byteBuffer.put(byteOutputStream.toByteArray());
    } catch (IOException e) {
      LOGGER.error("Serialize to ByteBuffer error.", e);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LOAD_TSFILE_COMMAND.serialize(stream);
    ReadWriteIOUtils.write(command.ordinal(), stream);
    ReadWriteIOUtils.write(uuid, stream);
    ReadWriteIOUtils.write(isGeneratedByPipe, stream);
    ReadWriteIOUtils.write(isLoadedLocally, stream);
    if (isLoadedLocally) {
      ReadWriteIOUtils.write(tsFilePath, stream);
      ReadWriteIOUtils.write(isDeleteAfterLoad, stream);
    }
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    InputStream stream = new ByteArrayInputStream(buffer.array());
    try {
      ReadWriteIOUtils.readShort(stream); // read PlanNodeType
      final int commandIndex = ReadWriteIOUtils.readInt(stream);
      final String uuid = ReadWriteIOUtils.readString(stream);
      final boolean isGeneratedByPipe = ReadWriteIOUtils.readBool(stream);
      final boolean isLoadedLocally = ReadWriteIOUtils.readBool(stream);
      if (isLoadedLocally) {
        final String tsFilePath = ReadWriteIOUtils.readString(stream);
        final boolean isDeleteAfterLoad = ReadWriteIOUtils.readBool(stream);
        return new LoadTsFileCommandNode(
            PlanNodeId.deserialize(stream),
            LoadCommand.values()[commandIndex],
            uuid,
            isGeneratedByPipe,
            true,
            tsFilePath,
            null,
            isDeleteAfterLoad);
      }

      return new LoadTsFileCommandNode(
          PlanNodeId.deserialize(stream),
          LoadCommand.values()[commandIndex],
          uuid,
          isGeneratedByPipe);
    } catch (IOException e) {
      LOGGER.error("Deserialize {} error.", LoadTsFilePieceNode.class.getSimpleName(), e);
      return null;
    }
  }

  public TLoadCommandReq toTLoadCommandReq() {
    return new TLoadCommandReq(command.ordinal(), uuid)
        .setIsGeneratedByPipe(isGeneratedByPipe)
        .setNodeId(getPlanNodeId().toString());
  }

  public static LoadTsFileCommandNode fromTLoadCommandReq(TLoadCommandReq req) {
    return new LoadTsFileCommandNode(
        new PlanNodeId(req.isSetNodeId() ? req.nodeId : ""),
        LoadCommand.values()[req.commandType],
        req.uuid,
        req.isSetIsGeneratedByPipe() && req.isGeneratedByPipe);
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    throw new NotImplementedException(
        String.format("split %s is not implemented", LoadTsFileCommandNode.class.getSimpleName()));
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    LoadTsFileCommandNode that = (LoadTsFileCommandNode) o;
    return command == that.command
        && Objects.equals(uuid, that.uuid)
        && isGeneratedByPipe == that.isGeneratedByPipe;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), command, uuid, isGeneratedByPipe);
  }

  @Override
  public String toString() {
    return "LoadTsFileCommandNode{"
        + "command="
        + command
        + ", uuid='"
        + uuid
        + '\''
        + ", progressIndex="
        + progressIndex
        + ", isLoadedLocally="
        + isLoadedLocally
        + ", tsFilePath='"
        + tsFilePath
        + '\''
        + ", isDeleteAfterLoad="
        + isDeleteAfterLoad
        + ", isGeneratedByPipe="
        + isGeneratedByPipe
        + '}';
  }
}
