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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.load.TsFileData;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.exception.write.PageException;
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
import java.util.ArrayList;
import java.util.List;

public class LoadTsFilePieceNode extends WritePlanNode {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFilePieceNode.class);

  private File tsFile;

  private long dataSize;
  private List<TsFileData> tsFileDataList;

  public LoadTsFilePieceNode(PlanNodeId id) {
    super(id);
  }

  public LoadTsFilePieceNode(PlanNodeId id, File tsFile) {
    super(id);
    this.tsFile = tsFile;
    this.dataSize = 0;
    this.tsFileDataList = new ArrayList<>();
  }

  public long getDataSize() {
    return dataSize;
  }

  public void addTsFileData(TsFileData tsFileData) {
    tsFileDataList.add(tsFileData);
    dataSize += tsFileData.getDataSize();
  }

  public List<TsFileData> getAllTsFileData() {
    return tsFileDataList;
  }

  public File getTsFile() {
    return tsFile;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of load piece TsFile is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    try {
      ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      DataOutputStream stream = new DataOutputStream(byteOutputStream);
      serializeAttributes(stream);
      byteBuffer.put(byteOutputStream.toByteArray());
    } catch (IOException e) {
      logger.error("Serialize to ByteBuffer error.", e);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LOAD_TSFILE.serialize(stream);
    ReadWriteIOUtils.write(tsFile.getPath(), stream); // TODO: can save this space
    ReadWriteIOUtils.write(tsFileDataList.size(), stream);
    for (TsFileData tsFileData : tsFileDataList) {
      try {
        tsFileData.serialize(stream);
      } catch (IOException e) {
        logger.error(
            String.format(
                "Serialize data of TsFile %s error, skip TsFileData %s",
                tsFile.getPath(), tsFileData));
      }
    }
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    throw new NotImplementedException("split load piece TsFile is not implemented");
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    InputStream stream = new ByteArrayInputStream(buffer.array());
    try {
      ReadWriteIOUtils.readShort(stream); // read PlanNodeType
      File tsFile = new File(ReadWriteIOUtils.readString(stream));
      LoadTsFilePieceNode pieceNode = new LoadTsFilePieceNode(new PlanNodeId(""), tsFile);
      int tsFileDataSize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < tsFileDataSize; i++) {
        TsFileData tsFileData = TsFileData.deserialize(stream);
        pieceNode.addTsFileData(tsFileData);
      }
      pieceNode.setPlanNodeId(PlanNodeId.deserialize(stream));
      return pieceNode;
    } catch (IOException | PageException | IllegalPathException e) {
      logger.error(String.format("Deserialize %s error.", LoadTsFilePieceNode.class.getName()), e);
      return null;
    }
  }

  @Override
  public String toString() {
    return "LoadTsFilePieceNode{" + "tsFile=" + tsFile + ", dataSize=" + dataSize + '}';
  }
}
