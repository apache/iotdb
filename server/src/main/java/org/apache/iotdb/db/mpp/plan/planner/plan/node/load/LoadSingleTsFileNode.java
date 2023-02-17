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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LoadSingleTsFileNode extends WritePlanNode {
  private static final Logger logger = LoggerFactory.getLogger(LoadSingleTsFileNode.class);

  private File tsFile;
  private TsFileResource resource;
  private boolean needDecodeTsFile;
  private boolean deleteAfterLoad;

  private TRegionReplicaSet localRegionReplicaSet;
  private DataPartition dataPartition;

  public LoadSingleTsFileNode(PlanNodeId id) {
    super(id);
  }

  public LoadSingleTsFileNode(
      PlanNodeId id,
      TsFileResource resource,
      boolean deleteAfterLoad,
      DataPartition dataPartition) {
    super(id);
    this.tsFile = resource.getTsFile();
    this.resource = resource;
    this.deleteAfterLoad = deleteAfterLoad;
    this.dataPartition = dataPartition;
  }

  public void checkIfNeedDecodeTsFile() throws IOException {
    Set<TRegionReplicaSet> allRegionReplicaSet = new HashSet<>();
    TTimePartitionSlot timePartitionSlot = null;
    needDecodeTsFile = false;
    for (String device : resource.getDevices()) {
      TTimePartitionSlot startSlot =
          TimePartitionUtils.getTimePartition(resource.getStartTime(device));
      if (timePartitionSlot == null) {
        timePartitionSlot = startSlot;
      }
      if (!startSlot.equals(timePartitionSlot)
          || !TimePartitionUtils.getTimePartition(resource.getEndTime(device))
              .equals(timePartitionSlot)) {
        needDecodeTsFile = true;
        return;
      }
      allRegionReplicaSet.add(
          dataPartition.getDataRegionReplicaSetForWriting(device, timePartitionSlot));
    }
    needDecodeTsFile = !isDispatchedToLocal(allRegionReplicaSet);
    if (!needDecodeTsFile && !resource.resourceFileExists()) {
      resource.serialize();
    }
  }

  private boolean isDispatchedToLocal(Set<TRegionReplicaSet> replicaSets) {
    if (replicaSets.size() > 1) {
      return false;
    }
    for (TRegionReplicaSet replicaSet : replicaSets) {
      List<TDataNodeLocation> dataNodeLocationList = replicaSet.getDataNodeLocations();
      if (dataNodeLocationList.size() > 1) {
        return false;
      }
      localRegionReplicaSet = replicaSet;
      return isDispatchedToLocal(dataNodeLocationList.get(0).getInternalEndPoint());
    }
    return true;
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return IoTDBDescriptor.getInstance().getConfig().getInternalAddress().equals(endPoint.getIp())
        && IoTDBDescriptor.getInstance().getConfig().getInternalPort() == endPoint.port;
  }

  public boolean needDecodeTsFile() {
    return needDecodeTsFile;
  }

  public boolean isDeleteAfterLoad() {
    return deleteAfterLoad;
  }

  /**
   * only used for load locally.
   *
   * @return local TRegionReplicaSet
   */
  public TRegionReplicaSet getLocalRegionReplicaSet() {
    return localRegionReplicaSet;
  }

  public DataPartition getDataPartition() {
    return dataPartition;
  }

  public TsFileResource getTsFileResource() {
    return resource;
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
    throw new NotImplementedException("clone of load single TsFile is not implemented");
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    throw new NotImplementedException("split load single TsFile is not implemented");
  }

  @Override
  public String toString() {
    return "LoadSingleTsFileNode{"
        + "tsFile="
        + tsFile
        + ", needDecodeTsFile="
        + needDecodeTsFile
        + '}';
  }

  public void clean() {
    try {
      if (deleteAfterLoad) {
        Files.deleteIfExists(tsFile.toPath());
        Files.deleteIfExists(
            new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
        Files.deleteIfExists(
            new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).toPath());
      }
    } catch (IOException e) {
      logger.warn(String.format("Delete After Loading %s error.", tsFile), e);
    }
  }
}
