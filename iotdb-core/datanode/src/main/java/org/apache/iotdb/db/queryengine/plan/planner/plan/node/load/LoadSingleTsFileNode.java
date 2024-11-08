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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class LoadSingleTsFileNode extends WritePlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadSingleTsFileNode.class);

  private final File tsFile;
  private final TsFileResource resource;
  private final boolean isTableModel;
  private final String database;
  private final boolean deleteAfterLoad;
  private final long writePointCount;
  private boolean needDecodeTsFile;

  private TRegionReplicaSet localRegionReplicaSet;

  public LoadSingleTsFileNode(
      PlanNodeId id,
      TsFileResource resource,
      boolean isTableModel,
      String database,
      boolean deleteAfterLoad,
      long writePointCount) {
    super(id);
    this.tsFile = resource.getTsFile();
    this.resource = resource;
    this.isTableModel = isTableModel;
    this.database = database;
    this.deleteAfterLoad = deleteAfterLoad;
    this.writePointCount = writePointCount;
  }

  public boolean isTsFileEmpty() {
    return resource.getDevices().isEmpty();
  }

  public boolean needDecodeTsFile(
      Function<List<Pair<IDeviceID, TTimePartitionSlot>>, List<TRegionReplicaSet>> partitionFetcher)
      throws IOException {
    List<Pair<IDeviceID, TTimePartitionSlot>> slotList = new ArrayList<>();
    resource
        .getDevices()
        .forEach(
            o -> {
              slotList.add(
                  new Pair<>(o, TimePartitionUtils.getTimePartitionSlot(resource.getStartTime(o))));
              slotList.add(
                  new Pair<>(o, TimePartitionUtils.getTimePartitionSlot(resource.getEndTime(o))));
            });

    if (slotList.isEmpty()) {
      throw new IllegalStateException(
          String.format("Devices in TsFile %s is empty, this should not happen here.", tsFile));
    } else if (slotList.stream()
        .anyMatch(slotPair -> !slotPair.getRight().equals(slotList.get(0).right))) {
      needDecodeTsFile = true;
    } else {
      needDecodeTsFile = !isDispatchedToLocal(new HashSet<>(partitionFetcher.apply(slotList)));
    }

    return needDecodeTsFile;
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
      if (dataNodeLocationList.size() == 1) {
        return isDispatchedToLocal(dataNodeLocationList.get(0).getInternalEndPoint());
      }
    }

    return true;
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return IoTDBDescriptor.getInstance().getConfig().getInternalAddress().equals(endPoint.getIp())
        && IoTDBDescriptor.getInstance().getConfig().getInternalPort() == endPoint.port;
  }

  public boolean isDeleteAfterLoad() {
    return deleteAfterLoad;
  }

  public boolean isTableModel() {
    return isTableModel;
  }

  public long getWritePointCount() {
    return writePointCount;
  }

  /**
   * only used for load locally.
   *
   * @return local TRegionReplicaSet
   */
  public TRegionReplicaSet getLocalRegionReplicaSet() {
    return localRegionReplicaSet;
  }

  public TsFileResource getTsFileResource() {
    return resource;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    // Do nothing
  }

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
    return Collections.emptyList();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    // Do nothing
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    // Do nothing
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
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
        Files.deleteIfExists(ModificationFile.getExclusiveMods(tsFile).toPath());
        Files.deleteIfExists(
            new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).toPath());
      }
    } catch (final IOException e) {
      LOGGER.warn("Delete After Loading {} error.", tsFile, e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LoadSingleTsFileNode loadSingleTsFileNode = (LoadSingleTsFileNode) o;
    return Objects.equals(tsFile, loadSingleTsFileNode.tsFile)
        && Objects.equals(resource, loadSingleTsFileNode.resource)
        && Objects.equals(isTableModel, loadSingleTsFileNode.isTableModel)
        && Objects.equals(database, loadSingleTsFileNode.database)
        && Objects.equals(needDecodeTsFile, loadSingleTsFileNode.needDecodeTsFile)
        && Objects.equals(deleteAfterLoad, loadSingleTsFileNode.deleteAfterLoad)
        && Objects.equals(localRegionReplicaSet, loadSingleTsFileNode.localRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        tsFile,
        resource,
        isTableModel,
        database,
        needDecodeTsFile,
        deleteAfterLoad,
        localRegionReplicaSet);
  }
}
