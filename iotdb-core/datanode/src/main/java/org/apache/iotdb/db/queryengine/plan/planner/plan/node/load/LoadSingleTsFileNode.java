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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;

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
      final PlanNodeId id,
      final TsFileResource resource,
      final boolean isTableModel,
      final String database,
      final boolean deleteAfterLoad,
      final long writePointCount,
      final boolean needDecodeTsFile) {
    super(id);
    this.tsFile = resource.getTsFile();
    this.resource = resource;
    this.isTableModel = isTableModel;
    this.database = database;
    this.deleteAfterLoad = deleteAfterLoad;
    this.writePointCount = writePointCount;
    this.needDecodeTsFile = needDecodeTsFile;
  }

  public boolean isTsFileEmpty() {
    return resource.getDevices().isEmpty();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public boolean needDecodeTsFile(
      Function<List<Pair<IDeviceID, TTimePartitionSlot>>, List<TRegionReplicaSet>>
          partitionFetcher) {
    if (needDecodeTsFile) {
      return true;
    }

    List<Pair<IDeviceID, TTimePartitionSlot>> slotList =
        new ArrayList<>(resource.getDevices().size() << 1);
    for (final IDeviceID device : resource.getDevices()) {
      // iterating the index, must present
      final TTimePartitionSlot startSlot =
          TimePartitionUtils.getTimePartitionSlot(resource.getStartTime(device).get());
      final TTimePartitionSlot endSlot =
          TimePartitionUtils.getTimePartitionSlot(resource.getEndTime(device).get());
      slotList.add(new Pair<>(device, startSlot));
      if (!startSlot.equals(endSlot)) {
        slotList.add(new Pair<>(device, endSlot));
      }
    }

    if (slotList.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_DEVICES_IN_TSFILE_S_IS_EMPTY_THIS_SHOULD_NOT_HAPPEN_HERE_BC1BE63C,
              tsFile));
    } else {
      final TTimePartitionSlot firstSlot = slotList.get(0).right;
      for (int i = 1, size = slotList.size(); i < size; i++) {
        if (!slotList.get(i).right.equals(firstSlot)) {
          needDecodeTsFile = true;
          return true;
        }
      }
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
    throw new NotImplementedException(
        DataNodeQueryMessages.CLONE_OF_LOAD_SINGLE_TSFILE_IS_NOT_IMPLEMENTED);
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
    throw new NotImplementedException(
        DataNodeQueryMessages.SPLIT_LOAD_SINGLE_TSFILE_IS_NOT_IMPLEMENTED);
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
    if (!deleteAfterLoad) {
      return;
    }
    deleteFile(tsFile);
    deleteFile(new File(LoadUtil.getTsFileResourcePath(tsFile.getAbsolutePath())));
    deleteFile(ModificationFile.getExclusiveMods(tsFile));
    deleteFile(new File(LoadUtil.getTsFileModsV1Path(tsFile.getAbsolutePath())));
  }

  private void deleteFile(final File file) {
    try {
      RetryUtils.retryOnException(
          () -> {
            Files.deleteIfExists(file.toPath());
            return null;
          });
    } catch (final Exception e) {
      LOGGER.warn(DataNodeQueryMessages.DELETE_AFTER_LOADING_ERROR, file, e);
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
