package com.timecho.iotdb.dataregion.compaction.selector.impl;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ICrossSpaceSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaResp;

import com.timecho.iotdb.dataregion.compaction.selector.utils.SharedStorageCompactionTaskResource;
import com.timecho.iotdb.dataregion.compaction.tool.SharedStorageCompactionUtils;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SharedStorageCompactionSelector implements ICrossSpaceSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SharedStorageCompactionSelector.class);
  private static final Map<String, Map<Long, Long>> dataRegionPartitionLastMaxVersion =
      new ConcurrentHashMap<>();
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private final long timePartition;
  private final String dataRegionId;
  private final DataRegion dataRegion;

  public SharedStorageCompactionSelector(String dataRegionId, long timePartition) {
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(Integer.parseInt(dataRegionId)));
  }

  @Override
  public List<CrossCompactionTaskResource> selectCrossSpaceTask(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long startTime = System.currentTimeMillis();
    long maxVersion = dataRegion.getPartitionMaxFileVersion(timePartition);
    long lastVersion =
        dataRegionPartitionLastMaxVersion
            .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
            .getOrDefault(timePartition, 0L);
    try {
      if (SharedStorageCompactionUtils.isLeader(dataRegion, timePartition)
          || !SharedStorageCompactionUtils.isAllFollowersSearchIndexConsumed(
              dataRegion, timePartition)
          || maxVersion <= lastVersion
          || !canShareOneReplica(dataRegion, timePartition, seqFiles, unseqFiles)) {
        return Collections.emptyList();
      }
    } catch (Exception e) {
      LOGGER.warn(TimechoServerMessages.FAILED_TO_SELECT_SHARED_STORAGE_COMPACTION_TASK, e);
      return Collections.emptyList();
    }
    long timeCost = System.currentTimeMillis() - startTime;
    CompactionMetrics.getInstance()
        .updateCompactionTaskSelectionTimeCost(CompactionTaskType.SHARED_STORAGE, timeCost);
    return Collections.singletonList(
        new SharedStorageCompactionTaskResource(seqFiles, unseqFiles, maxVersion));
  }

  private static boolean canShareOneReplica(
      DataRegion dataRegion,
      long timePartition,
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles) {
    if (seqFiles.isEmpty() && unseqFiles.isEmpty()) {
      return false;
    }
    // not latest time partition and all memTables are flushed
    if (timePartition >= dataRegion.getLatestTimePartition()
        || dataRegion.hasWorkTsFileProcessor(timePartition)) {
      return false;
    }
    // all files on the remote
    for (TsFileResource tsFileResource : seqFiles) {
      if (tsFileResource.getStatus() != TsFileResourceStatus.NORMAL_ON_REMOTE) {
        return false;
      }
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      if (tsFileResource.getStatus() != TsFileResourceStatus.NORMAL_ON_REMOTE) {
        return false;
      }
    }
    return true;
  }

  public static TFetchLeaderRemoteReplicaResp selectLeaderFiles(
      int dataRegionId, long timePartition) throws IOException {
    DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    List<TsFileResource> seqFiles =
        dataRegion.getTsFileManager().getOrCreateSequenceListByTimePartition(timePartition);
    List<TsFileResource> unseqFiles =
        dataRegion.getTsFileManager().getOrCreateUnsequenceListByTimePartition(timePartition);
    TFetchLeaderRemoteReplicaResp resp =
        new TFetchLeaderRemoteReplicaResp(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    try {
      if (!SharedStorageCompactionUtils.isLeader(dataRegion, timePartition)
          || !canShareOneReplica(dataRegion, timePartition, seqFiles, unseqFiles)) {
        return resp;
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to select leader files for data region {}'s shared storage compaction task in time partition {}.",
          dataRegionId,
          timePartition,
          e);
      return resp;
    }

    for (TsFileResource f : seqFiles) {
      serializeFile(resp, f);
    }
    for (TsFileResource f : unseqFiles) {
      serializeFile(resp, f);
    }
    return resp;
  }

  private static void serializeFile(TFetchLeaderRemoteReplicaResp resp, TsFileResource resource)
      throws IOException {
    resp.fileNames.add(resource.getTsFile().getName());
    // resource file is small, we can transfer it directly
    File resourceFile = fsFactory.getFile(resource.getTsFile() + TsFileResource.RESOURCE_SUFFIX);
    resp.resourceFiles.add(readFileContent(resourceFile));
    // mods file is smaller than 1MB because of the mods compaction, so it's safe to transfer it
    // directly
    File modsFile = fsFactory.getFile(resource.getTsFile() + ModificationFile.FILE_SUFFIX);
    resp.modsFiles.add(modsFile.exists() ? readFileContent(resourceFile) : ByteBuffer.allocate(0));
  }

  private static ByteBuffer readFileContent(File file) throws IOException {
    try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
      channel.read(buffer);
      buffer.clear();
      return buffer;
    }
  }

  public static void updateLastVersion(String dataRegionId, long timePartition, long version) {
    dataRegionPartitionLastMaxVersion
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .put(timePartition, version);
  }
}
