package com.timecho.iotdb.dataregion.compaction.tool;

import org.apache.iotdb.calc.utils.ObjectPathNaming;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.objectgc.ObjectDirectoryScanner;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.mpp.rpc.thrift.TFetchIoTConsensusProgressReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchIoTConsensusProgressResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaResp;

import com.timecho.iotdb.i18n.TimechoServerMessages;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.timecho.iotdb.dataregion.compaction.execute.task.SharedStorageCompactionTask.REMOTE_TMP_FILE_SUFFIX;

public class SharedStorageCompactionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SharedStorageCompactionUtils.class);
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public static boolean isLeader(DataRegion dataRegion, long timePartition) throws Exception {
    return isLocal(
        getDataRegionReplicaSet(dataRegion, timePartition)
            .get(0)
            .getDataNodeLocations()
            .get(0)
            .getInternalEndPoint());
  }

  public static boolean isAllFollowersSearchIndexConsumed(DataRegion dataRegion, long timePartition)
      throws Exception {
    List<TDataNodeLocation> dataNodeLocations =
        getDataRegionReplicaSet(dataRegion, timePartition).get(0).getDataNodeLocations();
    for (int i = 1; i < dataNodeLocations.size(); ++i) {
      TEndPoint endPoint = dataNodeLocations.get(i).getInternalEndPoint();
      if (isLocal(endPoint)) {
        if (!dataRegion.isAllSearchIndexSafelyDeleted()) {
          return false;
        }
      } else {
        try (SyncDataNodeInternalServiceClient client =
            Coordinator.getInstance().getInternalServiceClientManager().borrowClient(endPoint)) {
          TFetchIoTConsensusProgressResp resp =
              client.fetchIoTConsensusProgress(
                  new TFetchIoTConsensusProgressReq(
                      Integer.parseInt(dataRegion.getDataRegionIdString())));
          if (resp == null || !resp.isAllSearchIndexSafelyDeleted) {
            return false;
          }
        } catch (Exception e) {
          return false;
        }
      }
    }
    return true;
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private static List<TRegionReplicaSet> getDataRegionReplicaSet(
      DataRegion dataRegion, long timePartition) throws Exception {
    TsFileResource selectedResource =
        dataRegion.getTsFileManager().getAValidTsFileResourceForMigration(timePartition);
    if (selectedResource == null) {
      throw new Exception("Cannot get data region replica set for empty time partition.");
    }
    IDeviceID deviceID = selectedResource.getDevices().iterator().next();
    List<TTimePartitionSlot> slotList =
        Collections.singletonList(
            TimePartitionUtils.getTimePartitionSlot(
                selectedResource.getTimeIndex().getStartTime(deviceID).get()));
    String databaseName = dataRegion.getDatabaseName();

    Map<String, List<DataPartitionQueryParam>> map = new HashMap<>();
    DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
    dataPartitionQueryParam.setDatabaseName(databaseName);
    dataPartitionQueryParam.setDeviceID(deviceID);
    dataPartitionQueryParam.setTimePartitionSlotList(slotList);
    map.put(databaseName, Collections.singletonList(dataPartitionQueryParam));
    return ClusterPartitionFetcher.getInstance()
        .getDataPartition(map)
        .getDataRegionReplicaSetForWriting(deviceID, slotList, databaseName);
  }

  public static List<TsFileResource> pullRemoteReplica(
      DataRegion dataRegion, long timePartition, File targetDir) {
    List<TRegionReplicaSet> regionReplicaSet;
    try {
      regionReplicaSet = getDataRegionReplicaSet(dataRegion, timePartition);
    } catch (Exception e) {
      LOGGER.error(
          "Fail to pull remote replica for data region {} in time partition {}",
          dataRegion.getDataRegionIdString(),
          timePartition,
          e);
      return Collections.emptyList();
    }

    TDataNodeLocation leaderLocation = regionReplicaSet.get(0).getDataNodeLocations().get(0);
    TEndPoint endPoint = leaderLocation.getInternalEndPoint();
    if (isLocal(endPoint)) {
      return Collections.emptyList();
    }

    TFetchLeaderRemoteReplicaResp resp;
    try (SyncDataNodeInternalServiceClient client =
        Coordinator.getInstance().getInternalServiceClientManager().borrowClient(endPoint)) {
      resp =
          client.fetchLeaderRemoteReplica(
              new TFetchLeaderRemoteReplicaReq(
                  Integer.parseInt(dataRegion.getDataRegionIdString()), timePartition));
      if (resp == null || resp.fileNames.isEmpty()) {
        return Collections.emptyList();
      }
    } catch (Exception e) {
      LOGGER.error(TimechoServerMessages.FAIL_TO_PULL_REMOTE_REPLICA_FROM_ENDPOINT, endPoint, e);
      return Collections.emptyList();
    }

    List<TsFileResource> resources = new ArrayList<>(resp.fileNames.size());
    List<File> newFiles = new ArrayList<>();
    try {
      for (int i = 0; i < resp.fileNames.size(); ++i) {
        File tsFile = fsFactory.getFile(targetDir, resp.fileNames.get(i));
        File resourceFile =
            fsFactory.getFile(
                targetDir,
                resp.fileNames.get(i) + TsFileResource.RESOURCE_SUFFIX + REMOTE_TMP_FILE_SUFFIX);
        persist(resourceFile, resp.resourceFiles.get(i));
        newFiles.add(resourceFile);
        if (resp.modsFiles.get(i).capacity() != 0) {
          File modsFile =
              fsFactory.getFile(
                  targetDir,
                  resp.fileNames.get(i) + ModificationFile.FILE_SUFFIX + REMOTE_TMP_FILE_SUFFIX);
          persist(modsFile, resp.modsFiles.get(i));
          newFiles.add(modsFile);
        }
        resources.add(new TsFileResource(tsFile, TsFileResourceStatus.NORMAL_ON_REMOTE));
      }
    } catch (Exception e) {
      for (File newFile : newFiles) {
        try {
          fsFactory.deleteIfExists(newFile);
        } catch (IOException cleanupException) {
          e.addSuppressed(cleanupException);
        }
      }
      LOGGER.error(TimechoServerMessages.FAIL_TO_PERSIST_REMOTE_REPLICA_OF_ENDPOINT, endPoint, e);
      return Collections.emptyList();
    }
    return resources;
  }

  private static boolean isLocal(TEndPoint endPoint) {
    return IoTDBDescriptor.getInstance().getConfig().getInternalAddress().equals(endPoint.getIp())
        && IoTDBDescriptor.getInstance().getConfig().getInternalPort() == endPoint.port;
  }

  private static void persist(File file, ByteBuffer content) throws IOException {
    try (FileChannel channel =
        FileChannel.open(file.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      channel.write(content);
    }
  }

  public static void removeLocalReplica(TsFileResource resource) throws IOException {
    resource.forceMarkDeleted();
    File localFile = resource.getTsFile();
    fsFactory.deleteIfExists(
        fsFactory.getFile(localFile.getPath() + TsFileResource.RESOURCE_SUFFIX));
    fsFactory.deleteIfExists(
        fsFactory.getFile(
            localFile.getPath() + TsFileResource.RESOURCE_SUFFIX + TsFileResource.TEMP_SUFFIX));
    fsFactory.deleteIfExists(fsFactory.getFile(localFile.getPath() + ModificationFile.FILE_SUFFIX));
  }

  public static void deleteRemoteTmpFiles(File dir) throws IOException {
    File[] remoteTmpFiles =
        fsFactory.listFilesBySuffix(dir.getAbsolutePath(), REMOTE_TMP_FILE_SUFFIX);
    for (File tmpFile : remoteTmpFiles) {
      fsFactory.deleteIfExists(tmpFile);
    }
  }

  /**
   * After TsFile replica sharing, drop this follower's last-tier remote OBJECT copies ({@code .bin}
   * / {@code .back}) for the same data region and time partition whose TsFile version is at most
   * {@code maxVersion} (the partition max captured when the task was selected). In-progress {@code
   * .bin.tmp} files are skipped. Objects written after selection have a higher version and are left
   * intact. Leader objects are not touched (this node only sees its own {@code dataNodeId} prefix).
   *
   * @return {@code true} if deletion ran (or there was nothing to delete); {@code false} if objects
   *     for this partition are still on a lower tier and deletion was skipped
   */
  public static boolean deleteLocalRemoteObjects(
      String dataRegionId, long timePartition, long maxVersion) {
    try {
      int lastTier = TierManager.getInstance().getTiersNum() - 1;
      if (lastTier < 0) {
        return true;
      }
      if (hasUnmigratedObjects(dataRegionId, timePartition, maxVersion, lastTier)) {
        LOGGER.info(
            TimechoServerMessages
                .LOG_SKIP_DELETE_SHARED_OBJECT_FILES_FOR_DATA_REGION_ARG_TIME_PARTITION_UNMIGRATED_OBJECTS_REMAIN_94B6DD34,
            dataRegionId,
            timePartition);
        return false;
      }
      for (String objectRoot : TierManager.getInstance().getObjectFoldersForTier(lastTier)) {
        if (FSUtils.isLocal(objectRoot)) {
          continue;
        }
        File regionDir = fsFactory.getFile(objectRoot, dataRegionId);
        // Empty suffix lists every key under the region prefix (OS has no real directories).
        File[] files = fsFactory.listFilesBySuffix(regionDir.getPath(), "");
        if (files == null) {
          continue;
        }
        for (File file : files) {
          // OSFile.getName() is the full object key; parse the basename only.
          String name = ObjectPathNaming.baseFileName(file.getPath());
          if (!shouldDeleteLocalRemoteObject(name, timePartition, maxVersion)) {
            continue;
          }
          fsFactory.deleteIfExists(file);
        }
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          TimechoServerMessages
              .LOG_FAIL_TO_DELETE_SHARED_OBJECT_FILES_FOR_DATA_REGION_ARG_TIME_PARTITION_ARG_6A59C405,
          dataRegionId,
          timePartition,
          e);
      return false;
    }
  }

  /**
   * Returns {@code true} when any object in the share scope still lives below the last tier (TTL /
   * disk migration not finished yet).
   */
  static boolean hasUnmigratedObjects(
      String dataRegionId, long timePartition, long maxVersion, int lastTier) throws IOException {
    for (int tier = 0; tier < lastTier; tier++) {
      for (String objectRoot : TierManager.getInstance().getObjectFoldersForTier(tier)) {
        if (containsShareScopeObjects(objectRoot, dataRegionId, timePartition, maxVersion)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsShareScopeObjects(
      String objectRoot, String dataRegionId, long timePartition, long maxVersion)
      throws IOException {
    File regionDir = fsFactory.getFile(objectRoot, dataRegionId);
    if (FSUtils.isLocal(objectRoot)) {
      if (!regionDir.isDirectory()) {
        return false;
      }
      final boolean[] found = {false};
      Files.walkFileTree(
          regionDir.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
              if (ObjectDirectoryScanner.OBJECT_GC_TOMBSTONE_DIR.equals(
                  dir.getFileName().toString())) {
                return FileVisitResult.SKIP_SUBTREE;
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              if (shouldDeleteLocalRemoteObject(
                  file.getFileName().toString(), timePartition, maxVersion)) {
                found[0] = true;
                return FileVisitResult.TERMINATE;
              }
              return FileVisitResult.CONTINUE;
            }
          });
      return found[0];
    }
    File[] files = fsFactory.listFilesBySuffix(regionDir.getPath(), "");
    if (files == null) {
      return false;
    }
    for (File file : files) {
      String name = ObjectPathNaming.baseFileName(file.getPath());
      if (shouldDeleteLocalRemoteObject(name, timePartition, maxVersion)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether a last-tier object basename should be dropped by replica sharing. Objects with {@code
   * version > maxVersion} were written after the TsFile list was copied and must be kept.
   */
  static boolean shouldDeleteLocalRemoteObject(String name, long timePartition, long maxVersion) {
    if (!ObjectTypeUtils.isObjectCandidate(name)) {
      return false;
    }
    // Chunked OBJECT writes use {time}.bin.tmp; never delete in-flight temp files during share.
    if (name.endsWith(
        ObjectTypeUtils.OBJECT_FILE_SUFFIX + ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX)) {
      return false;
    }
    long timestamp = ObjectPathNaming.parseTime(name);
    if (timestamp < 0 || TimePartitionUtils.getTimePartitionId(timestamp) != timePartition) {
      return false;
    }
    return ObjectPathNaming.parseVersion(name) <= maxVersion;
  }
}
