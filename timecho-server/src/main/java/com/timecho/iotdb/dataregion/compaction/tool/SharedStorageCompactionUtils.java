package com.timecho.iotdb.dataregion.compaction.tool;

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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.mpp.rpc.thrift.TFetchIoTConsensusProgressReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchIoTConsensusProgressResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchLeaderRemoteReplicaResp;

import com.timecho.iotdb.i18n.TimechoServerMessages;
import org.apache.tsfile.file.metadata.IDeviceID;
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
      LOGGER.error(TimechoServerMessages.FAIL_TO_PERSIST_REMOTE_REPLICA_OF_ENDPOINT, endPoint, e);
      for (File newFile : newFiles) {
        newFile.delete();
      }
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
}
