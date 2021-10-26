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

package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.PullFileException;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.partition.slot.SlotManager;
import org.apache.iotdb.cluster.partition.slot.SlotManager.SlotStatus;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * FileSnapshot records the data files in a slot and their md5 (or other verification). When the
 * snapshot is used to perform a catch-up, the receiver should:
 *
 * <p>1. create a remote snapshot indicating that the slot is being pulled from the remote
 *
 * <p>2. traverse the file list, for each file:
 *
 * <p>2.1 if the file exists locally and the md5 is correct, skip it.
 *
 * <p>2.2 otherwise pull the file from the remote.
 *
 * <p>3. replace the remote snapshot with a FileSnapshot indicating that the slot of this node is
 * synchronized with the remote one.
 */
@SuppressWarnings("java:S1135") // ignore todos
public class FileSnapshot extends Snapshot implements TimeseriesSchemaSnapshot {

  private static final Logger logger = LoggerFactory.getLogger(FileSnapshot.class);

  public static final int PULL_FILE_RETRY_INTERVAL_MS = 5000;
  private Collection<TimeseriesSchema> timeseriesSchemas;
  private List<RemoteTsFileResource> dataFiles;

  public FileSnapshot() {
    dataFiles = new ArrayList<>();
    timeseriesSchemas = new ArrayList<>();
  }

  public void addFile(TsFileResource resource, Node header) throws IOException {
    addFile(resource, header, false);
  }

  public void addFile(TsFileResource resource, Node header, boolean isRangeUnique)
      throws IOException {
    RemoteTsFileResource remoteTsFileResource = new RemoteTsFileResource(resource, header);
    remoteTsFileResource.setPlanRangeUnique(isRangeUnique);
    dataFiles.add(remoteTsFileResource);
  }

  @Override
  public ByteBuffer serialize() {
    logger.info("Start to serialize a snapshot {}", this);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      logger.info("Start to serialize {} schemas", timeseriesSchemas.size());
      dataOutputStream.writeInt(timeseriesSchemas.size());
      for (TimeseriesSchema measurementSchema : timeseriesSchemas) {
        measurementSchema.serializeTo(dataOutputStream);
      }

      logger.info("Start to serialize {} data files", dataFiles.size());
      dataOutputStream.writeInt(dataFiles.size());
      for (RemoteTsFileResource dataFile : dataFiles) {
        dataFile.serialize(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int timeseriesNum = buffer.getInt();
    for (int i = 0; i < timeseriesNum; i++) {
      timeseriesSchemas.add(TimeseriesSchema.deserializeFrom(buffer));
    }
    int fileNum = buffer.getInt();
    for (int i = 0; i < fileNum; i++) {
      RemoteTsFileResource resource = new RemoteTsFileResource();
      resource.deserialize(buffer);
      dataFiles.add(resource);
    }
  }

  public List<RemoteTsFileResource> getDataFiles() {
    return dataFiles;
  }

  @Override
  public Collection<TimeseriesSchema> getTimeseriesSchemas() {
    return timeseriesSchemas;
  }

  @Override
  public void setTimeseriesSchemas(Collection<TimeseriesSchema> timeseriesSchemas) {
    this.timeseriesSchemas = timeseriesSchemas;
  }

  @Override
  public SnapshotInstaller<FileSnapshot> getDefaultInstaller(RaftMember member) {
    return new Installer((DataGroupMember) member);
  }

  @Override
  public String toString() {
    return String.format(
        "FileSnapshot{%d files, %d series, index-term: %d-%d}",
        dataFiles.size(), timeseriesSchemas.size(), lastLogIndex, lastLogTerm);
  }

  public static class Installer implements SnapshotInstaller<FileSnapshot> {

    /**
     * When a DataGroupMember pulls data from another node, the data files will be firstly stored in
     * the "REMOTE_FILE_TEMP_DIR", and then load file functionality of IoTDB will be used to load
     * the files into the IoTDB instance.
     */
    private static final String REMOTE_FILE_TEMP_DIR =
        IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "remote";

    private static final Logger logger = LoggerFactory.getLogger(Installer.class);
    private DataGroupMember dataGroupMember;
    private SlotManager slotManager;
    private String name;

    Installer(DataGroupMember dataGroupMember) {
      this.dataGroupMember = dataGroupMember;
      this.slotManager = dataGroupMember.getSlotManager();
      this.name = dataGroupMember.getName();
    }

    @Override
    public void install(FileSnapshot snapshot, int slot, boolean isDataMigration)
        throws SnapshotInstallationException {
      try {
        logger.info("Starting to install a snapshot {} into slot[{}]", snapshot, slot);
        installFileSnapshotSchema(snapshot);
        logger.info("Schemas in snapshot are registered");
        if (isDataMigration) {
          SlotStatus status = slotManager.getStatus(slot);
          if (status == SlotStatus.PULLING) {
            // as the schemas are set, writes can proceed
            slotManager.setToPullingWritable(slot);
            logger.debug("{}: slot {} is now pulling writable", name, slot);
          }
        }
        installFileSnapshotFiles(snapshot, slot, isDataMigration);
      } catch (PullFileException e) {
        throw new SnapshotInstallationException(e);
      }
    }

    @Override
    public void install(Map<Integer, FileSnapshot> snapshotMap, boolean isDataMigration)
        throws SnapshotInstallationException {
      logger.info("Starting to install snapshots {}", snapshotMap);
      installSnapshot(snapshotMap, isDataMigration);
    }

    private void installSnapshot(Map<Integer, FileSnapshot> snapshotMap, boolean isDataMigration)
        throws SnapshotInstallationException {
      // In data migration, meta group member other than new node does not need to synchronize the
      // leader, because data migration must be carried out after meta group applied add/remove node
      // log.
      dataGroupMember
          .getMetaGroupMember()
          .syncLocalApply(
              dataGroupMember.getMetaGroupMember().getPartitionTable().getLastMetaLogIndex() - 1,
              false);
      for (Entry<Integer, FileSnapshot> integerSnapshotEntry : snapshotMap.entrySet()) {
        Integer slot = integerSnapshotEntry.getKey();
        FileSnapshot snapshot = integerSnapshotEntry.getValue();
        installFileSnapshotSchema(snapshot);
        if (isDataMigration) {
          SlotStatus status = slotManager.getStatus(slot);
          if (status == SlotStatus.PULLING) {
            // as schemas are set, writes can proceed
            slotManager.setToPullingWritable(slot, false);
            logger.debug("{}: slot {} is now pulling writable", name, slot);
          }
        }
      }
      if (isDataMigration) {
        slotManager.save();
      }

      for (Entry<Integer, FileSnapshot> integerSnapshotEntry : snapshotMap.entrySet()) {
        Integer slot = integerSnapshotEntry.getKey();
        FileSnapshot snapshot = integerSnapshotEntry.getValue();
        try {
          installFileSnapshotFiles(snapshot, slot, isDataMigration);
        } catch (PullFileException e) {
          throw new SnapshotInstallationException(e);
        }
      }
      slotManager.save();
    }

    private void installFileSnapshotSchema(FileSnapshot snapshot) {
      // load metadata in the snapshot
      for (TimeseriesSchema schema : snapshot.getTimeseriesSchemas()) {
        // notice: the measurement in the schema is the full path here
        SchemaUtils.registerTimeseries(schema);
      }
    }

    private void installFileSnapshotFiles(FileSnapshot snapshot, int slot, boolean isDataMigration)
        throws PullFileException {
      List<RemoteTsFileResource> remoteTsFileResources = snapshot.getDataFiles();
      // pull file
      for (int i = 0, remoteTsFileResourcesSize = remoteTsFileResources.size();
          i < remoteTsFileResourcesSize;
          i++) {
        RemoteTsFileResource resource = remoteTsFileResources.get(i);
        logger.info(
            "Pulling {}/{} files, current: {}", i + 1, remoteTsFileResources.size(), resource);
        try {
          if (isDataMigration) {
            // This means that the minimum plan index and maximum plan index of some files are the
            // same,
            // so the logic of judging index coincidence needs to remove the case of equal
            resource.setMinPlanIndex(dataGroupMember.getLogManager().getLastLogIndex());
            resource.setMaxPlanIndex(dataGroupMember.getLogManager().getLastLogIndex());
            loadRemoteFile(resource);
          } else {
            if (!isFileAlreadyPulled(resource)) {
              loadRemoteFile(resource);
            } else {
              // notify the snapshot provider to remove the hardlink
              removeRemoteHardLink(resource);
            }
          }
        } catch (IllegalPathException e) {
          throw new PullFileException(resource.getTsFilePath(), resource.getSource(), e);
        }
      }

      // all files are loaded, the slot can be queried without accessing the previous holder
      slotManager.setToNull(slot, !isDataMigration);
      logger.info("{}: slot {} is ready", name, slot);
    }

    /**
     * Check if the file "resource" is a duplication of some local files. As all data file close is
     * controlled by the data group leader, the files with the same version should contain identical
     * data if without merge. Even with merge, the files that the merged file is from are recorded
     * so we can still find out if the data of a file is already replicated in this member.
     *
     * @param resource
     * @return
     */
    private boolean isFileAlreadyPulled(RemoteTsFileResource resource) throws IllegalPathException {
      Pair<String, Long> sgNameAndTimePartitionIdPair =
          FilePathUtils.getLogicalSgNameAndTimePartitionIdPair(
              resource.getTsFile().getAbsolutePath());
      return StorageEngine.getInstance()
          .isFileAlreadyExist(
              resource,
              new PartialPath(sgNameAndTimePartitionIdPair.left),
              sgNameAndTimePartitionIdPair.right);
    }

    private void removeRemoteHardLink(RemoteTsFileResource resource) {
      Node sourceNode = resource.getSource();
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        AsyncDataClient client = (AsyncDataClient) dataGroupMember.getAsyncClient(sourceNode);
        if (client != null) {
          try {
            client.removeHardLink(
                resource.getTsFile().getAbsolutePath(), new GenericHandler<>(sourceNode, null));
          } catch (TException e) {
            logger.error(
                "Cannot remove hardlink {} from {}",
                resource.getTsFile().getAbsolutePath(),
                sourceNode);
          }
        }
      } else {
        SyncDataClient client = (SyncDataClient) dataGroupMember.getSyncClient(sourceNode);
        if (client == null) {
          logger.error(
              "Cannot remove hardlink {} from {}, due to can not get client",
              resource.getTsFile().getAbsolutePath(),
              sourceNode);
          return;
        }
        try {
          client.removeHardLink(resource.getTsFile().getAbsolutePath());
        } catch (TException te) {
          client.getInputProtocol().getTransport().close();
          logger.error(
              "Cannot remove hardlink {} from {}",
              resource.getTsFile().getAbsolutePath(),
              sourceNode);
        } finally {
          ClientUtils.putBackSyncClient(client);
        }
      }
    }

    /**
     * Load a remote file from the header of the data group that the file is in. As different IoTDB
     * instances will name the file with the same version differently, we can only pull the file
     * from the header currently.
     *
     * @param resource
     */
    private void loadRemoteFile(RemoteTsFileResource resource) throws PullFileException {
      Node sourceNode = resource.getSource();
      // pull the file to a temporary directory
      File tempFile;
      try {
        tempFile = pullRemoteFile(resource, sourceNode);
      } catch (IOException e) {
        throw new PullFileException(resource.toString(), sourceNode, e);
      }
      if (tempFile != null) {
        resource.setFile(tempFile);
        try {
          // save the resource and load the file into IoTDB
          resource.serialize();
          loadRemoteResource(resource);
          logger.info("{}: Remote file {} is successfully loaded", name, resource);
          return;
        } catch (IOException e) {
          logger.error("{}: Cannot serialize {}", name, resource, e);
        } catch (IllegalPathException e) {
          logger.error("Illegal path when loading file {}", resource, e);
        }
      }
      logger.error("{}: Cannot load remote file {} from node {}", name, resource, sourceNode);
      throw new PullFileException(resource.toString(), sourceNode);
    }

    /**
     * When a file is successfully pulled to the local storage, load it into IoTDB with the resource
     * and remove the files that is a subset of the new file. Also change the modification file if
     * the new file is with one.
     *
     * @param resource
     */
    private void loadRemoteResource(RemoteTsFileResource resource) throws IllegalPathException {
      // the new file is stored at:
      // remote/<nodeIdentifier>/<FilePathUtils.getTsFilePrefixPath(resource)>/<tsfile>
      // you can see FilePathUtils.splitTsFilePath() method for details.
      PartialPath storageGroupName =
          new PartialPath(
              FilePathUtils.getLogicalStorageGroupName(resource.getTsFile().getAbsolutePath()));
      try {
        StorageEngine.getInstance().getProcessor(storageGroupName).loadNewTsFile(resource);
        if (resource.isPlanRangeUnique()) {
          // only when a file has a unique range can we remove other files that over lap with it,
          // otherwise we may remove data that is not contained in the file
          StorageEngine.getInstance()
              .getProcessor(storageGroupName)
              .removeFullyOverlapFiles(resource);
        }
      } catch (StorageEngineException | LoadFileException e) {
        logger.error("{}: Cannot load remote file {} into storage group", name, resource, e);
        return;
      }
      resource.setRemote(false);
    }

    /**
     * Download the remote file of "resource" from "node" to a local temporary directory. If the
     * resource has modification file, also download it.
     *
     * @param resource the TsFile to be downloaded
     * @param node where to download the file
     * @return the downloaded file or null if the file cannot be downloaded or its MD5 is not right
     * @throws IOException
     */
    private File pullRemoteFile(RemoteTsFileResource resource, Node node) throws IOException {
      logger.info(
          "{}: pulling remote file {} from {}, plan index [{}, {}]",
          name,
          resource,
          node,
          resource.getMinPlanIndex(),
          resource.getMaxPlanIndex());
      // the new file is stored at:
      // remote/<nodeIdentifier>/<FilePathUtils.getTsFilePrefixPath(resource)>/<newTsFile>
      // you can see FilePathUtils.splitTsFilePath() method for details.
      String tempFileName =
          FilePathUtils.getTsFileNameWithoutHardLink(resource.getTsFile().getAbsolutePath());
      String tempFilePath =
          node.getNodeIdentifier()
              + File.separator
              + FilePathUtils.getTsFilePrefixPath(resource.getTsFile().getAbsolutePath())
              + File.separator
              + tempFileName;
      File tempFile = new File(REMOTE_FILE_TEMP_DIR, tempFilePath);
      tempFile.getParentFile().mkdirs();
      if (pullRemoteFile(resource.getTsFile().getAbsolutePath(), node, tempFile)) {
        // TODO-Cluster#353: implement file examination, may be replaced with other algorithm
        if (resource.isWithModification()) {
          File tempModFile =
              new File(REMOTE_FILE_TEMP_DIR, tempFilePath + ModificationFile.FILE_SUFFIX);
          pullRemoteFile(resource.getModFile().getFilePath(), node, tempModFile);
        }
        return tempFile;
      }
      return null;
    }

    /**
     * Download the file "remotePath" from "node" and store it to "dest" using up to 64KB chunks. If
     * the network is bad, this method will retry upto 5 times before returning a failure.
     *
     * @param remotePath the file to be downloaded
     * @param node where to download the file
     * @param dest where to store the file
     * @return true if the file is successfully downloaded, false otherwise
     * @throws IOException
     */
    private boolean pullRemoteFile(String remotePath, Node node, File dest) throws IOException {
      int pullFileRetry = 5;
      for (int i = 0; i < pullFileRetry; i++) {
        try (BufferedOutputStream bufferedOutputStream =
            new BufferedOutputStream(new FileOutputStream(dest))) {
          if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
            downloadFileAsync(node, remotePath, bufferedOutputStream);
          } else {
            downloadFileSync(node, remotePath, bufferedOutputStream);
          }

          if (logger.isInfoEnabled()) {
            logger.info(
                "{}: remote file {} is pulled at {}, length: {}",
                name,
                remotePath,
                dest,
                dest.length());
          }
          return true;
        } catch (TException e) {
          logger.warn(
              "{}: Cannot pull file {} from {}, wait 5s to retry", name, remotePath, node, e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("{}: Pulling file {} from {} interrupted", name, remotePath, node, e);
          return false;
        }

        try {
          Files.delete(dest.toPath());
          Thread.sleep(PULL_FILE_RETRY_INTERVAL_MS);
        } catch (IOException e) {
          logger.warn("Cannot delete file when pulling {} from {} failed", remotePath, node);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          logger.warn("{}: Pulling file {} from {} interrupted", name, remotePath, node, ex);
          return false;
        }
        // next try
      }
      return false;
    }

    private void downloadFileAsync(Node node, String remotePath, OutputStream dest)
        throws IOException, TException, InterruptedException {
      long offset = 0;
      // TODO-Cluster: use elaborate downloading techniques
      int fetchSize = 64 * 1024;

      while (true) {
        AsyncDataClient client = (AsyncDataClient) dataGroupMember.getAsyncClient(node);
        if (client == null) {
          throw new IOException("No available client for " + node.toString());
        }
        ByteBuffer buffer = SyncClientAdaptor.readFile(client, remotePath, offset, fetchSize);
        int len = writeBuffer(buffer, dest);
        if (len == 0) {
          break;
        }
        offset += len;
      }
      dest.flush();
    }

    private int writeBuffer(ByteBuffer buffer, OutputStream dest) throws IOException {
      if (buffer == null || buffer.limit() - buffer.position() == 0) {
        return 0;
      }

      // notice: the buffer returned by thrift is a slice of a larger buffer which contains
      // the whole response, so buffer.position() is not 0 initially and buffer.limit() is
      // not the size of the downloaded chunk
      dest.write(
          buffer.array(),
          buffer.position() + buffer.arrayOffset(),
          buffer.limit() - buffer.position());
      return buffer.limit() - buffer.position();
    }

    private void downloadFileSync(Node node, String remotePath, OutputStream dest)
        throws IOException {
      SyncDataClient client = (SyncDataClient) dataGroupMember.getSyncClient(node);
      if (client == null) {
        throw new IOException("No available client for " + node.toString());
      }

      long offset = 0;
      // TODO-Cluster: use elaborate downloading techniques
      int fetchSize = 64 * 1024;

      try {
        while (true) {
          ByteBuffer buffer = client.readFile(remotePath, offset, fetchSize);
          int len = writeBuffer(buffer, dest);
          if (len == 0) {
            break;
          }
          offset += len;
        }
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
      dest.flush();
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
    FileSnapshot snapshot = (FileSnapshot) o;
    return Objects.equals(timeseriesSchemas, snapshot.timeseriesSchemas)
        && Objects.equals(dataFiles, snapshot.dataFiles);
  }

  @Override
  public void truncateBefore(long minIndex) {
    dataFiles.removeIf(
        res -> {
          boolean toBeTruncated = res.getMaxPlanIndex() < minIndex;
          if (toBeTruncated) {
            // also remove the hardlink
            res.remove();
          }
          return toBeTruncated;
        });
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeseriesSchemas, dataFiles);
  }

  public static class Factory implements SnapshotFactory<FileSnapshot> {

    public static final Factory INSTANCE = new Factory();

    @Override
    public FileSnapshot create() {
      return new FileSnapshot();
    }

    @Override
    public FileSnapshot copy(FileSnapshot origin) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      fileSnapshot.setLastLogIndex(origin.lastLogIndex);
      fileSnapshot.setLastLogTerm(origin.lastLogTerm);
      fileSnapshot.dataFiles = origin.dataFiles == null ? null : new ArrayList<>(origin.dataFiles);
      fileSnapshot.timeseriesSchemas =
          origin.timeseriesSchemas == null ? null : new ArrayList<>(origin.timeseriesSchemas);
      return fileSnapshot;
    }
  }
}
