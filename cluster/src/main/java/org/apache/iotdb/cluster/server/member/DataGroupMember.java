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

package org.apache.iotdb.cluster.server.member;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.client.async.ClientPool;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.PullFileException;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.exception.SnapshotApplicationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.manage.FilePartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTask;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTaskDescriptor;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.SlotManager;
import org.apache.iotdb.cluster.partition.SlotManager.SlotStatus;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.filter.SlotTsFileFilter;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.NodeReport.DataMemberReport;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.GenericForwardHandler;
import org.apache.iotdb.cluster.server.heartbeat.DataHeartbeatThread;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataPointReader;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGroupMember extends RaftMember implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataGroupMember.class);
  /**
   * When a DataGroupMember pulls data from another node, the data files will be firstly stored in
   * the "REMOTE_FILE_TEMP_DIR", and then load file functionality of IoTDB will be used to load the
   * files into the IoTDB instance.
   */
  private static final String REMOTE_FILE_TEMP_DIR =
      IoTDBDescriptor.getInstance().getConfig().getBaseDir() + File.separator + "remote";

  /**
   * The MetaGroupMember that in charge of the DataGroupMember. Mainly for providing partition table
   * and MetaLogManager.
   */
  private MetaGroupMember metaGroupMember;

  /**
   * The thread pool that runs the pull snapshot tasks. Pool size is the # of CPU cores.
   */
  private ExecutorService pullSnapshotService;


  /**
   * "queryManger" records the remote nodes which have queried this node, and the readers or
   * executors this member has created for those queries. When the queries end, an EndQueryRequest
   * will be sent to this member and related resources will be released.
   */
  private ClusterQueryManager queryManager;

  /**
   * "slotManager" tracks the status of slots during data transfers so that we can know whether the
   * slot has non-pulled data.
   */
  protected SlotManager slotManager;

  @TestOnly
  public DataGroupMember() {
    // constructor for test
  }

  DataGroupMember(TProtocolFactory factory, PartitionGroup nodes, Node thisNode,
      MetaGroupMember metaGroupMember) {
    super("Data(" + nodes.getHeader().getIp() + ":" + nodes.getHeader().getMetaPort() + ")",
        new ClientPool(new DataClient.Factory(factory)));
    this.thisNode = thisNode;
    this.metaGroupMember = metaGroupMember;
    allNodes = nodes;
    setQueryManager(new ClusterQueryManager());
    slotManager = new SlotManager(ClusterConstant.SLOT_NUM);
    logManager = new FilePartitionedSnapshotLogManager(new DataLogApplier(metaGroupMember,
        this), metaGroupMember.getPartitionTable(), allNodes.get(0), thisNode);
    initPeerMap();
    term.set(logManager.getHardState().getCurrentTerm());
    voteFor = logManager.getHardState().getVoteFor();
  }

  /**
   * Start heartbeat, catch-up, pull snapshot services and start all unfinished pull-snapshot-tasks.
   * Calling the method twice does not induce side effects.
   *
   * @throws TTransportException
   */
  @Override
  public void start() {
    if (heartBeatService != null) {
      return;
    }
    super.start();
    heartBeatService.submit(new DataHeartbeatThread(this));
    pullSnapshotService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    resumePullSnapshotTasks();
  }

  /**
   * Stop heartbeat, catch-up and pull snapshot services and release all query resources. Calling
   * the method twice does not induce side effects.
   */
  @Override
  public void stop() {
    super.stop();
    if (pullSnapshotService != null) {
      pullSnapshotService.shutdownNow();
      try {
        pullSnapshotService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for pullSnapshotService to end", e);
      }
      pullSnapshotService = null;
    }

    try {
      getQueryManager().endAllQueries();
    } catch (StorageEngineException e) {
      logger.error("Cannot release queries of {}", name, e);
    }
  }

  /**
   * The first node (on the hash ring) in this data group is the header. It determines the duty
   * (what range on the ring do the group take responsibility for) of the group and although other
   * nodes in this may change, this node is unchangeable unless the data group is dismissed. It is
   * also the identifier of this data group.
   */
  @Override
  public Node getHeader() {
    return allNodes.get(0);
  }

  public ClusterQueryManager getQueryManager() {
    return queryManager;
  }

  public void setQueryManager(ClusterQueryManager queryManager) {
    this.queryManager = queryManager;
  }

  public static class Factory {

    private TProtocolFactory protocolFactory;
    private MetaGroupMember metaGroupMember;

    Factory(TProtocolFactory protocolFactory, MetaGroupMember metaGroupMember) {
      this.protocolFactory = protocolFactory;
      this.metaGroupMember = metaGroupMember;
    }

    public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
      return new DataGroupMember(protocolFactory, partitionGroup, thisNode, metaGroupMember);
    }
  }

  /**
   * Try to add a Node into the group to which the member belongs.
   *
   * @param node
   * @return true if this node should leave the group because of the addition of the node, false
   * otherwise
   */
  public synchronized boolean addNode(Node node) {
    // when a new node is added, start an election instantly to avoid the stale leader still
    // taking the leadership, which guarantees the valid leader will not have the stale
    // partition table
    synchronized (term) {
      term.incrementAndGet();
      setLeader(null);
      setVoteFor(thisNode);
      updateHardState(term.get(), getVoteFor());
      setLastHeartbeatReceivedTime(System.currentTimeMillis());
      setCharacter(NodeCharacter.ELECTOR);
    }
    synchronized (allNodes) {
      int insertIndex = -1;
      // find the position to insert the new node, the nodes are ordered by their identifiers
      for (int i = 0; i < allNodes.size() - 1; i++) {
        Node prev = allNodes.get(i);
        Node next = allNodes.get(i + 1);
        if (prev.nodeIdentifier < node.nodeIdentifier && node.nodeIdentifier < next.nodeIdentifier
            || prev.nodeIdentifier < node.nodeIdentifier
            && next.nodeIdentifier < prev.nodeIdentifier
            || node.nodeIdentifier < next.nodeIdentifier
            && next.nodeIdentifier < prev.nodeIdentifier
        ) {
          insertIndex = i + 1;
          break;
        }
      }
      if (insertIndex > 0) {
        allNodes.add(insertIndex, node);
        peerMap.putIfAbsent(node, new Peer(logManager.getLastLogIndex()));
        // remove the last node because the group size is fixed to replication number
        Node removedNode = allNodes.remove(allNodes.size() - 1);
        peerMap.remove(removedNode);
        // if the local node is the last node and the insertion succeeds, this node should leave
        // the group
        logger.debug("{}: Node {} is inserted into the data group {}", name, node, allNodes);
        return removedNode.equals(thisNode);
      }
      return false;
    }
  }

  /**
   * Process the election request from another node in the group. To win the vote from the local
   * member, a node must have both meta and data logs no older than then local member, or it will be
   * turned down.
   *
   * @param electionRequest
   * @return Response.RESPONSE_META_LOG_STALE if the meta logs of the elector fall behind
   * Response.RESPONSE_LOG_MISMATCH if the data logs of the elector fall behind Response.SUCCESS if
   * the vote is given to the elector the term of local member if the elector's term is no bigger
   * than the local member
   */
  @Override
  long processElectionRequest(ElectionRequest electionRequest) {
    // to be a data group leader, a node should also be qualified to be the meta group leader
    // which guarantees the data group leader has the newest partition table.
    long thatTerm = electionRequest.getTerm();
    long thatMetaLastLogIndex = electionRequest.getLastLogIndex();
    long thatMetaLastLogTerm = electionRequest.getLastLogTerm();
    long thatDataLastLogIndex = electionRequest.getDataLogLastIndex();
    long thatDataLastLogTerm = electionRequest.getDataLogLastTerm();
    logger.info(
        "{} received an dataGroup election request, term:{}, metaLastLogIndex:{}, metaLastLogTerm:{}, dataLastLogIndex:{}, dataLastLogTerm:{}",
        name, thatTerm, thatMetaLastLogIndex, thatMetaLastLogTerm, thatDataLastLogIndex,
        thatDataLastLogTerm);

    // check meta logs
    // term of the electors's MetaGroupMember is not verified, so 0 and 1 are used to make sure
    // the verification does not fail
    long metaResponse = metaGroupMember.verifyElector(thatMetaLastLogIndex, thatMetaLastLogTerm);
    if (metaResponse == Response.RESPONSE_LOG_MISMATCH) {
      return Response.RESPONSE_META_LOG_STALE;
    }

    long resp = verifyElector(thatDataLastLogIndex, thatDataLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      logger.info(
          "{} accepted an dataGroup election request, term:{}/{}, dataLogIndex:{}/{}, dataLogTerm:{}/{}, metaLogIndex:{}/{},metaLogTerm:{}/{}",
          name, thatTerm, term.get(), thatDataLastLogIndex, logManager.getLastLogIndex(),
          thatDataLastLogTerm,
          logManager.getLastLogTerm(), thatMetaLastLogIndex,
          metaGroupMember.getLogManager().getLastLogIndex(), thatMetaLastLogTerm,
          metaGroupMember.getLogManager().getLastLogTerm());
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartbeatReceivedTime = System.currentTimeMillis();
      setVoteFor(electionRequest.getElector());
      updateHardState(thatTerm, getVoteFor());
    } else {
      logger.info(
          "{} rejected an dataGroup election request, term:{}/{}, dataLogIndex:{}/{}, dataLogTerm:{}/{}, metaLogIndex:{}/{},metaLogTerm:{}/{}",
          name, thatTerm, term.get(), thatDataLastLogIndex, logManager.getLastLogIndex(),
          thatDataLastLogTerm,
          logManager.getLastLogTerm(), thatMetaLastLogIndex,
          metaGroupMember.getLogManager().getLastLogIndex(), thatMetaLastLogTerm,
          metaGroupMember.getLogManager().getLastLogTerm());
    }
    return resp;
  }

  /**
   * Deserialize and apply a snapshot sent by the leader. The type of the snapshot must be currently
   * PartitionedSnapshot with FileSnapshot inside.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    logger.debug("{}: received a snapshot", name);
    PartitionedSnapshot snapshot = new PartitionedSnapshot<>(FileSnapshot::new);
    try {
      snapshot.deserialize(ByteBuffer.wrap(request.getSnapshotBytes()));
      logger.debug("{} received a snapshot {}", name, snapshot);
      applyPartitionedSnapshot(snapshot);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Apply FileSnapshots, which consist of MeasurementSchemas and RemoteTsFileResources. The
   * timeseries in the MeasurementSchemas will be registered and the files in the
   * "RemoteTsFileResources" will be loaded into the IoTDB instance if they do not totally overlap
   * with existing files.
   *
   * @param snapshotMap
   */
  public void applySnapshot(Map<Integer, Snapshot> snapshotMap)
      throws SnapshotApplicationException {
    for (Snapshot value : snapshotMap.values()) {
      if (value instanceof FileSnapshot) {
        FileSnapshot fileSnapshot = (FileSnapshot) value;
        applyFileSnapshotSchema(fileSnapshot);
      }
    }

    for (Entry<Integer, Snapshot> integerSnapshotEntry : snapshotMap.entrySet()) {
      Integer slot = integerSnapshotEntry.getKey();
      Snapshot snapshot = integerSnapshotEntry.getValue();
      if (snapshot instanceof FileSnapshot) {
        applyFileSnapshotVersions((FileSnapshot) snapshot, slot);
      }
    }

    for (Entry<Integer, Snapshot> integerSnapshotEntry : snapshotMap.entrySet()) {
      Integer slot = integerSnapshotEntry.getKey();
      Snapshot snapshot = integerSnapshotEntry.getValue();
      if (snapshot instanceof FileSnapshot) {
        try {
          applyFileSnapshotFiles((FileSnapshot) snapshot, slot);
        } catch (PullFileException e) {
          throw new SnapshotApplicationException(e);
        }
      }
    }
  }

  /**
   * Apply a snapshot to the state machine, i.e., load the data and meta data contained in the
   * snapshot into the IoTDB instance. Currently the type of the snapshot should be ony
   * FileSnapshot, but more types may be supported in the future.
   *
   * @param snapshot
   */
  public void applySnapshot(Snapshot snapshot, int slot) throws SnapshotApplicationException {
    logger.debug("{}: applying snapshot {}", name, snapshot);
    metaGroupMember.syncLeader();
    if (snapshot instanceof FileSnapshot) {
      try {
        applyFileSnapshot((FileSnapshot) snapshot, slot);
      } catch (PullFileException e) {
        throw new SnapshotApplicationException(e);
      }
    } else {
      logger.error("Unrecognized snapshot {}, ignored", snapshot);
    }
  }

  private void applyFileSnapshotSchema(FileSnapshot snapshot) {
    // load metadata in the snapshot
    for (TimeseriesSchema schema : snapshot.getTimeseriesSchemas()) {
      // notice: the measurement in the schema is the full path here
      SchemaUtils.registerTimeseries(schema);
    }
  }

  private void applyFileSnapshotVersions(FileSnapshot snapshot, int slot)
      throws SnapshotApplicationException {
    // load data in the snapshot
    List<RemoteTsFileResource> remoteTsFileResources = snapshot.getDataFiles();
    // set partition versions
    for (RemoteTsFileResource remoteTsFileResource : remoteTsFileResources) {
      String[] pathSegments = FilePathUtils.splitTsFilePath(remoteTsFileResource);
      int segSize = pathSegments.length;
      String storageGroupName = pathSegments[segSize - 3];
      try {
        try {
          // the storage group may not exists because the meta member is not synchronized
          MManager.getInstance().setStorageGroup(storageGroupName);
        } catch (MetadataException e) {
          // ignore
        }
        StorageEngine.getInstance().setPartitionVersionToMax(storageGroupName,
            remoteTsFileResource.getTimePartition(), remoteTsFileResource.getMaxVersion());
      } catch (StorageEngineException e) {
        throw new SnapshotApplicationException(e);
      }
    }
    SlotStatus status = slotManager.getStatus(slot);
    if (status == SlotStatus.PULLING) {
      // as the partition versions are set, writes can proceed without generating incorrect
      // versions
      slotManager.setToPullingWritable(slot);
    }
  }

  private void applyFileSnapshotFiles(FileSnapshot snapshot, int slot)
      throws PullFileException {
    List<RemoteTsFileResource> remoteTsFileResources = snapshot.getDataFiles();
    // pull file
    for (RemoteTsFileResource resource : remoteTsFileResources) {
      if (!isFileAlreadyPulled(resource)) {
        loadRemoteFile(resource);
      }
    }
    // all files are loaded, the slot can be queried without accessing the previous holder
    slotManager.setToNull(slot);
  }

  private void applyFileSnapshot(FileSnapshot snapshot, int slot)
      throws PullFileException, SnapshotApplicationException {
    applyFileSnapshotSchema(snapshot);
    applyFileSnapshotVersions(snapshot, slot);
    applyFileSnapshotFiles(snapshot, slot);
  }

  /**
   * Check if the file "resource" is a duplication of some local files. As all data file close is
   * controlled by the data group leader, the files with the same version should contain identical
   * data if without merge. Even with merge, the files that the merged file is from are recorded so
   * we can still find out if the data of a file is already replicated in this member.
   *
   * @param resource
   * @return
   */
  private boolean isFileAlreadyPulled(RemoteTsFileResource resource) {
    String[] pathSegments = FilePathUtils.splitTsFilePath(resource);
    int segSize = pathSegments.length;
    // {storageGroupName}/{partitionNum}/{fileName}
    String storageGroupName = pathSegments[segSize - 3];
    long partitionNumber = Long.parseLong(pathSegments[segSize - 2]);
    return StorageEngine.getInstance()
        .isFileAlreadyExist(resource, storageGroupName, partitionNumber);
  }

  /**
   * Apply a PartitionedSnapshot, which is a slotNumber -> FileSnapshot map. Only the slots that are
   * managed by the the group will be applied. The lastLogId and lastLogTerm are also updated
   * according to the snapshot.
   *
   * @param snapshot
   */
  private void applyPartitionedSnapshot(PartitionedSnapshot snapshot)
      throws SnapshotApplicationException {
    synchronized (logManager) {
      List<Integer> slots = metaGroupMember.getPartitionTable().getNodeSlots(getHeader());
      for (Integer slot : slots) {
        Snapshot subSnapshot = snapshot.getSnapshot(slot);
        if (subSnapshot != null) {
          applySnapshot(subSnapshot, slot);
        }
      }
      logManager.applyingSnapshot(snapshot);
    }
  }

  /**
   * Load a remote file from the header of the data group that the file is in. As different IoTDB
   * instances will name the file with the same version differently, we can only pull the file from
   * the header currently.
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
      }
    }
    logger.error("{}: Cannot load remote file {} from node {}", name, resource, sourceNode);
    throw new PullFileException(resource.toString(), sourceNode);
  }

  /**
   * When a file is successfully pulled to the local storage, load it into IoTDB with the resource
   * and remove the files that is a subset of the new file. Also change the modification file if the
   * new file is with one.
   *
   * @param resource
   */
  private void loadRemoteResource(RemoteTsFileResource resource) {
    // the new file is stored at:
    // remote/{nodeIdentifier}/{storageGroupName}/{partitionNum}/{fileName}
    String[] pathSegments = FilePathUtils.splitTsFilePath(resource);
    int segSize = pathSegments.length;
    String storageGroupName = pathSegments[segSize - 3];
    File remoteModFile =
        new File(resource.getFile().getAbsoluteFile() + ModificationFile.FILE_SUFFIX);
    try {
      StorageEngine.getInstance().getProcessor(storageGroupName).loadNewTsFile(resource);
      StorageEngine.getInstance().getProcessor(storageGroupName)
          .removeFullyOverlapFiles(resource);
    } catch (StorageEngineException | LoadFileException e) {
      logger.error("{}: Cannot load remote file {} into storage group", name, resource, e);
      return;
    }
    if (remoteModFile.exists()) {
      // when successfully loaded, the filepath of the resource will be changed to the IoTDB data
      // dir, so we can add a suffix to find the old modification file.
      File localModFile =
          new File(resource.getFile().getAbsoluteFile() + ModificationFile.FILE_SUFFIX);
      localModFile.delete();
      remoteModFile.renameTo(localModFile);
    }
    resource.setRemote(false);
  }

  /**
   * Download the remote file of "resource" from "node" to a local temporary directory. If the
   * resource has modification file, also download it.
   *
   * @param resource the TsFile to be downloaded
   * @param node     where to download the file
   * @return the downloaded file or null if the file cannot be downloaded or its MD5 is not right
   * @throws IOException
   */
  private File pullRemoteFile(RemoteTsFileResource resource, Node node) throws IOException {
    logger.debug("{}: pulling remote file {} from {}", name, resource, node);

    String[] pathSegments = FilePathUtils.splitTsFilePath(resource);
    int segSize = pathSegments.length;
    // the new file is stored at:
    // remote/{nodeIdentifier}/{storageGroupName}/{partitionNum}/{fileName}
    // the file in the snapshot is a hardlink, remove the hardlink suffix
    String tempFileName = pathSegments[segSize - 1].substring(0,
        pathSegments[segSize - 1].lastIndexOf('.'));
    String tempFilePath =
        node.getNodeIdentifier() + File.separator + pathSegments[segSize - 3] +
            File.separator + pathSegments[segSize - 2] + File.separator + tempFileName;
    File tempFile = new File(REMOTE_FILE_TEMP_DIR, tempFilePath);
    tempFile.getParentFile().mkdirs();
    File tempModFile = new File(REMOTE_FILE_TEMP_DIR,
        tempFilePath + ModificationFile.FILE_SUFFIX);
    if (pullRemoteFile(resource.getFile().getAbsolutePath(), node, tempFile)) {
      if (!checkMd5(tempFile, resource.getMd5())) {
        logger.error("The downloaded file of {} does not have the right MD5", resource);
        tempFile.delete();
        return null;
      }
      if (resource.isWithModification()) {
        pullRemoteFile(resource.getModFile().getFilePath(), node, tempModFile);
      }
      return tempFile;
    }
    return null;
  }

  private boolean checkMd5(File tempFile, byte[] expectedMd5) {
    // TODO-Cluster#353: implement, may be replaced with other algorithm
    return true;
  }

  /**
   * Download the file "remotePath" from "node" and store it to "dest" using up to 64KB chunks. If
   * the network is bad, this method will retry upto 5 times before returning a failure.
   *
   * @param remotePath the file to be downloaded
   * @param node       where to download the file
   * @param dest       where to store the file
   * @return true if the file is successfully downloaded, false otherwise
   * @throws IOException
   */
  private boolean pullRemoteFile(String remotePath, Node node, File dest) throws IOException {
    DataClient client = (DataClient) connectNode(node);
    if (client == null) {
      return false;
    }

    AtomicReference<ByteBuffer> result = new AtomicReference<>();
    int pullFileRetry = 5;
    for (int i = 0; i < pullFileRetry; i++) {
      try (BufferedOutputStream bufferedOutputStream =
          new BufferedOutputStream(new FileOutputStream(dest))) {
        int offset = 0;
        // TODO-Cluster: use elaborate downloading techniques
        int fetchSize = 64 * 1024;
        GenericHandler<ByteBuffer> handler = new GenericHandler<>(node, result);

        while (true) {
          result.set(null);
          synchronized (result) {
            client.readFile(remotePath, offset, fetchSize, handler);
            result.wait(RaftServer.getConnectionTimeoutInMS());
          }
          ByteBuffer buffer = result.get();
          if (buffer == null || buffer.limit() - buffer.position() == 0) {
            break;
          }

          // notice: the buffer returned by thrift is a slice of a larger buffer which contains
          // the whole response, so buffer.position() is not 0 initially and buffer.limit() is
          // not the size of the downloaded chunk
          bufferedOutputStream.write(buffer.array(), buffer.position() + buffer.arrayOffset(),
              buffer.limit() - buffer.position());
          offset += buffer.limit() - buffer.position();
        }
        bufferedOutputStream.flush();
        if (logger.isInfoEnabled()) {
          logger.info("{}: remote file {} is pulled at {}, length: {}", name, remotePath, dest,
              dest.length());
        }
        return true;
      } catch (TException | InterruptedException e) {
        logger.warn("{}: Cannot pull file {} from {}, wait 5s to retry", name, remotePath, node,
            e);
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
          // ignore
        }
      }
      dest.delete();
      // next try
    }
    return false;
  }

  /**
   * Send the requested snapshots to the applier node.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void pullSnapshot(PullSnapshotRequest request, AsyncMethodCallback resultHandler) {
    if (character != NodeCharacter.LEADER && !readOnly) {
      // if this node has been set readOnly, then it must have been synchronized with the leader
      // otherwise forward the request to the leader
      if (leader != null) {
        logger.debug("{} forwarding a pull snapshot request to the leader {}", name, leader);
        DataClient client = (DataClient) connectNode(leader);
        try {
          client.pullSnapshot(request, new GenericForwardHandler<>(resultHandler));
        } catch (TException e) {
          resultHandler.onError(e);
        }
        return;
      } else {
        waitLeader();
        if (leader == null) {
          resultHandler.onError(new LeaderUnknownException(getAllNodes()));
          return;
        }
      }
    }
    // if the requester pulls the snapshots because the header of the group is removed, then the
    // member should no longer receive new data
    if (request.isRequireReadOnly()) {
      setReadOnly();
    }

    List<Integer> requiredSlots = request.getRequiredSlots();
    for (Integer requiredSlot : requiredSlots) {
      // wait if the data of the slot is in another node
      slotManager.waitSlot(requiredSlot);
    }
    logger.debug("{}: {} slots are requested, first:{}, last: {}", name, requiredSlots.size(),
        requiredSlots.get(0), requiredSlots.get(requiredSlots.size() - 1));

    // If the logs between [currCommitLogIndex, currLastLogIndex] are committed after the
    // snapshot is generated, they will be invisible to the new slot owner and thus lost forever
    long currLastLogIndex = logManager.getLastLogIndex();
    logger.info("{}: Waiting for logs to commit before snapshot, {}/{}", name,
        logManager.getCommitLogIndex(), currLastLogIndex);
    while (logManager.getCommitLogIndex() < currLastLogIndex) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("{}: Unexpected interruption when waiting for logs to commit", name, e);
      }
    }

    // this synchronized should work with the one in AppendEntry when a log is going to commit,
    // which may prevent the newly arrived data from being invisible to the new header.
    synchronized (logManager) {
      PullSnapshotResp resp = new PullSnapshotResp();
      Map<Integer, ByteBuffer> resultMap = new HashMap<>();
      try {
        logManager.takeSnapshot();
      } catch (IOException e) {
        resultHandler.onError(e);
      }

      PartitionedSnapshot allSnapshot = (PartitionedSnapshot) logManager.getSnapshot();
      for (int requiredSlot : requiredSlots) {
        Snapshot snapshot = allSnapshot.getSnapshot(requiredSlot);
        if (snapshot != null) {
          resultMap.put(requiredSlot, snapshot.serialize());
        }
      }
      resp.setSnapshotBytes(resultMap);
      logger.debug("{}: Sending {} snapshots to the requester", name, resultMap.size());
      resultHandler.onComplete(resp);
    }
  }

  /**
   * Pull snapshots from the previous holders after newNode joins the cluster.
   *
   * @param slots
   * @param newNode
   */
  public void pullNodeAdditionSnapshots(List<Integer> slots, Node newNode) {
    synchronized (logManager) {
      logger.info("{} pulling {} slots from remote", name, slots.size());
      PartitionedSnapshot snapshot = (PartitionedSnapshot) logManager.getSnapshot();
      Map<Integer, Node> prevHolders = metaGroupMember.getPartitionTable()
          .getPreviousNodeMap(newNode);

      // group the slots by their owners
      Map<Node, List<Integer>> holderSlotsMap = new HashMap<>();
      for (int slot : slots) {
        // skip the slot if the corresponding data is already replicated locally
        if (snapshot.getSnapshot(slot) == null) {
          Node node = prevHolders.get(slot);
          if (node != null) {
            holderSlotsMap.computeIfAbsent(node, n -> new ArrayList<>()).add(slot);
          }
        }
      }

      // pull snapshots from each owner's data group
      for (Entry<Node, List<Integer>> entry : holderSlotsMap.entrySet()) {
        Node node = entry.getKey();
        List<Integer> nodeSlots = entry.getValue();
        PullSnapshotTaskDescriptor taskDescriptor =
            new PullSnapshotTaskDescriptor(metaGroupMember.getPartitionTable().getHeaderGroup(node),
                nodeSlots, false);
        pullFileSnapshot(taskDescriptor, null);
      }
    }
  }

  /**
   * Pull FileSnapshots (timeseries schemas and lists of TsFiles) of "nodeSlots" from one of the
   * "prevHolders". The actual pulling will be performed in a separate thread.
   *
   * @param descriptor
   * @param snapshotSave set to the corresponding disk file if the task is resumed from disk, or set
   *                     ot null otherwise
   */
  private void pullFileSnapshot(PullSnapshotTaskDescriptor descriptor, File snapshotSave) {
    Iterator<Integer> iterator = descriptor.getSlots().iterator();
    while (iterator.hasNext()) {
      Integer nodeSlot = iterator.next();
      SlotStatus status = slotManager.getStatus(nodeSlot);
      if (status != SlotStatus.NULL) {
        // the pulling may already be issued during restart, skip it in that case
        iterator.remove();
      } else {
        // mark the slot as pulling to control reads and writes of the pulling slot
        slotManager.setToPulling(nodeSlot, descriptor.getPreviousHolders().getHeader());
      }
    }
    if (descriptor.getSlots().isEmpty()) {
      return;
    }

    pullSnapshotService.submit(new PullSnapshotTask(descriptor, this, FileSnapshot::new, null));
  }

  /**
   * Restart all unfinished pull-snapshot-tasks of the member.
   */
  public void resumePullSnapshotTasks() {
    File snapshotTaskDir = new File(getPullSnapshotTaskDir());
    if (!snapshotTaskDir.exists()) {
      return;
    }

    File[] files = snapshotTaskDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(PullSnapshotTask.TASK_SUFFIX)) {
          try (DataInputStream dataInputStream =
              new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            PullSnapshotTaskDescriptor descriptor = new PullSnapshotTaskDescriptor();
            descriptor.deserialize(dataInputStream);
            pullFileSnapshot(descriptor, file);
          } catch (IOException e) {
            logger.error("Cannot resume pull-snapshot-task in file {}", file, e);
            file.delete();
          }
        }
      }
    }
  }

  /**
   * @return a directory that stores the information of ongoing pulling snapshot tasks.
   */
  public String getPullSnapshotTaskDir() {
    return getMemberDir() + "snapshot_task" + File.separator;
  }

  /**
   * @return the path of the directory that is provided exclusively for the member.
   */
  public String getMemberDir() {
    return IoTDBDescriptor.getInstance().getConfig().getBaseDir() + File.separator +
        "raft" + File.separator + getHeader().nodeIdentifier + File.separator;
  }

  public MetaGroupMember getMetaGroupMember() {
    return metaGroupMember;
  }

  /**
   * If the member is the leader, let all members in the group close the specified partition of a
   * storage group, else just return false.
   *
   * @param storageGroupName
   * @param partitionId
   * @param isSeq
   * @return false if the member is not a leader, true if the close request is accepted by the
   * quorum
   */
  public boolean closePartition(String storageGroupName, long partitionId, boolean isSeq) {
    if (character != NodeCharacter.LEADER) {
      return false;
    }
    CloseFileLog log = new CloseFileLog(storageGroupName, partitionId, isSeq);
    synchronized (logManager) {
      log.setCurrLogTerm(getTerm().get());
      log.setPreviousLogIndex(logManager.getLastLogIndex());
      log.setPreviousLogTerm(logManager.getLastLogTerm());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      logManager.append(log);

      logger.info("Send the close file request of {} to other nodes", log);
    }
    return appendLogInGroup(log);
  }

  /**
   * Execute a non-query plan. If the member is a leader, a log for the plan will be created and
   * process through the raft procedure, otherwise the plan will be forwarded to the leader.
   *
   * @param plan a non-query plan.
   * @return
   */
  TSStatus executeNonQuery(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    }

    return forwardPlan(plan, leader, getHeader());
  }

  /**
   * Send the timeseries schemas of some prefix paths to the requestor. The schemas will be sent in
   * the form of a list of MeasurementSchema, but notice the measurements in them are the full
   * paths.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    // try to synchronize with the leader first in case that some schema logs are accepted but
    // not committed yet
    if (!syncLeader()) {
      // if this node cannot synchronize with the leader with in a given time, forward the
      // request to the leader
      waitLeader();
      DataClient client = (DataClient) connectNode(leader);
      if (client == null) {
        resultHandler.onError(new LeaderUnknownException(getAllNodes()));
        return;
      }
      try {
        client.pullTimeSeriesSchema(request, resultHandler);
      } catch (TException e) {
        resultHandler.onError(e);
      }
      return;
    }

    // collect local timeseries schemas and send to the requester
    // the measurements in them are the full paths.
    List<String> prefixPaths = request.getPrefixPaths();
    List<MeasurementSchema> timeseriesSchemas = new ArrayList<>();
    for (String prefixPath : prefixPaths) {
      MManager.getInstance().collectSeries(prefixPath, timeseriesSchemas);
    }

    PullSchemaResp resp = new PullSchemaResp();
    // serialize the schemas
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeInt(timeseriesSchemas.size());
      for (MeasurementSchema timeseriesSchema : timeseriesSchemas) {
        timeseriesSchema.serializeTo(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable for we are using a ByteArrayOutputStream
    }
    resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
    resultHandler.onComplete(resp);
  }

  /**
   * Create an IPointReader of "path" with “timeFilter” and "valueFilter". A synchronization with
   * the leader will be performed first to preserve strong consistency. TODO-Cluster: also support
   * weak consistency
   *
   * @param path
   * @param dataType
   * @param timeFilter  nullable
   * @param valueFilter nullable
   * @param context
   * @return
   * @throws StorageEngineException
   */
  IPointReader getSeriesPointReader(Path path, Set<String> allSensors, TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    // pull the newest data
    if (syncLeader()) {
      return new SeriesRawDataPointReader(
          getSeriesReader(path, allSensors, dataType, timeFilter,
              valueFilter, context));
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

  /**
   * Create an IBatchReader of "path" with “timeFilter” and "valueFilter". A synchronization with
   * the leader will be performed first to preserve strong consistency. TODO-Cluster: also support
   * weak consistency
   *
   * @param path
   * @param dataType
   * @param timeFilter  nullable
   * @param valueFilter nullable
   * @param context
   * @return an IBatchReader or null if there is no satisfying data
   * @throws StorageEngineException
   */
  IBatchReader getSeriesBatchReader(Path path, Set<String> allSensors, TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    // pull the newest data
    if (syncLeader()) {
      SeriesReader seriesReader = getSeriesReader(path, allSensors, dataType, timeFilter,
          valueFilter, context);
      if (seriesReader.isEmpty()) {
        return null;
      }
      return new SeriesRawDataBatchReader(seriesReader);
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

  /**
   * Create a SeriesReader of "path" with “timeFilter” and "valueFilter". The consistency is not
   * guaranteed here and only data slots managed by the member will be queried.
   *
   * @param path
   * @param dataType
   * @param timeFilter  nullable
   * @param valueFilter nullable
   * @param context
   * @return
   * @throws StorageEngineException
   */
  private SeriesReader getSeriesReader(Path path, Set<String> allSensors, TSDataType
      dataType,
      Filter timeFilter,
      Filter valueFilter, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    if (!MManager.getInstance().isPathExist(path.getFullPath())) {
      try {
        List<MeasurementSchema> schemas = metaGroupMember
            .pullTimeSeriesSchemas(Collections.singletonList(path.getFullPath()));
        for (MeasurementSchema schema : schemas) {
          MManager.getInstance().cacheSchema(path.getFullPath(), schema);
        }
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }
    List<Integer> nodeSlots = metaGroupMember.getPartitionTable().getNodeSlots(getHeader());
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
    return new SeriesReader(path, allSensors, dataType, context, queryDataSource,
        timeFilter, valueFilter, new SlotTsFileFilter(nodeSlots));
  }

  /**
   * Create an IReaderByTimestamp of "path". A synchronization with the leader will be performed
   * first to preserve strong consistency. TODO-Cluster: also support weak consistency
   *
   * @param path
   * @param dataType
   * @param context
   * @return an IReaderByTimestamp or null if there is no satisfying data
   * @throws StorageEngineException
   */
  IReaderByTimestamp getReaderByTimestamp(Path path, Set<String> allSensors, TSDataType
      dataType,
      QueryContext context)
      throws StorageEngineException, QueryProcessException {
    if (syncLeader()) {
      SeriesReader seriesReader = getSeriesReader(path, allSensors, dataType,
          TimeFilter.gtEq(Long.MIN_VALUE),
          null, context);
      if (seriesReader.isEmpty()) {
        return null;
      }
      return new SeriesReaderByTimestamp(seriesReader);
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

  /**
   * Create an IBatchReader of a path, register it in the query manager to get a reader id for it
   * and send the id back to the requester. If the reader does not have any data, an id of -1 will
   * be returned.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void querySingleSeries(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    logger.debug("{}: {} is querying {}, queryId: {}", name, request.getRequester(),
        request.getPath(), request.getQueryId());
    if (!syncLeader()) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }

    Path path = new Path(request.getPath());
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    Filter timeFilter = null;
    Filter valueFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    if (request.isSetValueFilterBytes()) {
      valueFilter = FilterFactory.deserialize(request.valueFilterBytes);
    }
    Set<String> deviceMeasurements = request.getDeviceMeasurements();

    // the same query from a requester correspond to a context here
    RemoteQueryContext queryContext = getQueryManager().getQueryContext(request.getRequester(),
        request.getQueryId());
    logger.debug("{}: local queryId for {}#{} is {}", name, request.getQueryId(),
        request.getPath(), queryContext.getQueryId());
    try {
      IBatchReader batchReader = getSeriesBatchReader(path, deviceMeasurements, dataType,
          timeFilter,
          valueFilter, queryContext);

      // if the reader contains no data, send a special id of -1 to prevent the requester from
      // meaninglessly fetching data
      if (batchReader != null && batchReader.hasNextBatch()) {
        long readerId = getQueryManager().registerReader(batchReader);
        queryContext.registerLocalReader(readerId);
        logger.debug("{}: Build a reader of {} for {}#{}, readerId: {}", name, path,
            request.getRequester(), request.getQueryId(), readerId);
        resultHandler.onComplete(readerId);
      } else {
        logger.debug("{}: There is no data {} for {}#{}", name, path,
            request.getRequester(), request.getQueryId());
        resultHandler.onComplete(-1L);
        if (batchReader != null) {
          batchReader.close();
        }
      }
    } catch (IOException | StorageEngineException | QueryProcessException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Create an IReaderByTime of a path, register it in the query manager to get a reader id for it
   * and send the id back to the requester. If the reader does not have any data, an id of -1 will
   * be returned.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void querySingleSeriesByTimestamp(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    logger
        .debug("{}: {} is querying {} by timestamp, queryId: {}", name, request.getRequester(),
            request.getPath(), request.getQueryId());
    if (!syncLeader()) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }

    Path path = new Path(request.getPath());
    TSDataType dataType = TSDataType.values()[request.dataTypeOrdinal];
    Set<String> deviceMeasurements = request.getDeviceMeasurements();

    RemoteQueryContext queryContext = getQueryManager().getQueryContext(request.getRequester(),
        request.getQueryId());
    logger.debug("{}: local queryId for {}#{} is {}", name, request.getQueryId(),
        request.getPath(), queryContext.getQueryId());
    try {
      IReaderByTimestamp readerByTimestamp = getReaderByTimestamp(path, deviceMeasurements,
          dataType,
          queryContext);
      if (readerByTimestamp != null) {
        long readerId = getQueryManager().registerReaderByTime(readerByTimestamp);
        queryContext.registerLocalReader(readerId);

        logger.debug("{}: Build a readerByTimestamp of {} for {}, readerId: {}", name, path,
            request.getRequester(), readerId);
        resultHandler.onComplete(readerId);
      } else {
        logger.debug("{}: There is no data {} for {}#{}", name, path,
            request.getRequester(), request.getQueryId());
        resultHandler.onComplete(-1L);
      }
    } catch (StorageEngineException | QueryProcessException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Find the QueryContext related a query of "queryId" in "requester" and release all resources of
   * the context.
   *
   * @param header
   * @param requester
   * @param queryId
   * @param resultHandler
   */
  @Override
  public void endQuery(Node header, Node requester, long queryId,
      AsyncMethodCallback<Void> resultHandler) {
    try {
      getQueryManager().endQuery(requester, queryId);
      resultHandler.onComplete(null);
    } catch (StorageEngineException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Return the data of the reader whose id is "readerId", using timestamps in "timeBuffer".
   *
   * @param header
   * @param readerId
   * @param time
   * @param resultHandler
   */
  @Override
  public void fetchSingleSeriesByTimestamp(Node header, long readerId, long time,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    IReaderByTimestamp reader = getQueryManager().getReaderByTimestamp(readerId);
    if (reader == null) {
      resultHandler.onError(new ReaderNotFoundException(readerId));
      return;
    }
    try {
      Object value = reader.getValueInTimestamp(time);
      if (value != null) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        SerializeUtils.serializeObject(value, dataOutputStream);
        resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      } else {
        resultHandler.onComplete(ByteBuffer.allocate(0));
      }
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Fetch a batch from the reader whose id is "readerId".
   *
   * @param header
   * @param readerId
   * @param resultHandler
   */
  @Override
  public void fetchSingleSeries(Node header, long readerId,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    IBatchReader reader = getQueryManager().getReader(readerId);
    if (reader == null) {
      resultHandler.onError(new ReaderNotFoundException(readerId));
      return;
    }
    try {
      if (reader.hasNextBatch()) {
        BatchData batchData = reader.nextBatch();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        SerializeUtils.serializeBatchData(batchData, dataOutputStream);
        logger.debug("{}: Send results of reader {}, size:{}", name, readerId,
            batchData.length());
        resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      } else {
        resultHandler.onComplete(ByteBuffer.allocate(0));
      }
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Get the local paths that match any path in "paths". The result is not deduplicated.
   *
   * @param header
   * @param paths         paths potentially contain wildcards
   * @param resultHandler
   */
  @Override
  public void getAllPaths(Node header, List<String> paths,
      AsyncMethodCallback<List<String>> resultHandler) {
    try {
      List<String> ret = new ArrayList<>();
      for (String path : paths) {
        ret.addAll(MManager.getInstance().getAllTimeseriesName(path));
      }
      resultHandler.onComplete(ret);
    } catch (MetadataException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Get the local devices that match any path in "paths". The result is deduplicated.
   *
   * @param header
   * @param paths         paths potentially contain wildcards
   * @param resultHandler
   */
  @Override
  public void getAllDevices(Node header, List<String> paths,
      AsyncMethodCallback<Set<String>> resultHandler) {
    try {
      Set<String> results = new HashSet<>();
      for (String path : paths) {
        results.addAll(MManager.getInstance().getDevices(path));
      }
      resultHandler.onComplete(results);
    } catch (MetadataException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * When the node does not play a member in a group any more, the corresponding local data should
   * be removed.
   */
  public void removeLocalData(List<Integer> slots) {
    for (Integer slot : slots) {
      //TODO-Cluster: remove the data in the slot
    }
  }

  /**
   * When a node is removed and IT IS NOT THE HEADER of the group, the member should take over some
   * slots from the removed group, and add a new node to the group the removed node was in the
   * group.
   */
  public void removeNode(Node removedNode, NodeRemovalResult removalResult) {
    synchronized (allNodes) {
      if (allNodes.contains(removedNode)) {
        // update the group if the deleted node was in it
        allNodes = metaGroupMember.getPartitionTable().getHeaderGroup(getHeader());
        initPeerMap();
        if (removedNode.equals(leader)) {
          // if the leader is removed, also start an election immediately
          synchronized (term) {
            setCharacter(NodeCharacter.ELECTOR);
            setLastHeartbeatReceivedTime(Long.MIN_VALUE);
          }
        }
      }
      List<Integer> slotsToPull = removalResult.getNewSlotOwners().get(getHeader());
      if (slotsToPull != null) {
        // pull the slots that should be taken over
        PullSnapshotTaskDescriptor taskDescriptor = new PullSnapshotTaskDescriptor(
            removalResult.getRemovedGroup(),
            slotsToPull, true);
        pullFileSnapshot(taskDescriptor, null);
      }
    }
  }

  /**
   * Generate a report containing the character, leader, term, last log term, last log index, header
   * and readOnly or not of this member.
   *
   * @return
   */
  public DataMemberReport genReport() {
    return new DataMemberReport(character, leader, term.get(),
        logManager.getLastLogTerm(), logManager.getLastLogIndex(), logManager.getCommitLogIndex(),
        logManager.getCommitLogTerm(), getHeader(), readOnly,
        QueryCoordinator.getINSTANCE()
            .getLastResponseLatency(getHeader()), lastHeartbeatReceivedTime);
  }

  @TestOnly
  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  /**
   * Get the nodes of a prefix "path" at "nodeLevel". The method currently requires strong
   * consistency.
   *
   * @param header
   * @param path
   * @param nodeLevel
   * @param resultHandler
   */
  @Override
  public void getNodeList(Node header, String path, int nodeLevel,
      AsyncMethodCallback<List<String>> resultHandler) {
    if (!syncLeader()) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }
    try {
      resultHandler.onComplete(MManager.getInstance().getNodesList(path, nodeLevel));
    } catch (MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getChildNodePathInNextLevel(Node header, String path,
      AsyncMethodCallback<Set<String>> resultHandler) {
    if (!syncLeader()) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }
    try {
      resultHandler.onComplete(MManager.getInstance().getChildNodePathInNextLevel(path));
    } catch (MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getAllMeasurementSchema(Node header, ByteBuffer planBuffer,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    if (!syncLeader()) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }
    try {
      ShowTimeSeriesPlan plan = (ShowTimeSeriesPlan) PhysicalPlan.Factory.create(planBuffer);
      List<ShowTimeSeriesResult> allTimeseriesSchema;
      if (plan.getKey() != null && plan.getValue() != null) {
        allTimeseriesSchema = MManager.getInstance().getAllTimeseriesSchema(plan);
      } else {
        allTimeseriesSchema = MManager.getInstance().showTimeseries(plan);
      }

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      dataOutputStream.writeInt(allTimeseriesSchema.size());
      for (ShowTimeSeriesResult result : allTimeseriesSchema) {
        result.serialize(outputStream);
      }
      resultHandler.onComplete(ByteBuffer.wrap(outputStream.toByteArray()));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }


  /**
   * Execute aggregations over the given path and return the results to the requester.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void getAggrResult(GetAggrResultRequest request,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    logger.debug("{}: {} is querying {} by aggregation, queryId: {}", name,
        request.getRequestor(),
        request.getPath(), request.getQueryId());

    List<String> aggregations = request.getAggregations();
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    String path = request.getPath();
    Filter timeFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    RemoteQueryContext queryContext = queryManager
        .getQueryContext(request.getRequestor(), request.queryId);
    Set<String> deviceMeasurements = request.getDeviceMeasurements();

    // do the aggregations locally
    List<AggregateResult> results;
    try {
      results = getAggrResult(aggregations, deviceMeasurements, dataType, path, timeFilter,
          queryContext);
      logger.trace("{}: aggregation results {}, queryId: {}", name, results, request.getQueryId());
    } catch (StorageEngineException | IOException | QueryProcessException | LeaderUnknownException e) {
      resultHandler.onError(e);
      return;
    }

    // serialize and send the results
    List<ByteBuffer> resultBuffers = new ArrayList<>();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (AggregateResult result : results) {
      try {
        result.serializeTo(byteArrayOutputStream);
      } catch (IOException e) {
        // ignore since we are using a ByteArrayOutputStream
      }
      resultBuffers.add(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      byteArrayOutputStream.reset();
    }
    resultHandler.onComplete(resultBuffers);
  }

  /**
   * Execute "aggregation" over "path" with "timeFilter". This method currently requires strong
   * consistency. Only data managed by this group will be used for aggregation.
   *
   * @param aggregations aggregation names in SQLConstant
   * @param dataType
   * @param path
   * @param timeFilter   nullable
   * @param context
   * @return
   * @throws IOException
   * @throws StorageEngineException
   * @throws QueryProcessException
   * @throws LeaderUnknownException
   */
  public List<AggregateResult> getAggrResult(List<String> aggregations,
      Set<String> allSensors, TSDataType dataType, String path,
      Filter timeFilter, QueryContext context)
      throws IOException, StorageEngineException, QueryProcessException, LeaderUnknownException {
    if (!syncLeader()) {
      throw new LeaderUnknownException(getAllNodes());

    }
    if (!MManager.getInstance().isPathExist(path)) {
      try {
        List<MeasurementSchema> schemas = metaGroupMember
            .pullTimeSeriesSchemas(Collections.singletonList(path));
        for (MeasurementSchema schema : schemas) {
          MManager.getInstance().cacheSchema(path, schema);
        }
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }
    List<AggregateResult> results = new ArrayList<>();
    for (String aggregation : aggregations) {
      results.add(AggregateResultFactory.getAggrResultByName(aggregation, dataType));
    }
    List<Integer> nodeSlots = metaGroupMember.getPartitionTable().getNodeSlots(getHeader());
    AggregationExecutor.aggregateOneSeries(new Path(path), allSensors, context, timeFilter,
        dataType, results, new SlotTsFileFilter(nodeSlots));
    return results;
  }

  @TestOnly
  public void setLogManager(PartitionedSnapshotLogManager logManager) {
    if (this.logManager != null) {
      this.logManager.close();
    }
    this.logManager = logManager;
    super.setLogManager(logManager);
    initPeerMap();
  }

  /**
   * Create a local GroupByExecutor that will run aggregations of "aggregationTypes" over "path"
   * with "timeFilter". The method currently requires strong consistency.
   *
   * @param path
   * @param dataType
   * @param timeFilter       nullable
   * @param aggregationTypes ordinals of AggregationType
   * @param context
   * @return
   * @throws StorageEngineException
   */
  public LocalGroupByExecutor getGroupByExecutor(Path path,
      Set<String> deviceMeasurements, TSDataType dataType,
      Filter timeFilter,
      List<Integer> aggregationTypes, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    // pull the newest data
    if (syncLeader()) {
      List<Integer> nodeSlots = metaGroupMember.getPartitionTable().getNodeSlots(getHeader());
      LocalGroupByExecutor executor = new LocalGroupByExecutor(path, deviceMeasurements,
          dataType
          , context, timeFilter, new SlotTsFileFilter(nodeSlots));
      for (Integer aggregationType : aggregationTypes) {
        executor.addAggregateResult(AggregateResultFactory
            .getAggrResultByType(AggregationType.values()[aggregationType], dataType));
      }
      return executor;
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

  /**
   * Create a local GroupByExecutor that will run aggregations of "aggregationTypes" over "path"
   * with "timeFilter", register it in the query manager to generate the executor id, and send it
   * back to the requester.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void getGroupByExecutor(GroupByRequest
      request, AsyncMethodCallback<Long> resultHandler) {
    Path path = new Path(request.getPath());
    List<Integer> aggregationTypeOrdinals = request.getAggregationTypeOrdinals();
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    Filter timeFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    long queryId = request.getQueryId();
    logger.debug("{}: {} is querying {} using group by, queryId: {}", name,
        request.getRequestor(), path, queryId);
    Set<String> deviceMeasurements = request.getDeviceMeasurements();

    RemoteQueryContext queryContext = queryManager
        .getQueryContext(request.getRequestor(), queryId);
    try {
      LocalGroupByExecutor executor = getGroupByExecutor(path, deviceMeasurements, dataType,
          timeFilter, aggregationTypeOrdinals, queryContext);
      if (!executor.isEmpty()) {
        long executorId = queryManager.registerGroupByExecutor(executor);
        logger.debug("{}: Build a GroupByExecutor of {} for {}, executorId: {}", name, path,
            request.getRequestor(), executor);
        queryContext.registerLocalGroupByExecutor(executorId);
        resultHandler.onComplete(executorId);
      } else {
        logger.debug("{}: There is no data {} for {}#{}", name, path,
            request.getRequestor(), request.getQueryId());
        resultHandler.onComplete(-1L);
      }
    } catch (StorageEngineException | QueryProcessException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Fetch the aggregation results between [startTime, endTime] of the executor whose id is
   * "executorId". This method currently requires strong consistency.
   *
   * @param header
   * @param executorId
   * @param startTime
   * @param endTime
   * @param resultHandler
   */
  @Override
  public void getGroupByResult(Node header, long executorId, long startTime, long endTime,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    GroupByExecutor executor = getQueryManager().getGroupByExecutor(executorId);
    if (executor == null) {
      resultHandler.onError(new ReaderNotFoundException(executorId));
      return;
    }
    try {
      List<AggregateResult> results = executor.calcResult(startTime, endTime);
      List<ByteBuffer> resultBuffers = new ArrayList<>();
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      for (AggregateResult result : results) {
        result.serializeTo(byteArrayOutputStream);
        resultBuffers.add(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
        byteArrayOutputStream.reset();
      }
      logger.debug("{}: Send results of group by executor {}, size:{}", name, executor,
          resultBuffers.size());
      resultHandler.onComplete(resultBuffers);
    } catch (IOException | QueryProcessException e) {
      resultHandler.onError(e);
    }
  }

  public SlotManager getSlotManager() {
    return slotManager;
  }

  @Override
  public void previousFill(PreviousFillRequest request,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    Path path = new Path(request.getPath());
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    long queryId = request.getQueryId();
    long queryTime = request.getQueryTime();
    long beforeRange = request.getBeforeRange();
    Node requester = request.getRequester();
    Set<String> deviceMeasurements = request.getDeviceMeasurements();
    RemoteQueryContext queryContext = queryManager.getQueryContext(requester, queryId);

    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      TimeValuePair timeValuePair = localPreviousFill(path, dataType, queryTime, beforeRange,
          deviceMeasurements, queryContext);
      SerializeUtils.serializeTVPair(timeValuePair, dataOutputStream);
      resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    } catch (QueryProcessException | StorageEngineException | IOException | LeaderUnknownException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Perform a local previous fill and return the fill result.
   *
   * @param path
   * @param dataType
   * @param queryTime
   * @param beforeRange
   * @param deviceMeasurements
   * @param context
   * @return
   * @throws QueryProcessException
   * @throws StorageEngineException
   * @throws IOException
   */
  public TimeValuePair localPreviousFill(Path path, TSDataType dataType, long queryTime,
      long beforeRange,
      Set<String> deviceMeasurements, QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException, LeaderUnknownException {
    if (!syncLeader()) {
      throw new LeaderUnknownException(getAllNodes());
    }
    PreviousFill previousFill = new PreviousFill(dataType, queryTime, beforeRange);
    previousFill.configureFill(path, dataType, queryTime, deviceMeasurements, context);
    return previousFill.getFillResult();
  }
}
