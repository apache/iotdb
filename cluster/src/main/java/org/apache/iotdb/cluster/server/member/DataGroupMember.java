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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.manage.FilePartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTask;
import org.apache.iotdb.cluster.log.snapshot.RemoteFileSnapshot;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.ClusterQueryParser;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.NodeReport.DataMemberReport;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.GenericForwardHandler;
import org.apache.iotdb.cluster.server.heartbeat.DataHeartBeatThread;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithValueFilter;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGroupMember extends RaftMember implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataGroupMember.class);
  private static final String REMOTE_FILE_TEMP_DIR = "remote";

  private MetaGroupMember metaGroupMember;

  private ExecutorService pullSnapshotService;
  private PartitionedSnapshotLogManager logManager;

  private ClusterQueryManager queryManager;

  @TestOnly
  public DataGroupMember() {
    // constructor for test
  }

  DataGroupMember(TProtocolFactory factory, PartitionGroup nodes, Node thisNode,
      PartitionedSnapshotLogManager logManager, MetaGroupMember metaGroupMember,
      TAsyncClientManager clientManager) {
    super("Data(" + nodes.getHeader().getIp() + ":" + nodes.getHeader().getMetaPort() + ")",
        new ClientPool(new DataClient.Factory(clientManager, factory)));
    this.thisNode = thisNode;
    this.logManager = logManager;
    super.logManager = logManager;
    this.metaGroupMember = metaGroupMember;
    allNodes = nodes;
    queryProcessor = new ClusterQueryParser(metaGroupMember);
    queryManager = new ClusterQueryManager();
  }

  @Override
  public void start() throws TTransportException {
    super.start();
    heartBeatService.submit(new DataHeartBeatThread(this));
    pullSnapshotService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void stop() {
    super.stop();
    pullSnapshotService.shutdownNow();
    pullSnapshotService = null;
    try {
      queryManager.endAllQueries();
    } catch (StorageEngineException e) {
      logger.error("Cannot release queries of {}", name, e);
    }
  }

  /**
   * The first node (on the hash ring) in this data group is the header. It determines the duty
   * (what range on the ring do the group take responsibility for) of the group and although other
   * nodes in this may change, this node is unchangeable unless the data group is dismissed. It
   * is also the identifier of this data group.
   */
  @Override
  public Node getHeader() {
    return allNodes.get(0);
  }

  public static class Factory {
    private TProtocolFactory protocolFactory;
    private MetaGroupMember metaGroupMember;
    private TAsyncClientManager clientManager;
    private LogApplier applier;

    Factory(TProtocolFactory protocolFactory, MetaGroupMember metaGroupMember, LogApplier applier
        ,TAsyncClientManager clientManager) {
      this.protocolFactory = protocolFactory;
      this.metaGroupMember = metaGroupMember;
      this.applier = applier;
      this.clientManager = clientManager;
    }

    public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
      return new DataGroupMember(protocolFactory, partitionGroup, thisNode,
          new FilePartitionedSnapshotLogManager(applier, metaGroupMember.getPartitionTable(),
              partitionGroup.getHeader()), metaGroupMember, clientManager);
    }
  }

  /**
   * Try to add a Node into this group
   * @param node
   * @return true if this node should leave the group because of the addition of the node, false
   * otherwise
   */
  public synchronized boolean addNode(Node node) {
    // when a new node is added, start an election instantly to avoid the stale leader still
    // taking the leadership
    synchronized (term) {
      term.incrementAndGet();
      setLastHeartBeatReceivedTime(System.currentTimeMillis());
      setCharacter(NodeCharacter.ELECTOR);
      leader = null;
    }
    synchronized (allNodes) {
      int insertIndex = -1;
      for (int i = 0; i < allNodes.size() - 1; i++) {
        Node prev = allNodes.get(i);
        Node next = allNodes.get(i + 1);
        if (prev.nodeIdentifier < node.nodeIdentifier && node.nodeIdentifier < next.nodeIdentifier
            || prev.nodeIdentifier < node.nodeIdentifier && next.nodeIdentifier < prev.nodeIdentifier
            || node.nodeIdentifier < next.nodeIdentifier && next.nodeIdentifier < prev.nodeIdentifier
            ) {
          insertIndex = i + 1;
          break;
        }
      }
      if (insertIndex > 0) {
        allNodes.add(insertIndex, node);
        Node removedNode = allNodes.remove(allNodes.size() - 1);
        // if the local node is the last node and the insertion succeeds, this node should leave
        // the group
        logger.debug("{}: Node {} is inserted into the data group {}", name, node, allNodes);
        return removedNode.equals(thisNode);
      }
      return false;
    }
  }

  @Override
  long processElectionRequest(ElectionRequest electionRequest) {
    // to be a data group leader, a node should also be qualified to be the meta group leader
    // which guarantees the data group leader has the newest partition table.
    long thatTerm = electionRequest.getTerm();
    long thatMetaLastLogId = electionRequest.getLastLogIndex();
    long thatMetaLastLogTerm = electionRequest.getLastLogTerm();
    logger.info("{} received an election request, term:{}, metaLastLogId:{}, metaLastLogTerm:{}",
        name, thatTerm, thatMetaLastLogId, thatMetaLastLogTerm);

    long thisMetaLastLogIndex = metaGroupMember.getLogManager().getLastLogIndex();
    long thisMetaLastLogTerm = metaGroupMember.getLogManager().getLastLogTerm();

    // term of the electors's MetaGroupMember is not verified, so 0 and 1 are used to make sure
    // the verification does not fail
    long metaResponse = verifyElector(0, thisMetaLastLogIndex, thisMetaLastLogTerm,
        1, thatMetaLastLogId, thatMetaLastLogTerm);
    if (metaResponse == Response.RESPONSE_LOG_MISMATCH) {
      return Response.RESPONSE_META_LOG_STALE;
    }

    // check data logs
    long thatDataLastLogId = electionRequest.getDataLogLastIndex();
    long thatDataLastLogTerm = electionRequest.getDataLogLastTerm();
    logger.info("{} received an election request, term:{}, dataLastLogId:{}, dataLastLogTerm:{}",
        name, thatTerm, thatDataLastLogId, thatDataLastLogTerm);

    long resp = verifyElector(term.get(), logManager.getLastLogIndex(),
        logManager.getLastLogTerm(), thatTerm, thatDataLastLogId, thatDataLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      term.set(thatTerm);
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartBeatReceivedTime = System.currentTimeMillis();
      leader = electionRequest.getElector();
    }
    return resp;
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
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

  public void applySnapshot(Snapshot snapshot, int slot) {
    logger.debug("{}: applying snapshot {} of slot {}", name, snapshot, slot);
    if (snapshot instanceof FileSnapshot) {
      applyFileSnapshot((FileSnapshot) snapshot);
    } else {
      logger.error("Unrecognized snapshot {}", snapshot);
    }
  }

  private void applyFileSnapshot(FileSnapshot snapshot) {
    synchronized (logManager) {
      for (MeasurementSchema schema : snapshot.getTimeseriesSchemas()) {
        SchemaUtils.registerTimeseries(schema);
      }

      List<RemoteTsFileResource> remoteTsFileResources = snapshot.getDataFiles();
      for (RemoteTsFileResource resource : remoteTsFileResources) {
        if (!isFileAlreadyPulled(resource)) {
          loadRemoteFile(resource);
        }
      }
    }
  }

  private boolean isFileAlreadyPulled(RemoteTsFileResource resource) {
    // TODO-Cluster#352: The problem of duplicated data in remote files is partially resolved by
    //  tracking the merge history using the version numbers of the merged files. But a better
    //  solution still remains to be found.
    String separatorString = File.separator.equals("\\") ? "\\\\" : "/";
    String[] pathSegments = resource.getFile().getAbsolutePath().split(separatorString);
    int segSize = pathSegments.length;
    // {storageGroupName}/{fileName}
    String storageGroupName = pathSegments[segSize - 2];
    return StorageEngine.getInstance().isFileAlreadyExist(resource, storageGroupName);
  }


  private void applyPartitionedSnapshot(PartitionedSnapshot snapshot) {
    synchronized (logManager) {
      List<Integer> slots = metaGroupMember.getPartitionTable().getNodeSlots(getHeader());
      for (Integer slot : slots) {
        Snapshot subSnapshot = snapshot.getSnapshot(slot);
        if (subSnapshot != null) {
          applySnapshot(subSnapshot, slot);
        }
      }
      logManager.setLastLogId(snapshot.getLastLogId());
      logManager.setLastLogTerm(snapshot.getLastLogTerm());
    }
  }

  private void loadRemoteFile(RemoteTsFileResource resource) {
    Node source = resource.getSource();
    PartitionGroup partitionGroup = metaGroupMember.getPartitionTable().getHeaderGroup(source);
    for (Node node : partitionGroup) {
      File tempFile = pullRemoteFile(resource, node);
      if (tempFile != null) {
        resource.setFile(tempFile);
        try {
          resource.serialize();
          loadRemoteResource(resource);
          logger.info("{}: Remote file {} is successfully loaded", name, resource);
          return;
        } catch (IOException e) {
          logger.error("{}: Cannot serialize {}", name, resource, e);
        }
      }
    }
    logger.error("{}: Cannot load remote file {} from group {}", name, resource, partitionGroup);
  }

  private void loadRemoteResource(RemoteTsFileResource resource) {
    // remote/{nodeIdentifier}/{storageGroupName}/{fileName}
    String separatorString = File.separator.equals("\\") ? "\\\\" : "/";
    String[] pathSegments = resource.getFile().getAbsolutePath().split(separatorString);
    int segSize = pathSegments.length;
    String storageGroupName = pathSegments[segSize - 2];
    File remoteModFile =
        new File(resource.getFile().getAbsoluteFile() + ModificationFile.FILE_SUFFIX);
    try {
      StorageEngine.getInstance().getProcessor(storageGroupName).loadNewTsFile(resource);
      StorageEngine.getInstance().getProcessor(storageGroupName).removeFullyOverlapFiles(resource);
    } catch (TsFileProcessorException | StorageEngineException e) {
      logger.error("{}: Cannot load remote file {} into storage group", name, resource, e);
      return;
    }
    if (remoteModFile.exists()) {
      // when successfully loaded, the file in the resource will be changed
      File localModFile =
          new File(resource.getFile().getAbsoluteFile() + ModificationFile.FILE_SUFFIX);
      remoteModFile.renameTo(localModFile);
    }
    resource.setRemote(false);
  }

  private File pullRemoteFile(RemoteTsFileResource resource, Node node) {
    logger.debug("{}: pulling remote file {} from {}", name, resource, node);

    String separatorString = File.separator.equals("\\") ? "\\\\" : "/";
    String[] pathSegments = resource.getFile().getAbsolutePath().split(separatorString);
    int segSize = pathSegments.length;
    // remote/{nodeIdentifier}/{storageGroupName}/{fileName}
    String tempFileName =
        node.getNodeIdentifier() + File.separator + pathSegments[segSize - 2] + File.separator + pathSegments[segSize - 1];
    File tempFile = new File(REMOTE_FILE_TEMP_DIR, tempFileName);
    tempFile.getParentFile().mkdirs();
    File tempModFile = new File(REMOTE_FILE_TEMP_DIR, tempFileName + ModificationFile.FILE_SUFFIX);
    if (pullRemoteFile(resource.getFile().getAbsolutePath(), node, tempFile)) {
      if (!checkMd5(tempFile, resource.getMd5())) {
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
    // TODO-Cluster#353: implement
    return true;
  }

  private boolean pullRemoteFile(String remotePath, Node node, File dest) {
    DataClient client = (DataClient) connectNode(node);
    if (client == null) {
      return false;
    }

    AtomicReference<ByteBuffer> result = new AtomicReference<>();
    try (BufferedOutputStream bufferedOutputStream =
        new BufferedOutputStream(new FileOutputStream(dest))) {
      int offset = 0;
      int fetchSize = 64 * 1024;
      GenericHandler<ByteBuffer> handler = new GenericHandler<>(node, result);

      while (true) {
        result.set(null);
        synchronized (result) {
          client.readFile(remotePath, offset, fetchSize, getHeader(), handler);
          result.wait(RaftServer.connectionTimeoutInMS);
        }
        ByteBuffer buffer = result.get();
        if (buffer == null || buffer.array().length == 0) {
          break;
        }

        bufferedOutputStream.write(buffer.array(), buffer.position() + buffer.arrayOffset(),
            buffer.limit() - buffer.position());
        offset += buffer.array().length;
      }
      bufferedOutputStream.flush();
    } catch (IOException e) {
      logger.error("{}: Cannot create temp file for {}", name, remotePath, e);
      return false;
    } catch (TException | InterruptedException e) {
      logger.error("{}: Cannot pull file {} from {}", name, remotePath, node, e);
      dest.delete();
      return false;
    }
    logger.info("{}: remote file {} is pulled at {}", name, remotePath, dest);
    return true;
  }


  @Override
  public void pullSnapshot(PullSnapshotRequest request, AsyncMethodCallback resultHandler) {
    if (character != NodeCharacter.LEADER) {
      // forward the request to the leader
      if (leader != null) {
        logger.debug("{} forwarding a pull snapshot request to the leader {}", name, leader);
        DataClient client = (DataClient) connectNode(leader);
        try {
          client.pullSnapshot(request, new GenericForwardHandler<>(resultHandler));
        } catch (TException e) {
          resultHandler.onError(e);
        }
      } else {
        resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      }
      return;
    }
    if (request.isRequireReadOnly()) {
      setReadOnly();
    }

    // this synchronized should work with the one in AppendEntry when a log is going to commit,
    // which may prevent the newly arrived data from being invisible to the new header.
    synchronized (logManager) {
      List<Integer> requiredSlots = request.getRequiredSlots();
      logger.debug("{}: {} slots are requested", name, requiredSlots.size());

      PullSnapshotResp resp = new PullSnapshotResp();
      Map<Integer, ByteBuffer> resultMap = new HashMap<>();
      logManager.takeSnapshot();

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
   * @param slots
   * @param newNode
   */
  public void pullNodeAdditionSnapshots(List<Integer> slots, Node newNode) {
    synchronized (logManager) {
      logger.info("{} pulling {} slots from remote", name, slots.size());
      PartitionedSnapshot snapshot = (PartitionedSnapshot) logManager.getSnapshot();
      Map<Integer, Node> prevHolders = metaGroupMember.getPartitionTable().getPreviousNodeMap(newNode);
      Map<Node, List<Integer>> holderSlotsMap = new HashMap<>();
      // logger.debug("{}: Holders of each slot: {}", name, prevHolders);

      for (int slot : slots) {
        if (snapshot.getSnapshot(slot) == null) {
          Node node = prevHolders.get(slot);
          if (node != null) {
            holderSlotsMap.computeIfAbsent(node, n -> new ArrayList<>()).add(slot);
          }
        }
      }

      for (Entry<Node, List<Integer>> entry : holderSlotsMap.entrySet()) {
        Node node = entry.getKey();
        List<Integer> nodeSlots = entry.getValue();
        pullFileSnapshot(metaGroupMember.getPartitionTable().getHeaderGroup(node),  nodeSlots, false);
      }
    }
  }

  /**
   *
   * @param prevHolders
   * @param nodeSlots
   * @param requireReadOnly set to true if the previous holder has been removed from the cluster.
   *                       This will make the previous holder read-only so that different new
   *                        replicas can pull the same snapshot.
   */
  private void pullFileSnapshot(PartitionGroup prevHolders,  List<Integer> nodeSlots, boolean requireReadOnly) {
    Future<Map<Integer, FileSnapshot>> snapshotFuture =
        pullSnapshotService.submit(new PullSnapshotTask(prevHolders.getHeader(), nodeSlots, this,
            prevHolders, FileSnapshot::new, requireReadOnly));
    for (int slot : nodeSlots) {
      logManager.setSnapshot(new RemoteFileSnapshot(snapshotFuture), slot);
    }
  }


  public MetaGroupMember getMetaGroupMember() {
    return metaGroupMember;
  }

  TSStatus executeNonQuery(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    }

    return forwardPlan(plan, leader);
  }

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    // try to synchronize with the leader first in case that some schema logs are accepted but
    // not committed yet
    if (!syncLeader()) {
      // if this node cannot synchronize with the leader with in a given time, forward the
      // request to the leader
      AsyncClient client = connectNode(leader);
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
    String prefixPath = request.getPrefixPath();
    List<MeasurementSchema> timeseriesSchemas = new ArrayList<>();
    MManager.getInstance().collectSeries(prefixPath, timeseriesSchemas);

    PullSchemaResp resp = new PullSchemaResp();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeInt(timeseriesSchemas.size());
      for (MeasurementSchema timeseriesSchema : timeseriesSchemas) {
        timeseriesSchema.serializeTo(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable
    }
    resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
    resultHandler.onComplete(resp);
  }

  ManagedSeriesReader getSeriesReaderWithoutValueFilter(Path path, TSDataType dataType, Filter timeFilter,
      QueryContext context, boolean pushdownUnseq)
      throws IOException, StorageEngineException {
    // pull the newest data
    if (syncLeader()) {
      return new SeriesReaderWithoutValueFilter(path, dataType, timeFilter, context, pushdownUnseq);
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

  ManagedSeriesReader getSeriesReaderWithValueFilter(Path path, TSDataType dataType, Filter timeFilter,
      QueryContext context)
      throws IOException, StorageEngineException {
    // pull the newest data
    if (syncLeader()) {
      return new SeriesReaderWithValueFilter(path, dataType, timeFilter, context);
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

  IReaderByTimestamp getReaderByTimestamp(Path path, QueryContext context)
      throws StorageEngineException, IOException {
    if (syncLeader()) {
      return new SeriesReaderByTimestamp(path, context);
    } else {
      throw new StorageEngineException(new LeaderUnknownException(getAllNodes()));
    }
  }

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
    if (request.isSetFilterBytes()) {
      // TODO-Cluster: deserialize the filters
      timeFilter = FilterFactory.deserialize(request.filterBytes);
    }
    RemoteQueryContext queryContext = queryManager.getQueryContext(request.getRequester(),
        request.getQueryId());
    logger.debug("{}: local queryId for {}#{} is {}", name, request.getQueryId(),
        request.getPath(), queryContext.getQueryId());
    try {
      IBatchReader batchReader;
      if (request.isWithValueFilter()) {
        batchReader = getSeriesReaderWithValueFilter(path, dataType, timeFilter, queryContext);
      } else {
        batchReader = getSeriesReaderWithoutValueFilter(path, dataType, timeFilter, queryContext,
            request.isPushdownUnseq());
      }

      long readerId = queryManager.registerReader(batchReader);
      queryContext.registerLocalReader(readerId);

      logger.debug("{}: Build a reader of {} for {}, readerId: {}", name, path,
          request.getRequester(), readerId);
      resultHandler.onComplete(readerId);
    } catch (IOException | StorageEngineException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void querySingleSeriesByTimestamp(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    logger.debug("{}: {} is querying {} by timestamp, queryId: {}", name, request.getRequester(),
        request.getPath(), request.getQueryId());
    if (!syncLeader()) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }

    Path path = new Path(request.getPath());
    RemoteQueryContext queryContext = queryManager.getQueryContext(request.getRequester(),
        request.getQueryId());
    logger.debug("{}: local queryId for {}#{} is {}", name, request.getQueryId(),
        request.getPath(), queryContext.getQueryId());
    try {
      IReaderByTimestamp readerByTimestamp = getReaderByTimestamp(path, queryContext);
      long readerId = queryManager.registerReaderByTime(readerByTimestamp);
      queryContext.registerLocalReader(readerId);

      logger.debug("{}: Build a readerByTimestamp of {} for {}, readerId: {}", name, path,
          request.getRequester(), readerId);
      resultHandler.onComplete(readerId);
    } catch (IOException | StorageEngineException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void endQuery(Node header, Node requester, long queryId,
      AsyncMethodCallback<Void> resultHandler) {
    try {
      queryManager.endQuery(requester, queryId);
      resultHandler.onComplete(null);
    } catch (StorageEngineException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void fetchSingleSeriesByTimestamp(Node header, long readerId, long timestamp,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    IReaderByTimestamp reader = queryManager.getReaderByTimestamp(readerId);
    if (reader == null) {
      resultHandler.onError(new ReaderNotFoundException(readerId));
      return;
    }
    try {
      Object value = reader.getValueInTimestamp(timestamp);
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

  @Override
  public void fetchSingleSeries(Node header, long readerId,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    IBatchReader reader = queryManager.getReader(readerId);
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
        logger.debug("{}: Send results of reader {}, size:{}", name, readerId, batchData.length());
        resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      } else {
        resultHandler.onComplete(ByteBuffer.allocate(0));
      }
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getAllPaths(Node header, String path, AsyncMethodCallback<List<String>> resultHandler) {
    try {
      resultHandler.onComplete(MManager.getInstance().getPaths(path));
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
   * When a node is removed and IT IS NOT THE HEADER of the group, the member should take over
   * some slots from the removed group, and add a new node to the group the removed node was in the
   * group.
   */
  public void removeNode(Node removedNode, NodeRemovalResult removalResult) {
    synchronized (allNodes) {
      if (allNodes.contains(removedNode)) {
        // update the group if the deleted node was in it
        allNodes = metaGroupMember.getPartitionTable().getHeaderGroup(getHeader());
        if (removedNode.equals(leader)) {
          synchronized (term) {
            setCharacter(NodeCharacter.ELECTOR);
            setLastHeartBeatReceivedTime(Long.MIN_VALUE);
          }
        }
      }
      List<Integer> slotsToPull = removalResult.getNewSlotOwners().get(getHeader());
      if (slotsToPull != null) {
        // pull the slots that should be taken over
        pullFileSnapshot(removalResult.getRemovedGroup(), slotsToPull, true);
      }
    }
  }

  public DataMemberReport genReport() {
    return new DataMemberReport(character, leader, term.get(),
        logManager.getLastLogTerm(), logManager.getLastLogIndex(), getHeader(), readOnly);
  }
}

