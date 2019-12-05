/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import static org.apache.iotdb.cluster.config.ClusterConstant.CONNECTION_TIME_OUT_MS;

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
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.DataSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTask;
import org.apache.iotdb.cluster.log.snapshot.RemoteDataSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.RemoteFileSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardPullSnapshotHandler;
import org.apache.iotdb.cluster.server.heartbeat.DataHeartBeatThread;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
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

  private DataGroupMember(TProtocolFactory factory, PartitionGroup nodes, Node thisNode,
      PartitionedSnapshotLogManager logManager, MetaGroupMember metaGroupMember) throws IOException {
    super("Data(" + nodes.getHeader().getIp() + ":" + nodes.getHeader().getMetaPort() + ")",
        new ClientPool(new DataClient.Factory(new TAsyncClientManager(), factory)));
    this.thisNode = thisNode;
    this.logManager = logManager;
    super.logManager = logManager;
    this.metaGroupMember = metaGroupMember;
    allNodes = nodes;
    queryProcessor = new QueryProcessor(new QueryProcessExecutor());
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
  }

  /**
   * The first node (on the hash ring) in this data group is the header. It determines the duty
   * (what range on the ring do the group take responsibility for) of the group and although other
   * nodes in this may change, this node is unchangeable unless the data group is dismissed. It
   * is also the identifier of this data group.
   */
  public Node getHeader() {
    return allNodes.get(0);
  }

  public static class Factory {
    private TProtocolFactory protocolFactory;
    private MetaGroupMember metaGroupMember;
    private LogApplier applier;

    Factory(TProtocolFactory protocolFactory, MetaGroupMember metaGroupMember, LogApplier applier) {
      this.protocolFactory = protocolFactory;
      this.metaGroupMember = metaGroupMember;
      this.applier = applier;
    }

    public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode)
        throws IOException {
      return new DataGroupMember(protocolFactory, partitionGroup, thisNode,
          new PartitionedSnapshotLogManager(applier, metaGroupMember.getPartitionTable(),
              partitionGroup.getHeader()), metaGroupMember);
    }
  }

  /**
   * Try to add a Node into this group
   * @param node
   * @return true if this node should leave the group
   */
  public synchronized boolean addNode(Node node) {
    // when a new node is added, start an election instantly to avoid the stale leader still
    // taking the leadership
    synchronized (term) {
      term.incrementAndGet();
      setCharacter(NodeCharacter.ELECTOR);
      leader = null;
      setLastHeartBeatReceivedTime(System.currentTimeMillis());
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
    long thatMetaTerm = electionRequest.getTerm();
    long thatMetaLastLogId = electionRequest.getLastLogIndex();
    long thatMetaLastLogTerm = electionRequest.getLastLogTerm();
    logger.info("{} received an election request, term:{}, metaLastLogId:{}, metaLastLogTerm:{}",
        name, thatMetaTerm, thatMetaLastLogId, thatMetaLastLogTerm);

    long thisMetaLastLogIndex = metaGroupMember.getLogManager().getLastLogIndex();
    long thisMetaLastLogTerm = metaGroupMember.getLogManager().getLastLogTerm();
    long thisMetaTerm = metaGroupMember.getTerm().get();

    long metaResponse = verifyElector(thisMetaTerm, thisMetaLastLogIndex, thisMetaLastLogTerm,
        thatMetaTerm, thatMetaLastLogId, thatMetaLastLogTerm);
    if (metaResponse != Response.RESPONSE_AGREE) {
      return Response.RESPONSE_META_LOG_STALE;
    }

    // check data logs
    long thatDataTerm = electionRequest.getTerm();
    long thatDataLastLogId = electionRequest.getDataLogLastIndex();
    long thatDataLastLogTerm = electionRequest.getDataLogLastTerm();
    logger.info("{} received an election request, term:{}, dataLastLogId:{}, dataLastLogTerm:{}",
        name, thatDataTerm, thatDataLastLogId, thatDataLastLogTerm);

    long resp = verifyElector(term.get(), logManager.getLastLogIndex(),
        logManager.getLastLogTerm(), thatDataTerm, thatDataLastLogId, thatDataLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      term.set(thatDataTerm);
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartBeatReceivedTime = System.currentTimeMillis();
      leader = electionRequest.getElector();
    }
    return resp;
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    PartitionedSnapshot snapshot = new PartitionedSnapshot<>(DataSimpleSnapshot::new);
    try {
      snapshot.deserialize(ByteBuffer.wrap(request.getSnapshotBytes()));
      logger.debug("{} received a snapshot {}", name, snapshot);
      applyPartitionedSnapshot(snapshot);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  public void applySnapshot(Snapshot snapshot, int socket) {
    if (snapshot instanceof DataSimpleSnapshot) {
      applySimpleSnapshot((DataSimpleSnapshot) snapshot, socket);
    } else if (snapshot instanceof FileSnapshot) {
      applyFileSnapshot((FileSnapshot) snapshot, socket);
    } else {
      logger.error("Unrecognized snapshot {}", snapshot);
    }
  }

  private void applySimpleSnapshot(DataSimpleSnapshot snapshot, int socket) {
    synchronized (logManager) {
      for (MeasurementSchema schema : snapshot.getTimeseriesSchemas()) {
        SchemaUtils.registerTimeseries(schema);
      }

      for (Log log : snapshot.getSnapshot()) {
        try {
          logManager.getApplier().apply(log);
        } catch (QueryProcessException e) {
          logger.error("{}: Cannot apply a log {} in snapshot, ignored", name, log, e);
        }
      }
      logManager.setSnapshot(snapshot, socket);
    }
  }

  private void applyFileSnapshot(FileSnapshot snapshot, int socket) {
    synchronized (logManager) {
      for (MeasurementSchema schema : snapshot.getTimeseriesSchemas()) {
        SchemaUtils.registerTimeseries(schema);
      }

      List<RemoteTsFileResource> remoteTsFileResources = snapshot.getDataFiles();
      for (RemoteTsFileResource resource : remoteTsFileResources) {
        loadRemoteFile(resource);
      }
    }
  }

  private void applyPartitionedSnapshot(PartitionedSnapshot snapshot) {
    synchronized (logManager) {
      List<Integer> sockets = metaGroupMember.getPartitionTable().getNodeSockets(getHeader());
      for (Integer socket : sockets) {
        Snapshot subSnapshot = snapshot.getSnapshot(socket);
        if (subSnapshot != null) {
          applySnapshot(subSnapshot, socket);
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
        loadRemoteResource(resource);
        logger.info("{}: Remote file {} is successfully loaded", name, resource);
        return;
      }
    }
    logger.error("{}: Cannot load remote file {} from group {}", name, resource, partitionGroup);
  }

  private void loadRemoteResource(RemoteTsFileResource resource) {
    String[] pathSegments = resource.getFile().getAbsolutePath().split(File.separator);
    int segSize = pathSegments.length;
    String storageGroupName = pathSegments[segSize - 2];
    File remoteModFile =
        new File(resource.getFile().getAbsoluteFile() + ModificationFile.FILE_SUFFIX);
    //TODO-Cluster: load the file resource into a proper storage group processor
    try {
      StorageEngine.getInstance().getProcessor(storageGroupName).loadNewTsFile(resource);
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
    AsyncClient client = connectNode(node);
    if (client == null) {
      return null;
    }
    String[] pathSegments = resource.getFile().getAbsolutePath().split(File.separator);
    int segSize = pathSegments.length;
    // {nodeIdentifier}_{storageGroupName}_{fileName}
    String tempFileName =
        node.getNodeIdentifier() + "_" + pathSegments[segSize - 2] + "_" + pathSegments[segSize - 1];
    File tempFile = new File(REMOTE_FILE_TEMP_DIR, tempFileName);
    File tempModFile = new File(REMOTE_FILE_TEMP_DIR, tempFileName + ModificationFile.FILE_SUFFIX);
    if (pullRemoteFile(resource.getFile().getAbsolutePath(), node, tempFile, client)) {
      if (!checkMd5(tempFile, resource.getMd5())) {
        return null;
      }
      pullRemoteFile(resource.getModFile().getFilePath(), node, tempModFile, client);
      return tempFile;
    }
    return null;
  }

  private boolean checkMd5(File tempFile, byte[] expectedMd5) {
    // TODO-Cluster: implement
    return true;
  }

  private boolean pullRemoteFile(String remotePath, Node node, File dest, AsyncClient client) {
    AtomicReference<ByteBuffer> result = new AtomicReference<>();
    try (BufferedOutputStream bufferedOutputStream =
        new BufferedOutputStream(new FileOutputStream(dest))) {
      int offset = 0;
      int fetchSize = 64 * 1024;
      result.set(null);
      GenericHandler<ByteBuffer> handler = new GenericHandler<>(node, result);

      while (true) {
        synchronized (result) {
          client.readFile(remotePath, offset, fetchSize, handler);
          result.wait(CONNECTION_TIME_OUT_MS);
        }
        ByteBuffer buffer = result.get();
        if (buffer == null || buffer.array().length == 0) {
          break;
        }
        bufferedOutputStream.write(buffer.array());
        offset += buffer.array().length;
      }
      bufferedOutputStream.flush();
    } catch (IOException e) {
      logger.error("{}: Cannot create temp file for {}", name, remotePath);
      return false;
    } catch (TException | InterruptedException e) {
      logger.error("{}: Cannot pull file {} from {}", name, remotePath, node);
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
        AsyncClient client = connectNode(leader);
        try {
          client.pullSnapshot(request, new ForwardPullSnapshotHandler(resultHandler));
        } catch (TException e) {
          resultHandler.onError(e);
        }
      } else {
        resultHandler.onError(new LeaderUnknownException());
      }
      return;
    }

    // this synchronized should work with the one in AppendEntry when a log is going to commit,
    // which may prevent the newly arrived data from being invisible to the new header.
    synchronized (logManager) {
      List<Integer> requiredSockets = request.getRequiredSockets();
      // check whether this socket is held by the node
      List<Integer> heldSockets = metaGroupMember.getPartitionTable().getNodeSockets(getHeader());

      PullSnapshotResp resp = new PullSnapshotResp();
      Map<Integer, ByteBuffer> resultMap = new HashMap<>();
      logManager.takeSnapshot();

      for (int requiredSocket : requiredSockets) {
        if (!heldSockets.contains(requiredSocket)) {
          logger.debug("{}: the required socket {} is not held by the node", name, requiredSocket);
          continue;
        }

        PartitionedSnapshot allSnapshot = (PartitionedSnapshot) logManager.getSnapshot();
        Snapshot snapshot = allSnapshot.getSnapshot(requiredSocket);
        resultMap.put(requiredSocket, snapshot.serialize());
      }
      resp.setSnapshotBytes(resultMap);
      logger.debug("{}: Sending {} snapshots to the requester", name, resultMap.size());
      resultHandler.onComplete(resp);
    }
  }

  public void pullSnapshots(List<Integer> sockets, Node newNode) {
    synchronized (logManager) {
      logger.info("{} pulling {} sockets from remote", name, sockets.size());
      PartitionedSnapshot snapshot = (PartitionedSnapshot) logManager.getSnapshot();
      Map<Integer, Node> prevHolders = metaGroupMember.getPartitionTable().getPreviousNodeMap(newNode);
      Map<Node, List<Integer>> holderSocketsMap = new HashMap<>();
      // logger.debug("{}: Holders of each socket: {}", name, prevHolders);

      for (int socket : sockets) {
        if (snapshot.getSnapshot(socket) == null) {
          Node node = prevHolders.get(socket);
          holderSocketsMap.computeIfAbsent(node, n -> new ArrayList<>()).add(socket);
        }
      }

      for (Entry<Node, List<Integer>> entry : holderSocketsMap.entrySet()) {
        Node node = entry.getKey();
        List<Integer> nodeSockets = entry.getValue();
        pullDataSimpleSnapshot(node, nodeSockets);
      }
    }
  }

  private void pullDataSimpleSnapshot(Node node, List<Integer> nodeSockets) {
    PartitionGroup prevHolders = metaGroupMember.getPartitionTable().getHeaderGroup(node);
    if (prevHolders.contains(thisNode)) {
      // the sockets are already stored locally, skip them.
      return;
    }
    Future<Map<Integer, DataSimpleSnapshot>> snapshotFuture =
        pullSnapshotService.submit(new PullSnapshotTask(node, nodeSockets, this,
            prevHolders, DataSimpleSnapshot::new));
    for (int socket : nodeSockets) {
      logManager.setSnapshot(new RemoteDataSimpleSnapshot(snapshotFuture, socket), socket);
    }
  }

  private void pullFileSnapshot(Node node, List<Integer> nodeSockets) {
    Future<Map<Integer, FileSnapshot>> snapshotFuture =
        pullSnapshotService.submit(new PullSnapshotTask(node, nodeSockets, this,
            metaGroupMember.getPartitionTable().getHeaderGroup(node), FileSnapshot::new));
    for (int socket : nodeSockets) {
      logManager.setSnapshot(new RemoteFileSnapshot(snapshotFuture), socket);
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
        resultHandler.onError(new LeaderUnknownException());
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
}
