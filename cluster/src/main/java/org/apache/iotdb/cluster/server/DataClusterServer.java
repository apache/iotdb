/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.exception.NoHeaderVNodeException;
import org.apache.iotdb.cluster.exception.NotInSameGroupException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataClusterServer extends RaftServer implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataClusterServer.class);

  // key: the header of a data group, the member representing this node in this group
  private Map<Node, DataGroupMember> headerGroupMap = new ConcurrentHashMap<>();
  private int port;
  private PartitionTable partitionTable;
  private DataGroupMember.Factory dataMemberFactory;



  public DataClusterServer(int port, DataGroupMember.Factory dataMemberFactory) {
    super();
    this.port = port;
    this.dataMemberFactory = dataMemberFactory;
  }

  public void addDataGroupMember(DataGroupMember dataGroupMember) {
    headerGroupMap.put(dataGroupMember.getHeader(), dataGroupMember);
  }

  private DataGroupMember getDataMember(Node header) {
    // avoid creating two members for a header
    synchronized (headerGroupMap) {
      DataGroupMember member = headerGroupMap.get(header);
      if (member == null) {
        logger.info("Received a request from unregistered header {}", header);
        if (thisNode != null && partitionTable != null) {
          synchronized (partitionTable) {
            // it may be that the header and this node are in the same group, but it is the first time
            // the header contacts this node
            PartitionGroup partitionGroup = partitionTable.getHeaderGroup(header);
            if (partitionGroup.contains(thisNode)) {
              // the two nodes are in the same group, create a new data member
              try {
                member = dataMemberFactory.create(partitionGroup, thisNode);
                headerGroupMap.put(header, member);
                logger.info("Created a member for header {}", header);
              } catch (IOException e) {
                logger.error("Cannot create data member for header {}", header, e);
              }
            } else {
              logger.info("This node {} does not belong to the group {}", thisNode, partitionGroup);
            }
          }
        } else {
          logger.info("Partition is not ready, cannot create member");
        }
      }
      return member;
    }
  }

  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    if (!request.isSetHeader()) {
      resultHandler.onError(new NoHeaderVNodeException());
      return;
    }

    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header);
    if (member == null) {
      if (partitionTable != null) {
        resultHandler.onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode));
      } else {
        resultHandler.onError(new NotInSameGroupException(Collections.singletonList(header),
            thisNode));
      }
    } else {
      member.sendHeartBeat(request, resultHandler);
    }
  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    if (!electionRequest.isSetHeader()) {
      resultHandler.onError(new NoHeaderVNodeException());
      return;
    }

    Node header = electionRequest.getHeader();
    DataGroupMember member = getDataMember(header);
    if (member == null) {
      if (partitionTable != null) {
        resultHandler.onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode));
      } else {
        resultHandler.onError(new NotInSameGroupException(Collections.singletonList(header),
            thisNode));
      }
    }  else {
      member.startElection(electionRequest, resultHandler);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    if (!request.isSetHeader()) {
      resultHandler.onError(new NoHeaderVNodeException());
      return;
    }

    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header);
    if (member == null) {
      if (partitionTable != null) {
        resultHandler.onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode));
      } else {
        resultHandler.onError(new NotInSameGroupException(Collections.singletonList(header),
            thisNode));
      }
    }  else {
      member.appendEntries(request, resultHandler);
    }
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    if (!request.isSetHeader()) {
      resultHandler.onError(new NoHeaderVNodeException());
      return;
    }

    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header);
    if (member == null) {
      if (partitionTable != null) {
        resultHandler.onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode));
      } else {
        resultHandler.onError(new NotInSameGroupException(Collections.singletonList(header),
            thisNode));
      }
    }  else {
      member.appendEntry(request, resultHandler);
    }
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    if (!request.isSetHeader()) {
      resultHandler.onError(new NoHeaderVNodeException());
      return;
    }

    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header);
    if (member == null) {
      if (partitionTable != null) {
        resultHandler.onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode));
      } else {
        resultHandler.onError(new NotInSameGroupException(Collections.singletonList(header),
            thisNode));
      }
    }  else {
      member.sendSnapshot(request, resultHandler);
    }
  }

  @Override
  public void pullSnapshot(PullSnapshotRequest request, AsyncMethodCallback resultHandler) {
    if (!request.isSetHeader()) {
      resultHandler.onError(new NoHeaderVNodeException());
      return;
    }

    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header);
    if (member == null) {
      if (partitionTable != null) {
        resultHandler.onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode));
      } else {
        resultHandler.onError(new NotInSameGroupException(Collections.singletonList(header),
            thisNode));
      }
    }  else {
      member.pullSnapshot(request, resultHandler);
    }
  }

  @Override
  AsyncProcessor getProcessor() {
    return new AsyncProcessor(this);
  }

  @Override
  TNonblockingServerSocket getServerSocket() throws TTransportException {
    return new TNonblockingServerSocket(new InetSocketAddress(config.getLocalIP(),
        port), CONNECTION_TIME_OUT_MS);
  }

  @Override
  String getClientThreadPrefix() {
    return "DataClientThread-";
  }

  @Override
  String getServerClientName() {
    return "DataServerThread-";
  }

  public void addNode(Node node) {
    Iterator<Entry<Node, DataGroupMember>> entryIterator = headerGroupMap.entrySet().iterator();
    synchronized (headerGroupMap) {
      while (entryIterator.hasNext()) {
        Entry<Node, DataGroupMember> entry = entryIterator.next();
        DataGroupMember dataGroupMember = entry.getValue();
        boolean shouldLeave = dataGroupMember.addNode(node);
        if (shouldLeave) {
          entryIterator.remove();
          dataGroupMember.stop();
        }
      }
    }
  }

  public void setPartitionTable(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
  }

  public Collection<Node> getAllHeaders() {
    return headerGroupMap.keySet();
  }

  public void pullSnapshots() {
    List<Integer> sockets = partitionTable.getNodeSockets(thisNode);
    DataGroupMember dataGroupMember = getDataMember(thisNode);
    dataGroupMember.pullSnapshots(sockets, thisNode);
  }
}
