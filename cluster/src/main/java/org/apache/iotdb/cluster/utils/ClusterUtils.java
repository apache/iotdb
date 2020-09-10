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

package org.apache.iotdb.cluster.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterUtils {

  private static final Logger logger = LoggerFactory.getLogger(ClusterUtils.class);

  public static final int WAIT_START_UP_CHECK_TIME_SEC = 5;

  public static final long START_UP_TIME_THRESHOLD_MS = 60 * 1000L;

  public static final long START_UP_CHECK_TIME_INTERVAL_MS = 3 * 1000L;

  /**
   * the data group member's heartbeat offset relative to the {@link ClusterConfig#getInternalDataPort()},
   * which means the dataHeartbeatPort = getInternalDataPort() + DATA_HEARTBEAT_OFFSET.
   */
  public static final int DATA_HEARTBEAT_PORT_OFFSET = 1;

  /**
   * the meta group member's heartbeat offset relative to the {@link ClusterConfig#getInternalMetaPort()}
   * ()}, which means the metaHeartbeatPort = getInternalMetaPort() + META_HEARTBEAT_OFFSET.
   */
  public static final int META_HEARTBEAT_PORT_OFFSET = 1;

  private ClusterUtils() {
    // util class
  }

  public static CheckStatusResponse checkStatus(StartUpStatus remoteStartUpStatus,
      StartUpStatus localStartUpStatus) {
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    boolean seedNodeListEquals = true;
    boolean clusterNameEqual = true;

    if (localStartUpStatus.getPartitionInterval() != remoteStartUpStatus.getPartitionInterval()) {
      partitionIntervalEquals = false;
      logger.info("Remote partition interval conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getPartitionInterval(), remoteStartUpStatus.getPartitionInterval());
    }
    if (localStartUpStatus.getHashSalt() != remoteStartUpStatus.getHashSalt()) {
      hashSaltEquals = false;
      logger.info("Remote hash salt conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getHashSalt(), remoteStartUpStatus.getHashSalt());
    }
    if (localStartUpStatus.getReplicationNumber() != remoteStartUpStatus.getReplicationNumber()) {
      replicationNumEquals = false;
      logger.info("Remote replication number conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getReplicationNumber(), remoteStartUpStatus.getReplicationNumber());
    }
    if (!Objects
        .equals(localStartUpStatus.getClusterName(), remoteStartUpStatus.getClusterName())) {
      clusterNameEqual = false;
      logger.info("Remote cluster name conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getClusterName(), remoteStartUpStatus.getClusterName());
    }
    if (!ClusterUtils
        .checkSeedNodes(false, localStartUpStatus.getSeedNodeList(),
            remoteStartUpStatus.getSeedNodeList())) {
      seedNodeListEquals = false;
      if (logger.isInfoEnabled()) {
        logger.info("Remote seed node list conflicts with local. local: {}, remote: {}",
            localStartUpStatus.getSeedNodeList(), remoteStartUpStatus.getSeedNodeList());
      }
    }

    return new CheckStatusResponse(partitionIntervalEquals, hashSaltEquals,
        replicationNumEquals, seedNodeListEquals, clusterNameEqual);
  }

  public static boolean checkSeedNodes(boolean isClusterEstablished, List<Node> localSeedNodes,
      List<Node> remoteSeedNodes) {
    return isClusterEstablished ? seedNodesContains(localSeedNodes, remoteSeedNodes)
        : seedNodesEquals(localSeedNodes, remoteSeedNodes);
  }

  private static boolean seedNodesEquals(List<Node> thisNodeList, List<Node> thatNodeList) {
    Node[] thisNodeArray = thisNodeList.toArray(new Node[0]);
    Node[] thatNodeArray = thatNodeList.toArray(new Node[0]);
    Arrays.sort(thisNodeArray, ClusterUtils::compareSeedNode);
    Arrays.sort(thatNodeArray, ClusterUtils::compareSeedNode);
    if (thisNodeArray.length != thatNodeArray.length) {
      return false;
    } else {
      for (int i = 0; i < thisNodeArray.length; i++) {
        if (compareSeedNode(thisNodeArray[i], thatNodeArray[i]) != 0) {
          return false;
        }
      }
      return true;
    }
  }

  private static int compareSeedNode(Node thisSeedNode, Node thatSeedNode) {
    int ipCompare = thisSeedNode.getIp().compareTo(thatSeedNode.getIp());
    if (ipCompare != 0) {
      return ipCompare;
    } else {
      int metaPortCompare = thisSeedNode.getMetaPort() - thatSeedNode.getMetaPort();
      if (metaPortCompare != 0) {
        return metaPortCompare;
      } else {
        return thisSeedNode.getDataPort() - thatSeedNode.getDataPort();
      }
    }
  }

  private static boolean seedNodesContains(List<Node> seedNodeList, List<Node> subSeedNodeList) {
    // Because identifier is not compared here, List.contains() is not suitable
    if (subSeedNodeList == null) {
      return false;
    }
    seedNodeList.sort(ClusterUtils::compareSeedNode);
    subSeedNodeList.sort(ClusterUtils::compareSeedNode);
    int i = 0;
    int j = 0;
    while (i < seedNodeList.size() && j < subSeedNodeList.size()) {
      int compareResult = compareSeedNode(seedNodeList.get(i), subSeedNodeList.get(j));
      if (compareResult > 0) {
        if (logger.isInfoEnabled()) {
          logger.info("Node {} not found in cluster", subSeedNodeList.get(j));
        }
        return false;
      } else if (compareResult < 0) {
        i++;
      } else {
        j++;
      }
    }
    return j == subSeedNodeList.size();
  }

  public static void examineCheckStatusResponse(CheckStatusResponse response,
      AtomicInteger consistentNum, AtomicInteger inconsistentNum, Node seedNode) {
    boolean partitionIntervalEquals = response.partitionalIntervalEquals;
    boolean hashSaltEquals = response.hashSaltEquals;
    boolean replicationNumEquals = response.replicationNumEquals;
    boolean seedNodeListEquals = response.seedNodeEquals;
    boolean clusterNameEqual = response.clusterNameEquals;
    if (!partitionIntervalEquals) {
      logger.info(
          "Local partition interval conflicts with seed node[{}].", seedNode);
    }
    if (!hashSaltEquals) {
      logger
          .info("Local hash salt conflicts with seed node[{}]", seedNode);
    }
    if (!replicationNumEquals) {
      logger.info(
          "Local replication number conflicts with seed node[{}]", seedNode);
    }
    if (!seedNodeListEquals) {
      logger.info("Local seed node list conflicts with seed node[{}]", seedNode);
    }
    if (!clusterNameEqual) {
      logger.info("Local cluster name conflicts with seed node[{}]", seedNode);
    }
    if (partitionIntervalEquals
        && hashSaltEquals
        && replicationNumEquals
        && seedNodeListEquals
        && clusterNameEqual) {
      consistentNum.incrementAndGet();
    } else {
      inconsistentNum.incrementAndGet();
    }
  }

  public static boolean analyseStartUpCheckResult(int consistentNum, int inconsistentNum,
      int totalSeedNum) throws ConfigInconsistentException {
    if (consistentNum == totalSeedNum) {
      // break the loop and establish the cluster
      return true;
    } else if (inconsistentNum == 0) {
      // can't connect other nodes, try in next turn
      return false;
    } else {
      // find config InConsistence, stop building cluster
      throw new ConfigInconsistentException();
    }
  }

  public static TServer createTThreadPoolServer(TServerTransport socket,
      String clientThreadPrefix, TProcessor processor, TProtocolFactory protocolFactory) {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    TThreadPoolServer.Args poolArgs =
        new TThreadPoolServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(CommonUtils.getCpuCores());

    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.stopTimeoutVal, poolArgs.stopTimeoutUnit,
        new SynchronousQueue<>(), new ThreadFactory() {
      private AtomicLong threadIndex = new AtomicLong(0);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, clientThreadPrefix + threadIndex.incrementAndGet());
      }
    }));
    poolArgs.processor(processor);
    poolArgs.protocolFactory(protocolFactory);
    // async service requires FramedTransport
    poolArgs.transportFactory(new TFastFramedTransport.Factory(
        IoTDBDescriptor.getInstance().getConfig().getThriftInitBufferSize(),
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize()));

    // run the thrift server in a separate thread so that the main thread is not blocked
    return new TThreadPoolServer(poolArgs);
  }

  public static Node parseNode(String nodeUrl) {
    Node result = new Node();
    String[] split = nodeUrl.split(":");
    if (split.length != 3) {
      logger.warn("Bad seed url: {}", nodeUrl);
      return null;
    }
    String ip = split[0];
    try {
      int metaPort = Integer.parseInt(split[1]);
      int dataPort = Integer.parseInt(split[2]);
      result.setIp(ip);
      result.setMetaPort(metaPort);
      result.setDataPort(dataPort);
    } catch (NumberFormatException e) {
      logger.warn("Bad seed url: {}", nodeUrl);
    }
    return result;
  }
}
