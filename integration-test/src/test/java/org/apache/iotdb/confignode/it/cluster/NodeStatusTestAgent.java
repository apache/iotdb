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
package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.request.AsyncRequestManager;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.utils.function.CheckedTriConsumer;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.ratis.utils.Utils;

import com.google.common.collect.ImmutableMap;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.thrift.TException;

import java.io.OutputStream;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Loaded only into test JVMs. No production endpoint or production bytecode is changed. */
public final class NodeStatusTestAgent {
  private NodeStatusTestAgent() {}

  public static void agentmain(String arguments, Instrumentation ignored) {
    String[] parts = arguments.split("\\|", 2);
    String command = parts[0];
    Path output = Paths.get(parts[1]);
    // Return from attach immediately even if a consensus write needs to time out.
    Thread worker = new Thread(() -> execute(command, output), "node-status-test-control");
    worker.setDaemon(true);
    worker.start();
  }

  private static void execute(String command, Path output) {
    Properties result = new Properties();
    try {
      String[] args = command.split(":");
      if ("shutdown".equals(args[0])) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> store(output, result)));
        System.exit(0);
        return;
      }
      ConfigManager manager = ConfigNode.getInstance().getConfigManager();
      ConsensusManager consensus = manager.getConsensusManager();
      switch (args[0]) {
        case "snapshot":
          consensus.getConsensusImpl().triggerSnapshot(consensus.getConsensusGroupId(), true);
          break;
        case "barrier":
          installRemovalBarrier(output);
          break;
        case "partition":
          String isolated =
              "*".equals(args[1])
                  ? "*"
                  : Utils.fromNodeIdToRaftPeerId(Integer.parseInt(args[1])).toString();
          for (String point :
              new String[] {"requestVote", "appendEntries", "startLeaderElection"}) {
            CodeInjectionForTesting.put(
                "RaftServerImpl." + point,
                (local, remote, values) -> {
                  if ("*".equals(isolated)
                      || isolated.equals(String.valueOf(local))
                      || isolated.equals(String.valueOf(remote))) {
                    // Fail at receipt, before term/log processing. This drops Raft messages in both
                    // directions while leaving the JVM and ConfigNode business RPC server alive.
                    throw new IllegalStateException("Injected Raft message partition");
                  }
                  return false;
                });
          }
          break;
        case "heal":
          for (String point :
              new String[] {"requestVote", "appendEntries", "startLeaderElection"}) {
            CodeInjectionForTesting.remove("RaftServerImpl." + point);
          }
          break;
        case "transfer":
          consensus
              .getConsensusImpl()
              .transferLeader(
                  consensus.getConsensusGroupId(),
                  new Peer(
                      consensus.getConsensusGroupId(),
                      Integer.parseInt(args[1]),
                      new TEndPoint("127.0.0.1", Integer.parseInt(args[2]))));
          break;
        case "update":
          int id = Integer.parseInt(args[1]);
          NodeStatus status = NodeStatus.parse(args[2]);
          TSStatus response = manager.getLoadManager().trySetNodeStatus(id, status, true);
          result.setProperty("code", Integer.toString(response.getCode()));
          break;
        case "inspect":
          NodeInfo nodeInfo = manager.getNodeManager().getNodeInfo();
          for (int i = 1; i < args.length; i++) {
            int nodeId = Integer.parseInt(args[i]);
            result.setProperty(
                "persisted." + nodeId, String.valueOf(nodeInfo.getPersistedNodeStatus(nodeId)));
          }
          break;
        default:
          throw new IllegalArgumentException(command);
      }
      result.setProperty(
          "term",
          Long.toString(
              consensus.getConsensusImpl().getLogicalClock(consensus.getConsensusGroupId())));
      result.setProperty("leader", Boolean.toString(consensus.isLeader()));
    } catch (Throwable e) {
      result.setProperty("error", e.toString());
    }
    store(output, result);
  }

  private static void store(Path output, Properties result) {
    try {
      Path temporary = output.resolveSibling(output.getFileName() + ".tmp");
      try (OutputStream stream = Files.newOutputStream(temporary)) {
        result.store(stream, "Node status test control");
      }
      Files.move(temporary, output);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void installRemovalBarrier(Path output) throws Exception {
    CnToDnInternalServiceAsyncRequestManager requests =
        CnToDnInternalServiceAsyncRequestManager.getInstance();
    Field field = AsyncRequestManager.class.getDeclaredField("actionMap");
    field.setAccessible(true);
    Map actions = new HashMap((Map) field.get(requests));
    CheckedTriConsumer<Object, Object, Object, TException> original =
        (CheckedTriConsumer<Object, Object, Object, TException>)
            actions.get(CnToDnAsyncRequestType.CLEAN_DATA_NODE_CACHE);
    Path reached = Paths.get(output + ".reached");
    Path release = Paths.get(output + ".release");
    CheckedTriConsumer<Object, Object, Object, TException> gated =
        (request, client, callback) -> {
          if (!Files.exists(reached)) {
            store(reached, new Properties());
          }
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
          while (!Files.exists(release)) {
            if (System.nanoTime() >= deadline) {
              throw new TException("Removal test barrier timed out");
            }
            try {
              TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new TException(e);
            }
          }
          try {
            original.accept(request, client, callback);
          } catch (Exception e) {
            throw new TException(e);
          }
        };
    actions.put(CnToDnAsyncRequestType.CLEAN_DATA_NODE_CACHE, gated);
    field.set(requests, ImmutableMap.copyOf(actions));
  }
}
