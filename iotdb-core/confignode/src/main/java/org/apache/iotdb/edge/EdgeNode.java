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

package org.apache.iotdb.edge;

import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.service.DataNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry point of the IoTDB Edge distribution: starts the ConfigNode and the DataNode services
 * inside ONE JVM process, so that a resource-constrained edge machine only pays a single JVM's
 * fixed overhead (metaspace, code cache, GC structures, thread stacks).
 *
 * <p>The ConfigNode is bootstrapped on a background thread first; once its internal RPC port
 * accepts connections (i.e. the seed ConfigNode finished its consensus initialization), the
 * DataNode is started on the main thread. Both services then keep the JVM alive with their own
 * non-daemon threads. If either node fails fatally, its own error handling terminates the whole
 * process, which is the intended single-process semantics of the edge deployment.
 *
 * <p>Note for launchers: both {@code CONFIGNODE_HOME} and {@code IOTDB_HOME} system properties must
 * point to the installation directory (see {@code sbin/start-edge.sh}), otherwise the ConfigNode
 * resolves its data directories against the process working directory.
 */
public final class EdgeNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdgeNode.class);

  /** Max duration to wait for the ConfigNode internal RPC port to accept connections. */
  private static final long CONFIG_NODE_READY_TIMEOUT_MS = 300_000L;

  private static final long PORT_PROBE_INTERVAL_MS = 500L;

  /** Extra delay after the port opens, leaving time for the leader election to settle. */
  private static final long LEADER_ELECTION_GRACE_MS = 5_000L;

  private EdgeNode() {}

  public static void main(String[] args) throws Exception {
    LOGGER.info(ConfigNodeMessages.LOG_STARTING_IOTDB_EDGE_CONFIGNODE_AND_DATANODE_IN_ONE_77F32605);

    final AtomicReference<Throwable> configNodeError = new AtomicReference<>();
    Thread configNodeThread =
        new Thread(
            () -> {
              try {
                ConfigNode.main(new String[] {"-s"});
              } catch (Throwable t) {
                configNodeError.set(t);
              }
            },
            "EdgeNode-ConfigNode-Bootstrap");
    configNodeThread.start();

    String internalAddress = ConfigNodeDescriptor.getInstance().getConf().getInternalAddress();
    int internalPort = ConfigNodeDescriptor.getInstance().getConf().getInternalPort();
    waitPortOpen(internalAddress, internalPort, configNodeError);
    throwIfConfigNodeBootstrapFailed(configNodeError);
    Thread.sleep(LEADER_ELECTION_GRACE_MS);
    throwIfConfigNodeBootstrapFailed(configNodeError);
    LOGGER.info(ConfigNodeMessages.LOG_IOTDB_EDGE_CONFIGNODE_IS_READY_STARTING_DATANODE_6729159E);

    // DataNode.main returns after a successful start; the services of both nodes keep the JVM
    // alive with non-daemon threads afterwards.
    DataNode.main(new String[] {"-s"});
  }

  private static void throwIfConfigNodeBootstrapFailed(AtomicReference<Throwable> configNodeError) {
    Throwable error = configNodeError.get();
    if (error != null) {
      throw new IllegalStateException(
          ConfigNodeMessages.EXCEPTION_IOTDB_EDGE_CONFIGNODE_BOOTSTRAP_FAILED_02EEE59A, error);
    }
  }

  private static void waitPortOpen(
      String address, int port, AtomicReference<Throwable> configNodeError)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + CONFIG_NODE_READY_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (configNodeError.get() != null) {
        return;
      }
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(address, port), 1000);
        return;
      } catch (Exception e) {
        Thread.sleep(PORT_PROBE_INTERVAL_MS);
      }
    }
    throw new IllegalStateException(
        String.format(
            ConfigNodeMessages
                .EXCEPTION_IOTDB_EDGE_CONFIGNODE_INTERNAL_PORT_ARG_IS_NOT_READY_WITHIN_03697FF5,
            port,
            CONFIG_NODE_READY_TIMEOUT_MS));
  }
}
