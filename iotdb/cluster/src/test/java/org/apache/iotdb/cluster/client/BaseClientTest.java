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

package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.ClientUtils;

import java.io.IOException;
import java.net.ServerSocket;

public class BaseClientTest {

  protected Node defaultNode = constructDefaultNode();

  private ServerSocket metaServer;
  private Thread metaServerListeningThread;

  private ServerSocket dataServer;
  private Thread dataServerListeningThread;

  private ServerSocket metaHeartbeatServer;
  private Thread metaHeartbeatServerListeningThread;

  private ServerSocket dataHeartbeatServer;
  private Thread dataHeartbeatServerListeningThread;

  public void startMetaServer() throws IOException {
    metaServer = new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.META));
    metaServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  metaServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    metaServerListeningThread.start();
  }

  public void startDataServer() throws IOException {
    dataServer = new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.DATA));
    dataServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  dataServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    dataServerListeningThread.start();
  }

  public void startMetaHeartbeatServer() throws IOException {
    metaHeartbeatServer =
        new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.META_HEARTBEAT));
    metaHeartbeatServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  metaHeartbeatServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    metaHeartbeatServerListeningThread.start();
  }

  public void startDataHeartbeatServer() throws IOException {
    dataHeartbeatServer =
        new ServerSocket(ClientUtils.getPort(defaultNode, ClientCategory.DATA_HEARTBEAT));
    dataHeartbeatServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  dataHeartbeatServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    dataHeartbeatServerListeningThread.start();
  }

  public void stopMetaServer() throws InterruptedException, IOException {
    if (metaServer != null) {
      metaServer.close();
    }
    if (metaServerListeningThread != null) {
      metaServerListeningThread.interrupt();
      metaServerListeningThread.join();
    }
  }

  public void stopDataServer() throws IOException, InterruptedException {
    if (dataServer != null) {
      dataServer.close();
    }
    if (dataServerListeningThread != null) {
      dataServerListeningThread.interrupt();
      dataServerListeningThread.join();
    }
  }

  public void stopMetaHeartbeatServer() throws IOException, InterruptedException {
    if (metaHeartbeatServer != null) {
      metaHeartbeatServer.close();
    }
    if (metaHeartbeatServerListeningThread != null) {
      metaHeartbeatServerListeningThread.interrupt();
      metaHeartbeatServerListeningThread.join();
    }
  }

  public void stopDataHeartbeatServer() throws IOException, InterruptedException {
    if (dataHeartbeatServer != null) {
      dataHeartbeatServer.close();
    }
    if (dataHeartbeatServerListeningThread != null) {
      dataHeartbeatServerListeningThread.interrupt();
      dataHeartbeatServerListeningThread.join();
    }
  }

  public Node constructDefaultNode() {
    Node node = new Node();
    node.setMetaPort(9003).setInternalIp("localhost").setClientIp("localhost");
    node.setDataPort(40010);
    return node;
  }
}
