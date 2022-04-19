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

package org.apache.iotdb.commons;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.io.IOException;
import java.net.ServerSocket;

public class BaseClientTest {

  protected TEndPoint endPoint = new TEndPoint("localhost", 9003);

  private ServerSocket metaServer;
  private Thread metaServerListeningThread;

  public void startServer() throws IOException {
    metaServer = new ServerSocket(9003);
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

  public void stopServer() throws InterruptedException, IOException {
    if (metaServer != null) {
      metaServer.close();
    }
    if (metaServerListeningThread != null) {
      metaServerListeningThread.interrupt();
      metaServerListeningThread.join();
    }
  }
}
