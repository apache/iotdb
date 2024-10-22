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

package org.apache.iotdb.db.pipe.receiver.protocol.airgap;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IoTDBAirGapReceiverAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapReceiverAgent.class);

  private final ExecutorService listenExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.PIPE_RECEIVER_AIR_GAP_AGENT.getName());
  private final AtomicBoolean allowSubmitListen = new AtomicBoolean(false);

  private ServerSocket serverSocket;

  private final AtomicLong receiverId = new AtomicLong(0);

  public void listen() {
    try {
      final Socket socket = serverSocket.accept();
      new Thread(new IoTDBAirGapReceiver(socket, receiverId.incrementAndGet())).start();
    } catch (final IOException e) {
      LOGGER.warn("Unhandled exception during pipe air gap receiver listening", e);
    }

    if (allowSubmitListen.get()) {
      listenExecutor.submit(this::listen);
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      serverSocket = new ServerSocket(PipeConfig.getInstance().getPipeAirGapReceiverPort());
    } catch (final IOException e) {
      throw new StartupException(e);
    }

    allowSubmitListen.set(true);
    listenExecutor.submit(this::listen);

    LOGGER.info("IoTDBAirGapReceiverAgent {} started.", serverSocket);
  }

  @Override
  public void stop() {
    try {
      serverSocket.close();
    } catch (final IOException e) {
      LOGGER.warn("Failed to close IoTDBAirGapReceiverAgent's server socket", e);
    }

    allowSubmitListen.set(false);
    listenExecutor.shutdown();

    LOGGER.info("IoTDBAirGapReceiverAgent {} stopped.", serverSocket);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.AIR_GAP_SERVICE;
  }
}
