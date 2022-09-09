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
package org.apache.iotdb.db.sync.transport.client;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.sync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class SyncClientFactory {

  private static final Logger logger = LoggerFactory.getLogger(SyncClientFactory.class);

  public static ISyncClient createSyncClient(Pipe pipe, PipeSink pipeSink, String dataRegionId) {
    DataRegion dataRegion =
        StorageEngineV2.getInstance()
            .getDataRegion(new DataRegionId(Integer.parseInt(dataRegionId)));
    switch (pipeSink.getType()) {
      case IoTDB:
        IoTDBPipeSink ioTDBPipeSink = (IoTDBPipeSink) pipeSink;
        return new IoTDBSyncClient(
            pipe,
            ioTDBPipeSink.getIp(),
            ioTDBPipeSink.getPort(),
            getLocalIP(ioTDBPipeSink),
            dataRegion.getStorageGroupName());
      case ExternalPipe:
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static String getLocalIP(IoTDBPipeSink pipeSink) {
    String localIP;
    try {
      InetAddress inetAddress = InetAddress.getLocalHost();
      if (inetAddress.isLoopbackAddress()) {
        try (final DatagramSocket socket = new DatagramSocket()) {
          socket.connect(InetAddress.getByName(pipeSink.getIp()), pipeSink.getPort());
          localIP = socket.getLocalAddress().getHostAddress();
        }
      } else {
        localIP = inetAddress.getHostAddress();
      }
    } catch (UnknownHostException | SocketException e) {
      logger.error("Get local host error when create transport handler.", e);
      localIP = SyncConstant.UNKNOWN_IP;
    }
    return localIP;
  }
}
