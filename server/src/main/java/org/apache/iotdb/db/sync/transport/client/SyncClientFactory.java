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
import org.apache.iotdb.commons.sync.pipesink.IoTDBPipeSink;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncClientFactory {

  private static final Logger logger = LoggerFactory.getLogger(SyncClientFactory.class);

  public static ISyncClient createSyncClient(Pipe pipe, PipeSink pipeSink, String dataRegionId) {
    DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(Integer.parseInt(dataRegionId)));
    switch (pipeSink.getType()) {
      case IoTDB:
        IoTDBPipeSink ioTDBPipeSink = (IoTDBPipeSink) pipeSink;
        return new IoTDBSyncClient(
            pipe, ioTDBPipeSink.getIp(), ioTDBPipeSink.getPort(), dataRegion.getDatabaseName());
      case ExternalPipe:
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static ISyncClient createHeartbeatClient(Pipe pipe, PipeSink pipeSink) {
    switch (pipeSink.getType()) {
      case IoTDB:
        IoTDBPipeSink ioTDBPipeSink = (IoTDBPipeSink) pipeSink;
        return new IoTDBSyncClient(pipe, ioTDBPipeSink.getIp(), ioTDBPipeSink.getPort());
      case ExternalPipe:
      default:
        throw new UnsupportedOperationException();
    }
  }
}
