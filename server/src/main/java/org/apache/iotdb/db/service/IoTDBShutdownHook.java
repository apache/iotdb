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
package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryChecker;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.wal.WALManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBShutdownHook extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBShutdownHook.class);

  @Override
  public void run() {
    // close rocksdb if possible to avoid lose data
    if (SchemaEngineMode.valueOf(IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode())
        .equals(SchemaEngineMode.Rocksdb_based)) {
      IoTDB.configManager.clear();
    }

    // reject write operations to make sure all tsfiles will be sealed
    CommonDescriptor.getInstance().getConfig().setNodeStatusToShutdown();
    // wait all wal are flushed
    WALManager.getInstance().waitAllWALFlushed();

    // flush data to Tsfile and remove WAL log files
    if (!IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      StorageEngineV2.getInstance().syncCloseAllProcessor();
    }
    WALManager.getInstance().deleteOutdatedWALFiles();

    if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      // This setting ensures that compaction work is not discarded
      // even if there are frequent restarts
      DataRegionConsensusImpl.getInstance().getAllConsensusGroupIds().parallelStream()
          .forEach(id -> DataRegionConsensusImpl.getInstance().triggerSnapshot(id));
    }

    // clear lock file
    DirectoryChecker.getInstance().deregisterAll();

    if (logger.isInfoEnabled()) {
      logger.info(
          "IoTDB exits. Jvm memory usage: {}",
          MemUtils.bytesCntToStr(
              Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }
  }
}
