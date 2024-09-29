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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class TableTTLTest {
  private String databaseName = "db";
  private DataRegionId dataRegionId1 = new DataRegionId(1);
  private long ttl = 10000;
  private long prevPartitionInterval;
  private DataRegion dataRegion;

  @Before
  public void setup() throws DataRegionException {
    prevPartitionInterval = CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();
    CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(86400000);
    EnvironmentUtils.envSetUp();
    dataRegion =
        new DataRegion(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            String.valueOf(dataRegionId1.getId()),
            new TsFileFlushPolicy.DirectFlushPolicy(),
            databaseName);
  }

  @After
  public void tearDown() throws StorageEngineException, IOException {
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    dataRegion.abortCompaction();
    EnvironmentUtils.cleanEnv();
    CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(prevPartitionInterval);
    DataNodeTableCache.getInstance().invalid(databaseName);
  }

  private TsTable createTable(String name, long ttl) {
    TsTable tsTable = new TsTable(name);
    tsTable.addProp(TsTable.TTL_PROPERTY, String.valueOf(ttl));
    DataNodeTableCache.getInstance().preUpdateTable(databaseName, tsTable);
    DataNodeTableCache.getInstance().commitUpdateTable(databaseName, name);
    return tsTable;
  }
}
