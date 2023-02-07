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

package org.apache.iotdb.db.metadata.rescon;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheMemoryManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManagerHolder;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;

public class SchemaResourceManager {

  private SchemaResourceManager() {}

  public static void initSchemaResource() {
    MetricService.getInstance()
        .addMetricSet(
            new SchemaResourceManagerMetrics(
                SchemaStatisticsManager.getInstance(), MemoryStatistics.getInstance()));
    MemoryStatistics.getInstance().init();
    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaEngineMode()
        .equals(SchemaEngineMode.Schema_File.toString())) {
      initSchemaFileModeResource();
    }
  }

  public static void clearSchemaResource() {
    SchemaStatisticsManager.getInstance().clear();
    MemoryStatistics.getInstance().clear();
    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaEngineMode()
        .equals(SchemaEngineMode.Schema_File.toString())) {
      clearSchemaFileModeResource();
    }
  }

  private static void initSchemaFileModeResource() {
    MemManagerHolder.initMemManagerInstance();
    MemManagerHolder.getMemManagerInstance().init();
    CacheMemoryManager.getInstance().init();
  }

  private static void clearSchemaFileModeResource() {
    MemManagerHolder.getMemManagerInstance().clear();
    CacheMemoryManager.getInstance().clear();
  }
}
