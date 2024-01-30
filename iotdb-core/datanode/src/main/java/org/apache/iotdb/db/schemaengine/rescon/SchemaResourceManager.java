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

package org.apache.iotdb.db.schemaengine.rescon;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.SchemaEngineMode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.page.PageLifecycleManager;

public class SchemaResourceManager {

  private SchemaResourceManager() {}

  public static void initSchemaResource(ISchemaEngineStatistics engineStatistics) {
    if (CommonDescriptor.getInstance()
        .getConfig()
        .getSchemaEngineMode()
        .equals(SchemaEngineMode.PBTree.toString())) {
      initSchemaFileModeResource(engineStatistics);
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      PageLifecycleManager.getInstance()
          .loadConfiguration(
              config.getPbtreeCachePageNum() + config.getPbtreeBufferPageNum(),
              config.getPbtreeCachePageNum(),
              config.getPbtreeBufferPageNum());
    }
  }

  public static void clearSchemaResource() {
    if (CommonDescriptor.getInstance()
        .getConfig()
        .getSchemaEngineMode()
        .equals(SchemaEngineMode.PBTree.toString())) {
      clearSchemaFileModeResource();
    }
  }

  private static void initSchemaFileModeResource(ISchemaEngineStatistics engineStatistics) {
    ReleaseFlushMonitor.getInstance().init(engineStatistics);
  }

  private static void clearSchemaFileModeResource() {
    ReleaseFlushMonitor.getInstance().clear();
  }
}
