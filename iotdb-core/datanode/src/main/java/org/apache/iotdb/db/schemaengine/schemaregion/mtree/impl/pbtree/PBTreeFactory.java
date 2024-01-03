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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.MemoryStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.IMemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.MemoryManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile;

import java.io.File;
import java.io.IOException;

public class PBTreeFactory {

  private static class PBTreeFactoryHolder {
    private static PBTreeFactory INSTANCE = new PBTreeFactory();

    private PBTreeFactoryHolder() {}
  }

  public static PBTreeFactory getInstance() {
    return PBTreeFactoryHolder.INSTANCE;
  }

  private PBTreeFactory() {}

  public CachedMTreeStore createNewCachedMTreeStore(
      PartialPath storageGroup,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      SchemaRegionCachedMetric metric,
      Runnable flushCallback)
      throws MetadataException, IOException {
    SchemaFile schemaFile = SchemaFile.initSchemaFile(storageGroup.getFullPath(), schemaRegionId);
    schemaFile.setMetric(metric);
    return createCachedMTreeStore(
        schemaRegionId, regionStatistics, metric, flushCallback, schemaFile);
  }

  public CachedMTreeStore createCachedMTreeStoreFromSnapshot(
      File snapshotDir,
      String storageGroup,
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      SchemaRegionCachedMetric metric,
      Runnable flushCallback)
      throws MetadataException, IOException {
    SchemaFile schemaFile = SchemaFile.loadSnapshot(snapshotDir, storageGroup, schemaRegionId);
    schemaFile.setMetric(metric);
    return createCachedMTreeStore(
        schemaRegionId, regionStatistics, metric, flushCallback, schemaFile);
  }

  private CachedMTreeStore createCachedMTreeStore(
      int schemaRegionId,
      CachedSchemaRegionStatistics regionStatistics,
      SchemaRegionCachedMetric metric,
      Runnable flushCallback,
      ISchemaFile schemaFile)
      throws MetadataException {
    MemoryStatistics memoryStatistics = new MemoryStatistics(regionStatistics);

    LockManager lockManager = new LockManager();

    IMemoryManager memoryManager = new MemoryManager(memoryStatistics, lockManager);

    regionStatistics.setMemoryManager(memoryManager);

    ReleaseFlushMonitor releaseFlushMonitor = ReleaseFlushMonitor.getInstance();
    CachedMTreeStore cachedMTreeStore =
        new CachedMTreeStore(
            schemaRegionId,
            regionStatistics,
            metric,
            flushCallback,
            schemaFile,
            memoryManager,
            memoryStatistics,
            lockManager);
    releaseFlushMonitor.registerCachedMTreeStore(cachedMTreeStore);
    return cachedMTreeStore;
  }
}
