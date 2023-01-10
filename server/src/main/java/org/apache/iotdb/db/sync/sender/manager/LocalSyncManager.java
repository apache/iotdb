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
package org.apache.iotdb.db.sync.sender.manager;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class LocalSyncManager implements ISyncManager {

  private TsFilePipe syncPipe;
  private final DataRegion dataRegion;
  private final String dataRegionId;

  public LocalSyncManager(DataRegion dataRegion, Pipe pipe) {
    this.dataRegion = dataRegion;
    this.dataRegionId = dataRegion.getDataRegionId();
    this.syncPipe = (TsFilePipe) pipe;
  }

  /** tsfile */
  @Override
  public void syncRealTimeDeletion(Deletion deletion) {
    syncPipe.collectRealTimeDeletion(deletion, dataRegion.getDatabaseName(), dataRegionId);
  }

  @Override
  public void syncRealTimeTsFile(File tsFile) {
    syncPipe.collectRealTimeTsFile(tsFile, dataRegionId);
  }

  @Override
  public void syncRealTimeResource(File tsFile) {
    syncPipe.collectRealTimeResource(tsFile);
  }

  @Override
  public List<File> syncHistoryTsFile(long dataStartTime) {
    return new ArrayList<>(this.dataRegion.collectHistoryTsFileForSync(this, dataStartTime));
  }

  @Override
  public File createHardlink(File tsFile, long modsOffset) {
    return syncPipe.createHistoryTsFileHardlink(tsFile, modsOffset);
  }

  @Override
  public void delete() {
    // TODO(sync): parse to delete operation and sync
    // 1、get timeseries
    // 2、get time partition
    // 3、syncPipe.collectRealTimeDeletion();
  }

  public static List<PartialPath> splitPathPatternByDevice(PartialPath pathPattern)
      throws MetadataException {
    //    Set<PartialPath> devices =
    // LocalSchemaProcessor.getInstance().getBelongedDevices(pathPattern);
    //    List<PartialPath> resultPathPattern = new LinkedList<>();
    //    for (PartialPath device : devices) {
    //      pathPattern.alterPrefixPath(device).stream()
    //          .filter(i -> !i.equals(device))
    //          .forEach(resultPathPattern::add);
    //    }
    //    return resultPathPattern;
    throw new UnsupportedOperationException();
  }
}
