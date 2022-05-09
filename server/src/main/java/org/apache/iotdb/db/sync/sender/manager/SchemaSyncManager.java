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
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;

/**
 * SchemaSyncManager is designed to collect history metadata(i.e. all storage group and all
 * timeseries in IoTDB), and realtime metadata(i.e. create storage group, timeseries and delete
 * timeseries operations) for all registered {@linkplain TsFilePipe}.
 */
public class SchemaSyncManager {

  private static final Logger logger = LoggerFactory.getLogger(SchemaSyncManager.class);

  private TsFilePipe syncPipe = null;

  private static class SchemaSyncManagerHolder {

    private SchemaSyncManagerHolder() {
      // allowed to do nothing
    }

    private static final SchemaSyncManager INSTANCE = new SchemaSyncManager();
  }

  public static SchemaSyncManager getInstance() {
    return SchemaSyncManagerHolder.INSTANCE;
  }

  public void registerSyncTask(TsFilePipe syncPipe) {
    this.syncPipe = syncPipe;
  }

  public void deregisterSyncTask() {
    this.syncPipe = null;
  }

  public boolean isEnableSync() {
    return syncPipe != null;
  }

  public void syncMetadataPlan(PhysicalPlan plan) {
    syncPipe.collectRealTimeMetaData(plan);
  }

  public void clear() {
    this.syncPipe = null;
  }

  /** only support for SchemaRegion */
  public List<PhysicalPlan> collectHistoryMetadata() {
    List<PhysicalPlan> historyMetadata = new ArrayList<>();
    List<SetStorageGroupPlan> storageGroupPlanList = getStorageGroupAsPlan();
    for (SetStorageGroupPlan storageGroupPlan : storageGroupPlanList) {
      historyMetadata.add(storageGroupPlan);
    }

    for (ISchemaRegion schemaRegion : SchemaEngine.getInstance().getAllSchemaRegions()) {
      try {
        for (MeasurementPath measurementPath :
            schemaRegion.getMeasurementPaths(new PartialPath(ALL_RESULT_NODES), false)) {
          if (measurementPath.isUnderAlignedEntity()) {
            historyMetadata.add(
                new CreateAlignedTimeSeriesPlan(
                    measurementPath.getDevicePath(),
                    measurementPath.getMeasurement(),
                    (MeasurementSchema) measurementPath.getMeasurementSchema()));
          } else {
            historyMetadata.add(
                new CreateTimeSeriesPlan(
                    measurementPath, (MeasurementSchema) measurementPath.getMeasurementSchema()));
          }
        }
      } catch (MetadataException e) {
        logger.warn(
            String.format(
                "Collect history schema from schemaRegion: %s of sg %s error. Skip this schemaRegion.",
                schemaRegion.getSchemaRegionId(), schemaRegion.getStorageGroupFullPath()));
      }
    }

    return historyMetadata;
  }

  private List<SetStorageGroupPlan> getStorageGroupAsPlan() {
    List<PartialPath> allStorageGroups = IoTDB.configManager.getAllStorageGroupPaths();
    List<SetStorageGroupPlan> result = new LinkedList<>();
    for (PartialPath sgPath : allStorageGroups) {
      result.add(new SetStorageGroupPlan(sgPath));
    }
    return result;
  }

  public DeleteTimeSeriesPlan splitDeleteTimeseriesPlanByDevice(PartialPath pathPattern)
      throws MetadataException {
    return new DeleteTimeSeriesPlan(splitPathPatternByDevice(pathPattern));
  }

  public List<PartialPath> splitPathPatternByDevice(PartialPath pathPattern)
      throws MetadataException {
    Set<PartialPath> devices = IoTDB.schemaProcessor.getBelongedDevices(pathPattern);
    List<PartialPath> resultPathPattern = new LinkedList<>();
    for (PartialPath device : devices) {
      pathPattern.alterPrefixPath(device).stream()
          .filter(i -> !i.equals(device))
          .forEach(resultPathPattern::add);
    }
    return resultPathPattern;
  }
}
