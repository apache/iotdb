/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.writelog.recover;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogReplayer finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the
 * WALs from the logNode and redoes them into a given MemTable and ModificationFile.
 */
public class LogReplayer {

  private Logger logger = LoggerFactory.getLogger(LogReplayer.class);
  private String logNodePrefix;
  private String insertFilePath;
  private ModificationFile modFile;
  private VersionController versionController;
  private TsFileResource currentTsFileResource;
  private IMemTable recoverMemTable;

  // only unsequence file tolerates duplicated data
  private boolean sequence;

  private Map<String, Long> tempStartTimeMap = new HashMap<>();
  private Map<String, Long> tempEndTimeMap = new HashMap<>();

  public LogReplayer(String logNodePrefix, String insertFilePath, ModificationFile modFile,
      VersionController versionController, TsFileResource currentTsFileResource,
      IMemTable memTable, boolean sequence) {
    this.logNodePrefix = logNodePrefix;
    this.insertFilePath = insertFilePath;
    this.modFile = modFile;
    this.versionController = versionController;
    this.currentTsFileResource = currentTsFileResource;
    this.recoverMemTable = memTable;
    this.sequence = sequence;
  }

  /**
   * finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the WALs from
   * the logNode and redoes them into a given MemTable and ModificationFile.
   */
  public void replayLogs() {
    WriteLogNode logNode = MultiFileLogNodeManager.getInstance().getNode(
        logNodePrefix + FSFactoryProducer.getFSFactory().getFile(insertFilePath).getName());

    ILogReader logReader = logNode.getLogReader();
    try {
      while (logReader.hasNext()) {
        try {
          PhysicalPlan plan = logReader.next();
          if (plan instanceof InsertPlan) {
            replayInsert((InsertPlan) plan);
          } else if (plan instanceof DeletePlan) {
            replayDelete((DeletePlan) plan);
          } else if (plan instanceof UpdatePlan) {
            replayUpdate((UpdatePlan) plan);
          }
        } catch (Exception e) {
          logger.error("recover wal of {} failed", insertFilePath, e);
        }
      }
    } catch (IOException e) {
      logger.error("meet error when redo wal of {}", insertFilePath, e);
    } finally {
      logReader.close();
      try {
        modFile.close();
      } catch (IOException e) {
        logger.error("Canno close the modifications file {}", modFile.getFilePath(), e);
      }
    }
    tempStartTimeMap.forEach((k, v) -> currentTsFileResource.updateStartTime(k, v));
    tempEndTimeMap.forEach((k, v) -> currentTsFileResource.updateEndTime(k, v));
  }

  private void replayDelete(DeletePlan deletePlan) throws IOException {
    List<PartialPath> paths = deletePlan.getPaths();
    for (PartialPath path : paths) {
      recoverMemTable
          .delete(path.getDevice(), path.getMeasurement(), deletePlan.getDeleteStartTime(),
              deletePlan.getDeleteEndTime());
      modFile
          .write(
              new Deletion(path, versionController.nextVersion(), deletePlan.getDeleteStartTime(),
                  deletePlan.getDeleteEndTime()));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void replayInsert(InsertPlan plan) throws WriteProcessException, QueryProcessException {
    if (currentTsFileResource != null) {
      long minTime, maxTime;
      if (plan instanceof InsertRowPlan) {
        minTime = ((InsertRowPlan) plan).getTime();
        maxTime = ((InsertRowPlan) plan).getTime();
      } else {
        minTime = ((InsertTabletPlan) plan).getMinTime();
        maxTime = ((InsertTabletPlan) plan).getMaxTime();
      }
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = currentTsFileResource.getEndTime(plan.getDeviceId().getFullPath());
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= minTime &&
          sequence) {
        return;
      }
      Long startTime = tempStartTimeMap.get(plan.getDeviceId().getFullPath());
      if (startTime == null || startTime > minTime) {
        tempStartTimeMap.put(plan.getDeviceId().getFullPath(), minTime);
      }
      Long endTime = tempEndTimeMap.get(plan.getDeviceId().getFullPath());
      if (endTime == null || endTime < maxTime) {
        tempEndTimeMap.put(plan.getDeviceId().getFullPath(), maxTime);
      }
    }
    MeasurementSchema[] schemas;
    try {
      schemas = IoTDB.metaManager.getSchemas(plan.getDeviceId(), plan
          .getMeasurements());
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    if (plan instanceof InsertRowPlan) {
      InsertRowPlan tPlan = (InsertRowPlan) plan;
      tPlan.setSchemasAndTransferType(schemas);
      recoverMemTable.insert(tPlan);
    } else {
      InsertTabletPlan tPlan = (InsertTabletPlan) plan;
      tPlan.setSchemas(schemas);
      recoverMemTable.insertTablet(tPlan, 0, tPlan.getRowCount());
    }
  }

  @SuppressWarnings("unused")
  private void replayUpdate(UpdatePlan updatePlan) {
    // TODO: support update
    throw new UnsupportedOperationException("Update not supported");
  }
}
