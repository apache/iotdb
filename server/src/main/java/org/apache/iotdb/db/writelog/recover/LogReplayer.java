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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogReplayer finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the
 * WALs from the logNode and redoes them into a given MemTable and ModificationFile.
 */
public class LogReplayer {
  Logger logger = LoggerFactory.getLogger(LogReplayer.class);
  private String logNodePrefix;
  private String insertFilePath;
  private ModificationFile modFile;
  private VersionController versionController;
  private TsFileResource currentTsFileResource;
  // schema is used to get the measurement data type
  private Schema schema;
  private IMemTable recoverMemTable;

  // unsequence file tolerates duplicated data
  private boolean acceptDuplication;

  private Map<String, Long> tempStartTimeMap = new HashMap<>();
  private Map<String, Long> tempEndTimeMap = new HashMap<>();

  public LogReplayer(String logNodePrefix, String insertFilePath,
      ModificationFile modFile,
      VersionController versionController,
      TsFileResource currentTsFileResource,
      Schema schema, IMemTable memTable, boolean acceptDuplication) {
    this.logNodePrefix = logNodePrefix;
    this.insertFilePath = insertFilePath;
    this.modFile = modFile;
    this.versionController = versionController;
    this.currentTsFileResource = currentTsFileResource;
    this.schema = schema;
    this.recoverMemTable = memTable;
    this.acceptDuplication = acceptDuplication;
  }

  /**
   * finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the WALs from
   * the logNode and redoes them into a given MemTable and ModificationFile.
   */
  public void replayLogs() throws StorageGroupProcessorException {
    WriteLogNode logNode = MultiFileLogNodeManager.getInstance().getNode(
        logNodePrefix + FSFactoryProducer.getFSFactory().getFile(insertFilePath).getName());

    ILogReader logReader = logNode.getLogReader();
    try {
      while (logReader.hasNext()) {
        PhysicalPlan plan = logReader.next();
        if (plan instanceof InsertPlan) {
          replayInsert((InsertPlan) plan);
        } else if (plan instanceof DeletePlan) {
          replayDelete((DeletePlan) plan);
        } else if (plan instanceof UpdatePlan) {
          replayUpdate((UpdatePlan) plan);
        } else if (plan instanceof BatchInsertPlan) {
          replayBatchInsert((BatchInsertPlan) plan);
        }
      }
    } catch (IOException e) {
      throw new StorageGroupProcessorException("Cannot replay logs" + e.getMessage());
    } catch (QueryProcessException e) {
      throw new StorageGroupProcessorException(
          "Cannot replay logs for query processor exception" + e.getMessage());
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
    List<Path> paths = deletePlan.getPaths();
    for (Path path : paths) {
      recoverMemTable.delete(path.getDevice(), path.getMeasurement(), deletePlan.getDeleteTime());
      modFile
          .write(new Deletion(path, versionController.nextVersion(), deletePlan.getDeleteTime()));
    }
  }

  private void replayBatchInsert(BatchInsertPlan batchInsertPlan) throws QueryProcessException {
    if (currentTsFileResource != null) {
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      Long lastEndTime = currentTsFileResource.getEndTimeMap().get(batchInsertPlan.getDeviceId());
      if (lastEndTime != null && lastEndTime >= batchInsertPlan.getMinTime() &&
          !acceptDuplication) {
        return;
      }
      Long startTime = tempStartTimeMap.get(batchInsertPlan.getDeviceId());
      if (startTime == null || startTime > batchInsertPlan.getMinTime()) {
        tempStartTimeMap.put(batchInsertPlan.getDeviceId(), batchInsertPlan.getMinTime());
      }
      Long endTime = tempEndTimeMap.get(batchInsertPlan.getDeviceId());
      if (endTime == null || endTime < batchInsertPlan.getMaxTime()) {
        tempEndTimeMap.put(batchInsertPlan.getDeviceId(), batchInsertPlan.getMaxTime());
      }
    }

    recoverMemTable.insertBatch(batchInsertPlan, 0, batchInsertPlan.getRowCount());
  }

  private void replayInsert(InsertPlan insertPlan) throws QueryProcessException {
    if (currentTsFileResource != null) {
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      Long lastEndTime = currentTsFileResource.getEndTimeMap().get(insertPlan.getDeviceId());
      if (lastEndTime != null && lastEndTime >= insertPlan.getTime() &&
          !acceptDuplication) {
        return;
      }
      Long startTime = tempStartTimeMap.get(insertPlan.getDeviceId());
      if (startTime == null || startTime > insertPlan.getTime()) {
        tempStartTimeMap.put(insertPlan.getDeviceId(), insertPlan.getTime());
      }
      Long endTime = tempEndTimeMap.get(insertPlan.getDeviceId());
      if (endTime == null || endTime < insertPlan.getTime()) {
        tempEndTimeMap.put(insertPlan.getDeviceId(), insertPlan.getTime());
      }
    }
    String[] measurementList = insertPlan.getMeasurements();
    TSDataType[] dataTypes = new TSDataType[measurementList.length];
    for (int i = 0; i < measurementList.length; i++) {
      dataTypes[i] = schema.getMeasurementDataType(measurementList[i]);
    }
    insertPlan.setDataTypes(dataTypes);
    try {
      recoverMemTable.insert(insertPlan);
    } catch (Exception e) {
      logger.error(
          "occurs exception when replaying the record {} at timestamp {}: {}.(Will ignore the record)",
          insertPlan.getPaths(), insertPlan.getTime(), e.getMessage());
    }
  }

  @SuppressWarnings("unused")
  private void replayUpdate(UpdatePlan updatePlan) {
    // TODO: support update
    throw new UnsupportedOperationException("Update not supported");
  }
}
