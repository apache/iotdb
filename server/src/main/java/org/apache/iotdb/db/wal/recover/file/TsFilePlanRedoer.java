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
package org.apache.iotdb.db.wal.recover.file;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.mpp.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * This class helps redo wal logs into a TsFile. Notice: You should update time map in {@link
 * TsFileResource} before using this class to avoid duplicated insertion and this class doesn't
 * guarantee concurrency safety.
 */
public class TsFilePlanRedoer {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePlanRedoer.class);

  private final TsFileResource tsFileResource;
  /** only unsequence file tolerates duplicated data */
  private final boolean sequence;
  /** virtual database's idTable of this tsFile */
  private final IDTable idTable;
  /** store data when redoing logs */
  private IMemTable recoveryMemTable = new PrimitiveMemTable();

  public TsFilePlanRedoer(TsFileResource tsFileResource, boolean sequence, IDTable idTable) {
    this.tsFileResource = tsFileResource;
    this.sequence = sequence;
    this.idTable = idTable;
  }

  void redoDelete(DeleteDataNode deleteDataNode) throws IOException {
    List<PartialPath> paths = deleteDataNode.getPathList();
    for (PartialPath path : paths) {
      // path here is device path pattern
      recoveryMemTable.delete(
          path,
          path.getDevicePath(),
          deleteDataNode.getDeleteStartTime(),
          deleteDataNode.getDeleteEndTime());
      tsFileResource
          .getModFile()
          .write(
              new Deletion(
                  path,
                  tsFileResource.getTsFileSize(),
                  deleteDataNode.getDeleteStartTime(),
                  deleteDataNode.getDeleteEndTime()));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  void redoInsert(InsertNode node) throws WriteProcessException {
    if (!node.hasValidMeasurements()) {
      return;
    }
    if (tsFileResource != null) {
      String deviceId =
          node.isAligned()
              ? node.getDevicePath().getDevicePath().getFullPath()
              : node.getDevicePath().getFullPath();
      // orders of insert node is guaranteed by storage engine, just check time in the file
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = tsFileResource.getEndTime(deviceId);
      long minTimeInNode;
      if (node instanceof InsertRowNode) {
        minTimeInNode = ((InsertRowNode) node).getTime();
      } else {
        minTimeInNode = ((InsertTabletNode) node).getTimes()[0];
      }
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= minTimeInNode && sequence) {
        return;
      }
    }

    if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
      // TODO get device id by idTable
      // idTable.getSeriesSchemas(node);
    } else {
      if (!IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
        SchemaValidator.validate(node);
      }
      node.setDeviceID(DeviceIDFactory.getInstance().getDeviceID(node.getDevicePath()));
    }

    if (node instanceof InsertRowNode) {
      if (node.isAligned()) {
        recoveryMemTable.insertAlignedRow((InsertRowNode) node);
      } else {
        recoveryMemTable.insert((InsertRowNode) node);
      }
    } else {
      if (node.isAligned()) {
        recoveryMemTable.insertAlignedTablet(
            (InsertTabletNode) node, 0, ((InsertTabletNode) node).getRowCount());
      } else {
        recoveryMemTable.insertTablet(
            (InsertTabletNode) node, 0, ((InsertTabletNode) node).getRowCount());
      }
    }
  }

  void resetRecoveryMemTable(IMemTable memTable) {
    this.recoveryMemTable = memTable;
  }

  IMemTable getRecoveryMemTable() {
    return recoveryMemTable;
  }
}
