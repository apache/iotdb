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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.deviceID.DeviceIDFactory;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;

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
  /** virtual storage group's idTable of this tsFile */
  private final IDTable idTable;
  /** store data when redoing logs */
  private IMemTable recoveryMemTable = new PrimitiveMemTable();

  public TsFilePlanRedoer(TsFileResource tsFileResource, boolean sequence, IDTable idTable) {
    this.tsFileResource = tsFileResource;
    this.sequence = sequence;
    this.idTable = idTable;
  }

  void redoDelete(DeletePlan deletePlan) throws IOException, MetadataException {
    List<PartialPath> paths = deletePlan.getPaths();
    for (PartialPath path : paths) {
      for (PartialPath device : IoTDB.schemaProcessor.getBelongedDevices(path)) {
        recoveryMemTable.delete(
            path, device, deletePlan.getDeleteStartTime(), deletePlan.getDeleteEndTime());
      }
      tsFileResource
          .getModFile()
          .write(
              new Deletion(
                  path,
                  tsFileResource.getTsFileSize(),
                  deletePlan.getDeleteStartTime(),
                  deletePlan.getDeleteEndTime()));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  void redoInsert(InsertPlan plan) throws WriteProcessException, QueryProcessException {
    if (!plan.hasValidMeasurements()) {
      return;
    }
    if (tsFileResource != null) {
      String deviceId =
          plan.isAligned()
              ? plan.getDevicePath().getDevicePath().getFullPath()
              : plan.getDevicePath().getFullPath();
      // orders of insert plan is guaranteed by storage engine, just check time in the file
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = tsFileResource.getEndTime(deviceId);
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= plan.getMinTime() && sequence) {
        return;
      }
    }

    plan.setMeasurementMNodes(new IMeasurementMNode[plan.getMeasurements().length]);
    try {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
        idTable.getSeriesSchemas(plan);
      } else {
        IoTDB.schemaProcessor.getSeriesSchemasAndReadLockDevice(plan);
        plan.setDeviceID(DeviceIDFactory.getInstance().getDeviceID(plan.getDevicePath()));
      }
    } catch (IOException | MetadataException e) {
      throw new QueryProcessException("can't replay insert logs, ", e);
    }

    // mark failed plan manually
    checkDataTypeAndMarkFailed(plan.getMeasurementMNodes(), plan);
    if (plan instanceof InsertRowPlan) {
      if (plan.isAligned()) {
        recoveryMemTable.insertAlignedRow((InsertRowPlan) plan);
      } else {
        recoveryMemTable.insert((InsertRowPlan) plan);
      }
    } else {
      if (plan.isAligned()) {
        recoveryMemTable.insertAlignedTablet(
            (InsertTabletPlan) plan, 0, ((InsertTabletPlan) plan).getRowCount());
      } else {
        recoveryMemTable.insertTablet(
            (InsertTabletPlan) plan, 0, ((InsertTabletPlan) plan).getRowCount());
      }
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

  private void checkDataTypeAndMarkFailed(final IMeasurementMNode[] mNodes, InsertPlan tPlan) {
    for (int i = 0; i < mNodes.length; i++) {
      if (mNodes[i] == null) {
        tPlan.markFailedMeasurementInsertion(
            i,
            new PathNotExistException(
                tPlan.getDevicePath().getFullPath()
                    + IoTDBConstant.PATH_SEPARATOR
                    + tPlan.getMeasurements()[i]));
      } else if (mNodes[i].getSchema().getType() != tPlan.getDataTypes()[i]) {
        tPlan.markFailedMeasurementInsertion(
            i,
            new DataTypeMismatchException(
                tPlan.getDevicePath().getFullPath(),
                mNodes[i].getName(),
                tPlan.getDataTypes()[i],
                mNodes[i].getSchema().getType(),
                tPlan.getMinTime(),
                tPlan.getFirstValueOfIndex(i)));
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
