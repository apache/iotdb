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

package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MetaDataTransfer {

  private static final Logger logger = LoggerFactory.getLogger(MetaDataTransfer.class);

  private String mtreeSnapshotPath;
  private MRocksDBManager rocksDBManager;

  private AtomicInteger storageGroupToCreateCount = new AtomicInteger();
  private AtomicLong timeSeriesToCreateCount = new AtomicLong();

  private AtomicInteger createStorageGroupCount = new AtomicInteger();
  private AtomicLong createTimeSeriesCount = new AtomicLong();

  MetaDataTransfer() throws MetadataException {
    rocksDBManager = new MRocksDBManager();
  }

  public void init() throws IOException {
    mtreeSnapshotPath =
        IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
            + File.separator
            + MetadataConstant.MTREE_SNAPSHOT;

    File mtreeSnapshot = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    long time = System.currentTimeMillis();
    if (mtreeSnapshot.exists()) {
      transferFromSnapshot(mtreeSnapshot);
      logger.debug(
          "spend {} ms to deserialize mtree from snapshot", System.currentTimeMillis() - time);
    }

    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }

    time = System.currentTimeMillis();
    String logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    File logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    // init the metadata from the operation log
    if (logFile.exists()) {
      try (MLogReader mLogReader = new MLogReader(schemaDir, MetadataConstant.METADATA_LOG); ) {
        transferFromMLog(mLogReader);
        logger.info(
            "spend {} ms to deserialize mtree from mlog.bin", System.currentTimeMillis() - time);
      } catch (Exception e) {
        throw new IOException("Failed to parser mlog.bin for err:" + e);
      }
    } else {
      logger.info("no mlog.bin file find, skip transfer");
    }

    logger.info("Do transfer success");
  }

  private void transferFromMLog(MLogReader mLogReader) {
    int idx = 0;
    PhysicalPlan plan;
    while (mLogReader.hasNext()) {
      try {
        plan = mLogReader.next();
        idx++;
      } catch (Exception e) {
        logger.error("Parse mlog error at lineNumber {} because:", idx, e);
        break;
      }
      if (plan == null) {
        continue;
      }
      try {
        rocksDBManager.operation(plan);
      } catch (MetadataException | IOException e) {
        logger.error("Can not operate cmd {} for err:", plan.getOperatorType(), e);
      }
    }
  }

  public void transferFromSnapshot(File mtreeSnapshot) {
    try (MLogReader mLogReader = new MLogReader(mtreeSnapshot)) {
      doTransferFromSnapshot(mLogReader);
    } catch (IOException e) {
      logger.warn("Failed to deserialize from {}. Use a new MTree.", mtreeSnapshot.getPath());
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void doTransferFromSnapshot(MLogReader mLogReader) {
    long start = System.currentTimeMillis();
    List<IMeasurementMNode> measurementList = new ArrayList<>();
    List<StorageGroupMNode> sgList = new ArrayList<>();
    while (mLogReader.hasNext()) {
      PhysicalPlan plan = null;
      try {
        plan = mLogReader.next();
        if (plan == null) {
          continue;
        }
        if (plan instanceof StorageGroupMNodePlan) {
          sgList.add(StorageGroupMNode.deserializeFrom((StorageGroupMNodePlan) plan));
        } else if (plan instanceof MeasurementMNodePlan) {
          measurementList.add(MeasurementMNode.deserializeFrom((MeasurementMNodePlan) plan));
        }
      } catch (Exception e) {
        logger.error(
            "Can not operate cmd {} for err:", plan == null ? "" : plan.getOperatorType(), e);
      }
    }

    sgList
        .parallelStream()
        .forEach(
            sgNode -> {
              try {
                rocksDBManager.setStorageGroup(sgNode.getPartialPath());
                if (sgNode.getDataTTL() > 0) {
                  rocksDBManager.setTTL(sgNode.getPartialPath(), sgNode.getDataTTL());
                }
              } catch (MetadataException | IOException e) {
                logger.error("");
              }
            });

    measurementList
        .parallelStream()
        .forEach(
            mNode -> {
              try {
                rocksDBManager.createTimeSeries(
                    mNode.getPartialPath(), mNode.getSchema(), mNode.getAlias(), null, null);
              } catch (MetadataException e) {
                logger.error("");
              }
            });

    logger.info("snapshot transfer complete after {}ms", System.currentTimeMillis() - start);
  }
}
