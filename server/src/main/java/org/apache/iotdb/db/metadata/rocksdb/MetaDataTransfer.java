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
import org.apache.iotdb.db.exception.metadata.AcquireLockTimeoutException;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTree;
import org.apache.iotdb.db.metadata.mtree.traverser.collector.MeasurementCollector;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaDataTransfer {

  private static final Logger logger = LoggerFactory.getLogger(MetaDataTransfer.class);

  private String mtreeSnapshotPath;
  private MRocksDBManager rocksDBManager;
  private MLogWriter mLogWriter;
  private String failedMLogPath =
      IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
          + File.separator
          + MetadataConstant.METADATA_LOG
          + ".transfer_failed";

  private AtomicInteger failedPlanCount = new AtomicInteger(0);
  private List<PhysicalPlan> retryPlans = new ArrayList<>();

  MetaDataTransfer() throws MetadataException {
    rocksDBManager = new MRocksDBManager();
  }

  public static void main(String[] args) {
    try {
      MetaDataTransfer transfer = new MetaDataTransfer();
      transfer.doTransfer();
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
    }
  }

  public void doTransfer() throws IOException {
    File failedFile = new File(failedMLogPath);
    if (failedFile.exists()) {
      failedFile.delete();
    }

    mLogWriter = new MLogWriter(failedMLogPath);
    mLogWriter.setLogNum(0);

    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }

    mtreeSnapshotPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT;
    File mtreeSnapshot = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    long time = System.currentTimeMillis();
    if (mtreeSnapshot.exists()) {
      transferFromSnapshot(mtreeSnapshot);
      logger.info("spend {} ms to transfer data from snapshot", System.currentTimeMillis() - time);
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

    mLogWriter.close();

    logger.info(
        "do transfer complete with {} plan failed. Failed plan are persisted in mlog.bin.transfer_failed",
        failedPlanCount.get());
  }

  private void transferFromMLog(MLogReader mLogReader) {
    int idx = 0;
    PhysicalPlan plan;
    List<PhysicalPlan> nonCollisionCollections = new ArrayList<>();
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
        switch (plan.getOperatorType()) {
          case CREATE_TIMESERIES:
          case CREATE_ALIGNED_TIMESERIES:
          case AUTO_CREATE_DEVICE_MNODE:
            nonCollisionCollections.add(plan);
            if (nonCollisionCollections.size() > 100000) {
              executeOperation(nonCollisionCollections, true);
            }
            break;
          case DELETE_TIMESERIES:
          case SET_STORAGE_GROUP:
          case DELETE_STORAGE_GROUP:
          case TTL:
          case CHANGE_ALIAS:
            executeOperation(nonCollisionCollections, true);
            rocksDBManager.operation(plan);
            break;
          case CHANGE_TAG_OFFSET:
          case CREATE_TEMPLATE:
          case DROP_TEMPLATE:
          case APPEND_TEMPLATE:
          case PRUNE_TEMPLATE:
          case SET_TEMPLATE:
          case ACTIVATE_TEMPLATE:
          case UNSET_TEMPLATE:
          case CREATE_CONTINUOUS_QUERY:
          case DROP_CONTINUOUS_QUERY:
            logger.error("unsupported operations {}", plan.toString());
            break;
          default:
            logger.error("Unrecognizable command {}", plan.getOperatorType());
        }
      } catch (MetadataException | IOException e) {
        logger.error("Can not operate cmd {} for err:", plan.getOperatorType(), e);
        if (!(e instanceof StorageGroupAlreadySetException)
            && !(e instanceof PathAlreadyExistException)
            && !(e instanceof AliasAlreadyExistException)) {
          persistFailedLog(plan);
        }
      }
    }
    executeOperation(nonCollisionCollections, true);
    if (retryPlans.size() > 0) {
      executeOperation(retryPlans, false);
    }
  }

  private void executeOperation(List<PhysicalPlan> plans, boolean needsToRetry) {
    plans
        .parallelStream()
        .forEach(
            x -> {
              try {
                rocksDBManager.operation(x);
              } catch (IOException e) {
                logger.error("failed to operate plan: {}", x.toString(), e);
                retryPlans.add(x);
              } catch (MetadataException e) {
                logger.error("failed to operate plan: {}", x.toString(), e);
                if (e instanceof AcquireLockTimeoutException && needsToRetry) {
                  retryPlans.add(x);
                } else {
                  persistFailedLog(x);
                }
              } catch (Exception e) {
                if (needsToRetry) {
                  retryPlans.add(x);
                } else {
                  persistFailedLog(x);
                }
              }
            });
    logger.info("parallel executed {} operations", plans.size());
    plans.clear();
  }

  private void persistFailedLog(PhysicalPlan plan) {
    logger.info("persist won't retry and failed plan: {}", plan.toString());
    failedPlanCount.incrementAndGet();
    try {
      switch (plan.getOperatorType()) {
        case CREATE_TIMESERIES:
          mLogWriter.createTimeseries((CreateTimeSeriesPlan) plan);
          break;
        case CREATE_ALIGNED_TIMESERIES:
          mLogWriter.createAlignedTimeseries((CreateAlignedTimeSeriesPlan) plan);
          break;
        case AUTO_CREATE_DEVICE_MNODE:
          mLogWriter.autoCreateDeviceMNode((AutoCreateDeviceMNodePlan) plan);
          break;
        case DELETE_TIMESERIES:
          mLogWriter.deleteTimeseries((DeleteTimeSeriesPlan) plan);
          break;
        case SET_STORAGE_GROUP:
          SetStorageGroupPlan setStorageGroupPlan = (SetStorageGroupPlan) plan;
          mLogWriter.setStorageGroup(setStorageGroupPlan.getPath());
          break;
        case DELETE_STORAGE_GROUP:
          DeleteStorageGroupPlan deletePlan = (DeleteStorageGroupPlan) plan;
          for (PartialPath path : deletePlan.getPaths()) {
            mLogWriter.deleteStorageGroup(path);
          }
          break;
        case TTL:
          SetTTLPlan ttlPlan = (SetTTLPlan) plan;
          mLogWriter.setTTL(ttlPlan.getStorageGroup(), ttlPlan.getDataTTL());
          break;
        case CHANGE_ALIAS:
          ChangeAliasPlan changeAliasPlan = (ChangeAliasPlan) plan;
          mLogWriter.changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
          break;
        case CHANGE_TAG_OFFSET:
        case CREATE_TEMPLATE:
        case DROP_TEMPLATE:
        case APPEND_TEMPLATE:
        case PRUNE_TEMPLATE:
        case SET_TEMPLATE:
        case ACTIVATE_TEMPLATE:
        case UNSET_TEMPLATE:
        case CREATE_CONTINUOUS_QUERY:
        case DROP_CONTINUOUS_QUERY:
          throw new UnsupportedOperationException(plan.getOperatorType().toString());
        default:
          logger.error("Unrecognizable command {}", plan.getOperatorType());
      }
    } catch (IOException e) {
      logger.error(
          "fatal error, exception when persist failed plan, metadata transfer should be failed", e);
    }
  }

  public void transferFromSnapshot(File mtreeSnapshot) {
    try (MLogReader mLogReader = new MLogReader(mtreeSnapshot)) {
      doTransferFromSnapshot(mLogReader);
    } catch (IOException | MetadataException e) {
      logger.warn("Failed to deserialize from {}. Use a new MTree.", mtreeSnapshot.getPath());
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void doTransferFromSnapshot(MLogReader mLogReader) throws IOException, MetadataException {
    long start = System.currentTimeMillis();
    MTree mTree = new MTree();
    mTree.init();
    List<IStorageGroupMNode> storageGroupNodes = mTree.getAllStorageGroupNodes();

    AtomicInteger errorCount = new AtomicInteger(0);
    storageGroupNodes
        .parallelStream()
        .forEach(
            sgNode -> {
              try {
                rocksDBManager.setStorageGroup(sgNode.getPartialPath());
                if (sgNode.getDataTTL() > 0) {
                  rocksDBManager.setTTL(sgNode.getPartialPath(), sgNode.getDataTTL());
                }
              } catch (MetadataException | IOException e) {
                if (!(e instanceof StorageGroupAlreadySetException)
                    && !(e instanceof PathAlreadyExistException)
                    && !(e instanceof AliasAlreadyExistException)) {
                  errorCount.incrementAndGet();
                }
                logger.error(
                    "create storage group {} failed", sgNode.getPartialPath().getFullPath(), e);
              }
            });

    if (errorCount.get() > 0) {
      logger.info("Fatal error. create some storage groups fail, terminate metadata transfer");
      return;
    }

    List<IMeasurementMNode> measurementMNodes = new ArrayList<>();

    MeasurementCollector collector =
        new MeasurementCollector(
            mTree.getNodeByPath(new PartialPath("root")), new PartialPath("root.**"), -1, -1) {
          @Override
          protected void collectMeasurement(IMeasurementMNode node) throws MetadataException {
            measurementMNodes.add(node);
          }
        };
    collector.traverse();

    measurementMNodes
        .parallelStream()
        .forEach(
            mNode -> {
              try {
                rocksDBManager.createTimeSeries(
                    mNode.getPartialPath(), mNode.getSchema(), mNode.getAlias(), null, null);
              } catch (AcquireLockTimeoutException e) {
                try {
                  rocksDBManager.createTimeSeries(
                      mNode.getPartialPath(), mNode.getSchema(), mNode.getAlias(), null, null);
                } catch (MetadataException metadataException) {
                  logger.error(
                      "create timeseries {} failed in retry",
                      mNode.getPartialPath().getFullPath(),
                      e);
                  errorCount.incrementAndGet();
                }
              } catch (MetadataException e) {
                logger.error(
                    "create timeseries {} failed", mNode.getPartialPath().getFullPath(), e);
                errorCount.incrementAndGet();
              }
            });

    logger.info(
        "metadata snapshot transfer complete after {}ms with {} errors",
        System.currentTimeMillis() - start,
        errorCount.get());
  }
}
