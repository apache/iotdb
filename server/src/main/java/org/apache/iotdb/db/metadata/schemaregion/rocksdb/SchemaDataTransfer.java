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

package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.AcquireLockTimeoutException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.MTreeAboveSG;
import org.apache.iotdb.db.metadata.mtree.MTreeBelowSG;
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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

/** Use the class to transfer data from mlog.bin and mtree-*.snapshot to rocksdb based manager */
public class SchemaDataTransfer {

  private static final Logger logger = LoggerFactory.getLogger(SchemaDataTransfer.class);

  private static final int DEFAULT_TRANSFER_THREAD_POOL_SIZE = 200;
  private static final int DEFAULT_TRANSFER_PLANS_BUFFER_SIZE = 100_000;

  private final ForkJoinPool forkJoinPool = new ForkJoinPool(DEFAULT_TRANSFER_THREAD_POOL_SIZE);

  private String mtreeSnapshotPath;
  private RSchemaRegion rocksDBManager;
  private MLogWriter mLogWriter;
  private final String FAILED_MLOG_PATH =
      IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
          + File.separator
          + MetadataConstant.METADATA_LOG
          + ".transfer_failed";

  private final String IDX_FILE_PATH =
      RSchemaReadWriteHandler.ROCKSDB_PATH + File.separator + "transfer_mlog.idx";

  private final AtomicInteger failedPlanCount = new AtomicInteger(0);
  private final List<PhysicalPlan> retryPlans = new ArrayList<>();

  SchemaDataTransfer() throws MetadataException {
    rocksDBManager = new RSchemaRegion();
  }

  public static void main(String[] args) {
    try {
      SchemaDataTransfer transfer = new SchemaDataTransfer();
      transfer.doTransfer();
    } catch (MetadataException | IOException | ExecutionException | InterruptedException e) {
      logger.error(e.getMessage());
    }
  }

  public void doTransfer() throws IOException, ExecutionException, InterruptedException {
    File failedFile = new File(FAILED_MLOG_PATH);
    if (failedFile.exists()) {
      logger.info("Failed file [" + FAILED_MLOG_PATH + "] delete:" + failedFile.delete());
    }

    mLogWriter = new MLogWriter(FAILED_MLOG_PATH);
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
    if (mtreeSnapshot.exists()) {
      try {
        doTransferFromSnapshot();
      } catch (MetadataException e) {
        logger.error("Fatal error, terminate data transfer!!!", e);
      }
    }

    String logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    File logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    // init the metadata from the operation log
    if (logFile.exists()) {
      try (MLogReader mLogReader = new MLogReader(schemaDir, MetadataConstant.METADATA_LOG)) {
        int startIdx = 0;
        File idxFile = new File(IDX_FILE_PATH);
        if (idxFile.exists()) {
          try (BufferedReader br = new BufferedReader(new FileReader(idxFile))) {
            String idxStr = br.readLine();
            if (StringUtils.isNotEmpty(idxStr)) {
              startIdx = Integer.valueOf(idxStr);
            }
          }
        }
        transferFromMLog(mLogReader, startIdx);
      } catch (Exception e) {
        throw new IOException("Failed to parser mlog.bin for err:" + e);
      }
    } else {
      logger.info("No mlog.bin file find, skip data transfer");
    }
    mLogWriter.close();

    logger.info("Transfer metadata from MManager to MRocksDBManager complete!");
  }

  private void transferFromMLog(MLogReader mLogReader, long startIdx)
      throws IOException, MetadataException, ExecutionException, InterruptedException {
    long time = System.currentTimeMillis();
    logger.info("start from {} to transfer data from mlog.bin", startIdx);
    int currentIdx = 0;
    PhysicalPlan plan;
    List<PhysicalPlan> nonCollisionCollections = new ArrayList<>();
    while (mLogReader.hasNext()) {
      try {
        plan = mLogReader.next();
        currentIdx++;
        if (currentIdx <= startIdx) {
          continue;
        }
      } catch (Exception e) {
        logger.error("Parse mlog error at lineNumber {} because:", currentIdx, e);
        throw e;
      }
      if (plan == null) {
        continue;
      }

      switch (plan.getOperatorType()) {
        case CREATE_TIMESERIES:
        case CREATE_ALIGNED_TIMESERIES:
        case AUTO_CREATE_DEVICE_MNODE:
          nonCollisionCollections.add(plan);
          if (nonCollisionCollections.size() > DEFAULT_TRANSFER_PLANS_BUFFER_SIZE) {
            executeBufferedOperation(nonCollisionCollections);
          }
          break;
        case SET_STORAGE_GROUP:
        case TTL:
        case CHANGE_ALIAS:
        case DELETE_TIMESERIES:
          executeBufferedOperation(nonCollisionCollections);
          try {
            rocksDBManager.operation(plan);
          } catch (IOException e) {
            logger.warn("operate [" + plan.getOperatorType().toString() + "] failed, try again.");
            rocksDBManager.operation(plan);
          } catch (MetadataException e) {
            logger.error("Can not operate cmd {} for err:", plan.getOperatorType(), e);
          }
          break;
        case DELETE_STORAGE_GROUP:
          DeleteStorageGroupPlan deleteStorageGroupPlan = (DeleteStorageGroupPlan) plan;
          for (PartialPath path : deleteStorageGroupPlan.getPaths()) {
            logger.info("delete storage group: {}", path.getFullPath());
          }
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
          logger.error("unsupported operations {}", plan);
          break;
        default:
          logger.error("Unrecognizable command {}", plan.getOperatorType());
      }
    }

    executeBufferedOperation(nonCollisionCollections);

    if (retryPlans.size() > 0) {
      for (PhysicalPlan retryPlan : retryPlans) {
        try {
          rocksDBManager.operation(retryPlan);
        } catch (MetadataException e) {
          logger.error("Execute plan failed: {}", retryPlan, e);
        } catch (Exception e) {
          persistFailedLog(retryPlan);
        }
      }
    }

    File idxFile = new File(IDX_FILE_PATH);
    try (FileWriter writer = new FileWriter(idxFile)) {
      writer.write(String.valueOf(currentIdx));
    }
    logger.info(
        "Transfer data from mlog.bin complete after {}ms with {} errors",
        System.currentTimeMillis() - time,
        failedPlanCount.get());
  }

  private void executeBufferedOperation(List<PhysicalPlan> plans)
      throws ExecutionException, InterruptedException {
    if (plans.size() <= 0) {
      return;
    }
    forkJoinPool
        .submit(
            () ->
                plans
                    .parallelStream()
                    .forEach(
                        x -> {
                          try {
                            rocksDBManager.operation(x);
                          } catch (MetadataException e) {
                            if (e instanceof AcquireLockTimeoutException) {
                              retryPlans.add(x);
                            } else {
                              logger.error("Execute plan failed: {}", x, e);
                            }
                          } catch (Exception e) {
                            retryPlans.add(x);
                          }
                        }))
        .get();
    logger.debug("parallel executed {} operations", plans.size());
    plans.clear();
  }

  private void persistFailedLog(PhysicalPlan plan) {
    logger.info("persist failed plan: {}", plan.toString());
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
          "Fatal error, exception when persist failed plan, metadata transfer should be failed", e);
      throw new RuntimeException("Terminate transfer as persist log fail.");
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void doTransferFromSnapshot() throws MetadataException {
    logger.info("Start transfer data from snapshot");
    long start = System.currentTimeMillis();

    MTreeAboveSG mTree = new MTreeAboveSG();
    //    mTree.init();
    //    List<IStorageGroupMNode> storageGroupNodes = mTree.getAllStorageGroupNodes();
    //
    AtomicInteger errorCount = new AtomicInteger(0);
    //    storageGroupNodes
    //        .parallelStream()
    //        .forEach(
    //            sgNode -> {
    //              try {
    //                rocksDBManager.setStorageGroup(sgNode.getPartialPath());
    //                if (sgNode.getDataTTL() > 0) {
    //                  rocksDBManager.setTTL(sgNode.getPartialPath(), sgNode.getDataTTL());
    //                }
    //              } catch (MetadataException | IOException e) {
    //                if (!(e instanceof StorageGroupAlreadySetException)
    //                    && !(e instanceof PathAlreadyExistException)
    //                    && !(e instanceof AliasAlreadyExistException)) {
    //                  errorCount.incrementAndGet();
    //                }
    //                logger.error(
    //                    "create storage group {} failed", sgNode.getPartialPath().getFullPath(),
    // e);
    //              }
    //            });

    //    if (errorCount.get() > 0) {
    //      logger.error("Fatal error. Create some storage groups fail, terminate metadata
    // transfer");
    //      return;
    //    }

    Queue<IMeasurementMNode> failCreatedNodes = new ConcurrentLinkedQueue<>();
    AtomicInteger createdNodeCnt = new AtomicInteger(0);
    AtomicInteger lastValue = new AtomicInteger(-1);

    new Thread(
            () -> {
              while (lastValue.get() < createdNodeCnt.get()) {
                try {
                  lastValue.set(createdNodeCnt.get());
                  Thread.sleep(10L * 1000);
                  logger.info("created count: {}", createdNodeCnt.get());
                } catch (InterruptedException e) {
                  logger.error("timer thread error", e);
                }
              }
              logger.info("exit");
            })
        .start();

    List<IStorageGroupMNode> sgNodes = mTree.getAllStorageGroupNodes();
    sgNodes
        .parallelStream()
        .forEach(
            sgNode -> {
              try {
                MTreeBelowSG sg = new MTreeBelowSG(sgNode);
                List<IMeasurementMNode> measurementMNodes = sg.getAllMeasurementMNode();
                measurementMNodes
                    .parallelStream()
                    .forEach(
                        mNode -> {
                          try {
                            rocksDBManager.createTimeSeries(
                                mNode.getPartialPath(),
                                mNode.getSchema(),
                                mNode.getAlias(),
                                null,
                                null);
                            createdNodeCnt.incrementAndGet();
                          } catch (AcquireLockTimeoutException e) {
                            failCreatedNodes.add(mNode);
                          } catch (MetadataException e) {
                            logger.error(
                                "create timeseries {} failed",
                                mNode.getPartialPath().getFullPath(),
                                e);
                            errorCount.incrementAndGet();
                          }
                        });
              } catch (IOException e) {
                logger.error("Failed to construct BelowSg instance", e);
              }
            });

    if (!failCreatedNodes.isEmpty()) {
      failCreatedNodes.stream()
          .forEach(
              mNode -> {
                try {
                  rocksDBManager.createTimeSeries(
                      mNode.getPartialPath(), mNode.getSchema(), mNode.getAlias(), null, null);
                  createdNodeCnt.incrementAndGet();
                } catch (Exception e) {
                  logger.error(
                      "create timeseries {} failed in retry",
                      mNode.getPartialPath().getFullPath(),
                      e);
                  errorCount.incrementAndGet();
                }
              });
    }

    logger.info(
        "Transfer data from snapshot complete after {}ms with {} errors",
        System.currentTimeMillis() - start,
        errorCount.get());
  }
}
